package provider

import (
	"context"
	"fmt"
	"gx/ipfs/QmR8BauakNcBa3RbE4nbQu76PDiJgoQgz8AJdhJuiU4TAw/go-cid"
	"gx/ipfs/QmZBH87CAPFHcc7cYmBqeSQ98zQ3SX9KUxiYgzPmLWNVKz/go-libp2p-routing"
	logging "gx/ipfs/QmcuXC5cxs79ro2cUuHs4HQ2bkDLJUYokwL8aivcX6HW3C/go-log"
	"sync"
	"time"
)

var (
	log = logging.Logger("provider")
)

const (
	provideOutgoingWorkerLimit = 8
	provideOutgoingTimeout     = 15 * time.Second
)

type AnchorStrategy func(context.Context, chan cid.Cid, cid.Cid)
type EligibleStrategy func(cid.Cid) bool

type Provider struct {
	ctx context.Context
	lock sync.Mutex

	// cids we want to provide
	incoming chan cid.Cid
	// cids we are working on providing now
	outgoing chan cid.Cid

	// strategy for deciding which cids, given a cid, should be provided, the so-called "anchors"
	anchors AnchorStrategy

	tracker *Tracker
	queue        *Queue

	contentRouting routing.ContentRouting // TODO: temp, maybe
}

func NewProvider(ctx context.Context, anchors AnchorStrategy, tracker *Tracker, queue *Queue, contentRouting routing.ContentRouting) *Provider {
	return &Provider{
		ctx:            ctx,
		lock: 			sync.Mutex{},
		outgoing:       make(chan cid.Cid),
		incoming:       make(chan cid.Cid),
		anchors:        anchors,
		tracker:		tracker,
		queue:          queue,
		contentRouting: contentRouting,
	}
}

// Start workers to handle provide requests.
func (p *Provider) Run() {
	go p.handleIncoming()
	go p.handleOutgoing()
	go p.handlePopulateOutgoing()
}

// Provider the given cid using specified strategy.
func (p *Provider) Provide(root cid.Cid) error {
	isTracking, err := p.tracker.IsTracking(root)
	if err != nil {
		return err
	}
	if isTracking {
		return nil
	}

	p.anchors(p.ctx, p.incoming, root)
	return nil
}

// Announce to the world that a block is provided.
//
// TODO: Refactor duplication between here and the reprovider.
func (p *Provider) Announce(cid cid.Cid) {
	ctx, cancel := context.WithTimeout(p.ctx, provideOutgoingTimeout)
	defer cancel()

	if err := p.contentRouting.Provide(ctx, cid, true); err != nil {
		log.Warning("Failed to provide key: %s", err)
	}
	fmt.Println("Announced", cid)
	p.tracker.Track(cid)
	fmt.Println("Tracking", cid)
}

// Workers

func (p *Provider) handleIncoming() {
	for {
		select {
		case key := <-p.incoming:
			fmt.Println("Moving from incoming channel to providing queue", key)
			p.lock.Lock()
			p.queue.Enqueue(key)
			p.lock.Unlock()
		case <-p.ctx.Done():
			return
		}
	}
}

// Handle all outgoing cids by providing them
func (p *Provider) handleOutgoing() {
	for workers := 0; workers < provideOutgoingWorkerLimit; workers++ {
		go func() {
			for {
				select {
				case key := <-p.outgoing:
					fmt.Println("Announcing", key)
					p.Announce(key)
				case <-p.ctx.Done():
					return
				}
			}
		}()
	}
}

// Move CIDs from the providing queue to outgoing
func (p *Provider) handlePopulateOutgoing() {
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		p.lock.Lock()
		if p.queue.IsEmpty() {
			p.lock.Unlock()
			// this is maybe dumb and a crutch
			time.Sleep(1 * time.Second)
			continue
		}
		key, err := p.queue.Dequeue()
		p.lock.Unlock()
		if err != nil {
			// TODO something useful, like log
			fmt.Println("Error!", err)
			continue
		}
		fmt.Println("Moving from providing queue to outgoing channel", key)
		p.outgoing <- *key
	}
}
