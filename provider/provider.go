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
	provideOutgoingWorkerLimit = 512
	provideOutgoingTimeout     = time.Second * 15
)

type AnchorStrategy func(context.Context, chan cid.Cid, cid.Cid)
type EligibleStrategy func(cid.Cid) bool

type Provider struct {
	ctx context.Context

	// cids we want to provide
	incoming chan cid.Cid
	// cids we are working on providing now
	outgoing chan cid.Cid

	// strategy for deciding which cids, given a cid, should be provided, the so-called "anchors"
	anchors AnchorStrategy

	// strategy for deciding which cids are eligible to be provided
	eligible EligibleStrategy
	queue *Queue
	queueLock sync.Mutex

	contentRouting routing.ContentRouting // TODO: temp, maybe
}

func NewProvider(ctx context.Context, anchors AnchorStrategy, eligible EligibleStrategy, contentRouting routing.ContentRouting) *Provider {
	return &Provider{
		ctx: ctx,
		outgoing: make(chan cid.Cid),
		incoming: make(chan cid.Cid),
		anchors: anchors,
		eligible: eligible,
		queue: NewQueue(),
		queueLock: sync.Mutex{},
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
func (p *Provider) Provide(root cid.Cid) {
	if !p.eligible(root) {
		return
	}

	p.anchors(p.ctx, p.incoming, root)
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
}

// Workers

// Handle incoming requests to provide blocks
//
// Basically, buffer everything that comes through the incoming channel.
// Then, whenever the outgoing channel is ready to receive a value, pull
// a value out of the buffer and put it onto the outgoing channel.
func (p *Provider) handleIncoming() {
	for {
		select {
		case key := <-p.incoming:
			fmt.Println("Moving from incoming channel to providing queue", key)
			p.queueLock.Lock()
			p.queue.Enqueue(key)
			p.queueLock.Unlock()
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

// Move CIDs from the disk-based queue to outgoing
func (p *Provider) handlePopulateOutgoing() {
	for {
		select {
        case <-p.ctx.Done():
			return
        default:
		}

		p.queueLock.Lock()
		if p.queue.IsEmpty() {
			p.queueLock.Unlock()
			// this is probably dumb and a crutch
			time.Sleep(1 * time.Second)
			continue
		}
		key := p.queue.Dequeue()
		p.queueLock.Unlock()
		fmt.Println("Moving from providing queue to outgoing channel", key)
		p.outgoing <- key
	}
}
