package coordinator

import (
	"fmt"
	"log"
	"time"

	"coordinator/src/event"
	"coordinator/src/node"

	"github.com/google/uuid"
)

// enqueueEventLocked queues an event in memory, or persists it if full.
// The caller must hold c.mu.
func (c *Coordinator) enqueueEventLocked(
	ev event.Event[any],
) (bool, string, error) {
	if ev.Meta.ID == "" {
		ev.Meta.ID = uuid.New().String()
	}

	if ev.Meta.Timestamp.IsZero() {
		ev.Meta.Timestamp = time.Now()
	}

	if ev.Meta.TTL <= 0 {
		ev.Meta.TTL = 6 * time.Hour
	}

	if len(c.pending) < c.maxPending {
		c.pending = append(c.pending, ev)
		log.Println("Event enqueued in memory:", ev.Meta.ID)
		return true, ev.Meta.ID, nil
	}

	if err := c.store.Put(ev); err != nil {
		return false, ev.Meta.ID,
			fmt.Errorf("persist event %s: %w", ev.Meta.ID, err)
	}

	log.Println("Event saved to store:", ev.Meta.ID)
	return false, ev.Meta.ID, nil
}


func (c *Coordinator) dequeueEventForNodeLocked(
	client *node.Client,
) *event.Event[any] {
	now := time.Now()

	for i := 0; i < len(c.pending); {
		ev := c.pending[i]

		if event.IsExpired(ev, now) {
			log.Printf("Discarding expired event %s", ev.Meta.ID)
			c.pending = append(c.pending[:i], c.pending[i+1:]...)
			continue
		}

		// The pending queue is for deferred Single delivery.
		// Don't consume a Broadcast as though it were a Single.
		if ev.Meta.Delivery != event.Single {
			log.Printf(
				"Discarding non-Single event %s from pending queue",
				ev.Meta.ID,
			)
			c.pending = append(c.pending[:i], c.pending[i+1:]...)
			continue
		}

		if !matchesNode(ev, client) {
			i++
			continue
		}

		// Remove and return the matching event.
		result := ev
		c.pending = append(c.pending[:i], c.pending[i+1:]...)

		return &result
	}

	// No eligible in-memory event; try persistent storage.
	ev, err := c.store.PopMatchingForNode(
		client.ID,
		now,
	)
	if err != nil {
		log.Printf(
			"Failed to dequeue event for node %s: %v",
			client.ID,
			err,
		)
	}

	return ev
}
