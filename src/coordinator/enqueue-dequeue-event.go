package coordinator

import (
	"coordinator/src/event"
	"log"
	"time"

	"github.com/google/uuid"
)

// dequeueAndSend removes the first event from pending and refills from store automatically.
// Returns the dequeued event, or nil if queue was empty.
func (c *Coordinator) dequeueEventLocked() *event.Event {
	if len(c.pending) == 0 {
		return nil
	}

	// Pop the first event
	ev := c.pending[0]
	c.pending = c.pending[1:]

	// Refill pending from store
	for len(c.pending) < c.maxPending {
		next, err := c.store.Pop(ev.CreatedAt) // or time.Now() depending on your Pop logic
		if err != nil || next == nil {
			break
		}
		c.pending = append(c.pending, *next)
	}

	return &ev
}

func (c *Coordinator) DequeueEvent() *event.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dequeueEventLocked()
}

// EnqueueEvent adds an event to the pending queue or the store if full.
// Returns true if queued in memory, false if saved to store.
func (c *Coordinator) enqueueEventLocked(ev event.Event) (bool, string) {
	// should be locked from outside
	// Assign ID and timestamp if not set
	if ev.ID == "" {
		ev.ID = uuid.New().String()
	}
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = time.Now()
	}
	ev.TTL = 6 * time.Hour

	// Queue in memory if possible
	if len(c.pending) < c.maxPending {
		c.pending = append(c.pending, ev)
		log.Println("Event enqueued in memory:", ev.ID)
		return true, ev.ID
	}

	// Otherwise, save to store
	log.Println("Pending full, saving event to store:", ev.ID)
	if err := c.store.Put(ev); err != nil {
		log.Println("Error saving event to store:", err)
	}
	return false, ev.ID
}

func (c *Coordinator) EnqueueEvent(ev event.Event) (bool, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.enqueueEventLocked(ev)
}
