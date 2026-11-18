package coordinator

import (
	"coordinator/src/event"
	"log"
	"time"
)

func (c *Coordinator) StartExpiredEventCleanup(interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			c.cleanupExpiredEvents()
		}
	}()
}

func (c *Coordinator) cleanupExpiredEvents() {
	now := time.Now()

	c.mu.Lock()

	kept := c.pending[:0]
	removed := 0

	for _, ev := range c.pending {
		if event.IsExpired(ev,now) {
			removed++
			continue
		}

		kept = append(kept, ev)
	}

	c.pending = kept
	c.mu.Unlock()

	if removed > 0 {
		log.Printf("Removed %d expired events from memory queue", removed)
	}

	deleted, err := c.store.DeleteExpired(now)
	if err != nil {
		log.Printf("Failed to clean expired persistent events: %v", err)
		return
	}

	if deleted > 0 {
		log.Printf("Removed %d expired events from Bolt store", deleted)
	}
}
