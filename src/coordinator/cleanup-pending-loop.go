package coordinator

import (
	"log"
	"time"
)

func (c *Coordinator) CleanupPendingLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		var newPending []Event
		for _, ev := range c.pending {
			if ev.CreatedAt.Add(ev.TTL).After(now) {
				newPending = append(newPending, ev)
			} else {
				log.Println("Dropping expired event:", ev.ID)
			}
		}
		c.pending = newPending
		c.mu.Unlock()
	}
}
