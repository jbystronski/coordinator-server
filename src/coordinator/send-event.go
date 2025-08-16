package coordinator

import (
	"coordinator/src/event"
	"log"
)

func (c *Coordinator) SendEvent(ev event.Event) (int, string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// if ev.ID == "" {
	// 	ev.ID = uuid.New().String()
	// }
	//
	// ev.CreatedAt = time.Now()
	// ev.TTL = 6 * time.Hour
	//
	// log.Println("New event queued:", ev.ID, ev.Type)

	if len(c.nodes) == 0 {
		_, id := c.enqueueEventLocked(ev)
		return 0, id
	}

	// if len(c.nodes) == 0 {
	// 	if len(c.pending) >= c.maxPending {
	// 		log.Println("Pending full, saving event to store:", ev.ID)
	// 		if err := c.store.Put(ev); err != nil {
	// 			log.Println("Error saving event to store:", err)
	// 		}
	// 	} else {
	// 		c.pending = append(c.pending, ev)
	// 		log.Println("No nodes connected, event queued:", ev.ID)
	// 	}
	// 	return 0, ev.ID
	// }
	var count int
	switch ev.Delivery {
	case event.Broadcast:
		count = c.SendEventToAll(ev)
	case event.Single:
		count = c.SendEventToSingle(ev)
	default:
		log.Println("unknown delivery mode, defaulting to broadcast")
		count = c.SendEventToAll(ev)
	}

	// c.RefillFromStore()

	return count, ev.ID
}
