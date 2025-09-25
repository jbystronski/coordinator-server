package coordinator

import (
	"log"

	"coordinator/src/event"
)

func (c *Coordinator) SendEvent(ev event.Event[any]) (int, string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.nodes) == 0 {
		_, eventId := c.enqueueEventLocked(ev)
		return 0, eventId
	}

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

	return count, ev.EventID
}
