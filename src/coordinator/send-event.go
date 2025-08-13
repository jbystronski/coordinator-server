package coordinator

import "log"

func (c *Coordinator) SendEvent(ev Event) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.nodes) == 0 {
		if len(c.pending) >= c.maxPending {
			oldest := c.pending[0]
			log.Println("Pending full, dropping oldest event:", oldest.ID)
			c.pending = c.pending[1:]
		}
		c.pending = append(c.pending, ev)
		log.Println("No nodes connected, event queued:", ev.ID)
		return 0
	}

	switch ev.Delivery {
	case Broadcast:
		return c.SendEventToAll(ev)
	case Single:
		return c.SendEventToSingle(ev)
	default:
		log.Println("unknown delivery mode, defaulting to broadcast")
		return c.SendEventToAll(ev)
	}
}
