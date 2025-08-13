package coordinator

import (
	"log"
	"time"
)

func (c *Coordinator) waitForAck(eventID string, timeout time.Duration) {
	<-time.After(timeout)

	c.mu.Lock()
	inflight, exists := c.inFlight[eventID]
	if exists {
		log.Printf("Event %s timed out waiting for ACK from node %s", eventID, inflight.NodeID)
		delete(c.inFlight, eventID)
		// Requeue the event for delivery to another node
		c.pending = append(c.pending, inflight.Event)
	}
	c.mu.Unlock()
}
