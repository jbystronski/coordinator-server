package coordinator

import (
	"coordinator/src/event"
	"log"
)

func (c *Coordinator) handleAck(clientId, eventId string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	inFlight, ok := c.inFlight[eventId]
	if ok && inFlight.Event.Delivery == event.Single {
		delete(c.inFlight, eventId)

		log.Printf("ACK received for event %s from node %s", eventId, clientId)
	}
}
