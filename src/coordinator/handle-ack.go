package coordinator

import (
	"coordinator/src/event"
	"log"
)

// func (c *Coordinator) handleAck(clientId, eventId string) {
// 	c.mu.Lock()
// 	defer c.mu.Unlock()
// 	fmt.Println("handling ack")
// 	inFlight, ok := c.inFlight[eventId]
// 	if ok && inFlight.Event.Meta.Delivery == event.Single {
// 		delete(c.inFlight, eventId)

// 		log.Printf("ACK received for event %s from node %s", eventId, clientId)
// 	}
// }


func (c *Coordinator) handleAck(clientID, eventID string) {
	c.mu.Lock()
	defer c.mu.Unlock()



	inFlight, ok := c.inFlight[eventID]
	if !ok {
		log.Printf("ACK for unknown or expired event %s from node %s",
			eventID, clientID)
		return
	}

	if inFlight.NodeID != clientID {
		log.Printf("ACK from unexpected node: event=%s expected=%s got=%s",
			eventID, inFlight.NodeID, clientID)
		return
	}

	if inFlight.Event.Meta.Delivery != event.Single {
		log.Printf("ACK ignored: event %s has delivery mode %v",
			eventID, inFlight.Event.Meta.Delivery)
		return
	}

	delete(c.inFlight, eventID)
	log.Printf("ACK received for event %s from node %s", eventID, clientID)
}
