package coordinator

import (
	"encoding/json"
	"log"
	"time"

	"coordinator/src/event"
)

func (c *Coordinator) SendEventToSingle(ev event.Event[any]) int {
	nodeIDs := make([]string, 0, len(c.nodes))
	for id := range c.nodes {
		nodeIDs = append(nodeIDs, id)
	}
	n := len(nodeIDs)

	if n == 0 {
		// This case should never happen if SendEvent checks before calling
		return 0
	}
	start := c.lastIndex % n

	data, err := json.Marshal(ev)
	if err != nil {
		log.Println("failed to marshal event:", err)
		return 0
	}

	for i := range n {
		idx := (start + i) % n
		nodeID := nodeIDs[idx]
		client := c.nodes[nodeID]

		select {
		case client.Send <- data:
			c.lastIndex = (idx + 1) % n
			log.Println("Event", ev.EventID, "sent to node", nodeID)
			if ev.Delivery == event.Single {
				if c.inFlight == nil {
					c.inFlight = make(map[string]event.InFlightEvent[any])
				}
				c.inFlight[ev.EventID] = event.InFlightEvent[any]{
					Event:  ev,
					NodeID: nodeID,
					SentAt: time.Now(),
				}
				go c.waitForAck(ev.EventID, 15*time.Second)
			}
			return 1
		default:
			// node busy, try next
		}
	}

	// All nodes busy, queue for later

	log.Println("All nodes busy, event queued:", ev.EventID)
	return 0
}
