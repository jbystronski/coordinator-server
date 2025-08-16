package coordinator

import (
	"coordinator/src/event"
	"encoding/json"
	"log"
	"time"
)

func (c *Coordinator) SendEventToSingle(ev event.Event) int {
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
			log.Println("Event", ev.ID, "sent to node", nodeID)
			if ev.Delivery == event.Single {
				if c.inFlight == nil {
					c.inFlight = make(map[string]event.InFlightEvent)
				}
				c.inFlight[ev.ID] = event.InFlightEvent{
					Event:  ev,
					NodeID: nodeID,
					SentAt: time.Now(),
				}
				go c.waitForAck(ev.ID, 15*time.Second)
			}
			return 1
		default:
			// node busy, try next
		}
	}

	// All nodes busy, queue for later

	log.Println("All nodes busy, event queued:", ev.ID)
	return 0
}
