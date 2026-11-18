package coordinator

import (
	"encoding/json"
	"log"
	"time"

	"coordinator/src/event"
	"coordinator/src/node"
)


func (c *Coordinator) SendEventToSingle(ev event.Event[any],clients []*node.Client) int {


	n := len(clients)

	if n == 0 {
		return 0
	}

	c.mu.Lock()
	start := c.lastIndex % n
	c.mu.Unlock()

	data, err := json.Marshal(ev)
	if err != nil {
		log.Println("failed to marshal event:", err)
		return 0
	}

	requiresAck := ev.Meta.Delivery == event.Single




	for i := range n {
		idx := (start + i) % n
		client := clients[idx]
		nodeID := client.ID

		// Register BEFORE making the event available to the WebSocket writer.
		sentAt := time.Now()

		if requiresAck {
			c.mu.Lock()

			if c.inFlight == nil {
				c.inFlight = make(map[string]event.InFlightEvent[any])
			}

			c.inFlight[ev.Meta.ID] = event.InFlightEvent[any]{
				Event:  ev,
				NodeID: nodeID,
				SentAt: sentAt,
			}

			c.mu.Unlock()
		}

		select {
		case client.Send <- data:
			c.mu.Lock()
			c.lastIndex = (idx + 1) % n
			c.mu.Unlock()

			log.Printf(
				"Event %s sent to node %s",
				ev.Meta.ID,
				nodeID,
			)

			if requiresAck {
				go c.waitForAck(ev.Meta.ID, 15*time.Second)
			}

			return 1

		default:
			// The channel is full. Remove this attempted registration
			// before trying the next node.
			if requiresAck {
				c.mu.Lock()

				if inflight, ok := c.inFlight[ev.Meta.ID]; ok &&
					inflight.NodeID == nodeID &&
					inflight.SentAt.Equal(sentAt) {
					delete(c.inFlight, ev.Meta.ID)
				}

				c.mu.Unlock()
			}
		}
	}

	log.Printf(
		"All matching nodes are busy for event %s",
		ev.Meta.ID,
	)

	return 0
}
