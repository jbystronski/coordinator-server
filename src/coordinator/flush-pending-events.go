package coordinator

import (
	"coordinator/src/event"
	"coordinator/src/node"
	"encoding/json"
	"log"
	"time"
)

func (c *Coordinator) flushPendingToNode(client *node.Client) {
	for {
		ev := c.dequeueEventLocked() // dequeues from pending and refills from store
		if ev == nil {
			break
		}

		data, _ := json.Marshal(ev)

		select {
		case client.Send <- data:
			if ev.Delivery == event.Single {
				if c.inFlight == nil {
					c.inFlight = make(map[string]event.InFlightEvent)
				}
				c.inFlight[ev.ID] = event.InFlightEvent{
					Event:  *ev,
					NodeID: client.ID,
					SentAt: time.Now(),
				}
				go c.waitForAck(ev.ID, 15*time.Second)
			}
			log.Println("Flushed event", ev.ID, "to node", client.ID)
		default:

			queued, _ := c.enqueueEventLocked(*ev)
			if queued {
				log.Println("Pending event", ev.ID, "requeued in memory due to full channel")
			} else {
				log.Println("Pending event", ev.ID, "saved to store due to full channel")
			}

		}
	}
}
