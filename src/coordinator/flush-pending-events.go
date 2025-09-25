package coordinator

import (
	"encoding/json"
	"log"
	"time"

	"coordinator/src/event"
	"coordinator/src/node"
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
					c.inFlight = make(map[string]event.InFlightEvent[any])
				}
				c.inFlight[ev.EventID] = event.InFlightEvent[any]{
					Event:  *ev,
					NodeID: client.ID,
					SentAt: time.Now(),
				}
				go c.waitForAck(ev.EventID, 15*time.Second)
			}
			log.Println("Flushed event", ev.EventID, "to node", client.ID)
		default:

			queued, _ := c.enqueueEventLocked(*ev)
			if queued {
				log.Println("Pending event", ev.EventID, "requeued in memory due to full channel")
			} else {
				log.Println("Pending event", ev.EventID, "saved to store due to full channel")
			}

		}
	}
}
