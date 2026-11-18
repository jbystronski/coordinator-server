package coordinator

import (
	"coordinator/src/event"
	"coordinator/src/node"
	"log"
	"time"

	"github.com/google/uuid"
)


func (c *Coordinator) disconnectNode(client *node.Client) {
	c.mu.Lock()

	if c.nodes[client.ID] != client {
		c.mu.Unlock()
		return
	}

	delete(c.nodes, client.ID)

	// Requeue unacknowledged events belonging to this connection.
	for eventId, inflight := range c.inFlight {
		if inflight.NodeID == client.ID {
			if queued, _, _ := c.enqueueEventLocked(inflight.Event); queued {
				log.Println("Requeued event", eventId, "in memory after node disconnect", client.ID)
			} else {
				log.Println("Requeued event", eventId, "saved to store after node disconnect", client.ID)
			}
			delete(c.inFlight, eventId)
		}
	}

	var domain string
	var newMaster *node.Client

	if client.IsMaster {
		if d, ok := domainOf(client.ID); ok {
			domain = d
			newMaster = c.electMasterLocked(domain)
		}
	}


	c.mu.Unlock()


	_ = client.Conn.Close()
	log.Println("Node disconnected:", client.ID)

	if newMaster != nil {
		clients := c.matchingNodes([]string{domain + "::"})

		ev := event.Event[any]{
			Meta: event.Meta{
				ID:        uuid.NewString(),
				Timestamp: time.Now(),
				Delivery:  event.Broadcast,
				Targets:   []string{domain + "::"},
			},
			Type: event.MasterSet,
			Payload: event.MasterSetPayload{
						NodeID: newMaster.ID,
					},
		}

		c.SendEventToAll(ev, clients)
	}
}
