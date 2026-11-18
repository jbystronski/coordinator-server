package coordinator

import (
	"encoding/json"
	"log"
	"time"

	"coordinator/src/event"
	"coordinator/src/node"
)




func (c *Coordinator) flushPendingToNodeLocked(
	client *node.Client,
) {
	for {
		ev := c.dequeueEventForNodeLocked(client)
		if ev == nil {
			return
		}

		// Only Single events belong in the pending queue.
		// Remove stale queued broadcasts rather than leaving them
		// stranded until their TTL expires.
		if ev.Meta.Delivery != event.Single {
			log.Printf(
				"Discarding non-Single event %s from pending queue",
				ev.Meta.ID,
			)
			continue
		}

		data, err := json.Marshal(ev)
		if err != nil {
			log.Printf(
				"Failed to marshal event %s: %v",
				ev.Meta.ID,
				err,
			)

			c.enqueueEventLocked(*ev)
			return
		}

		sentAt := time.Now()

		// Register before making the event available to writePump.
		if c.inFlight == nil {
			c.inFlight = make(map[string]event.InFlightEvent[any])
		}

		c.inFlight[ev.Meta.ID] = event.InFlightEvent[any]{
			Event:  *ev,
			NodeID: client.ID,
			SentAt: sentAt,
		}

		select {
		case client.Send <- data:
			log.Printf(
				"Flushed event %s to node %s",
				ev.Meta.ID,
				client.ID,
			)

			go c.waitForAck(ev.Meta.ID, 15*time.Second)

		default:
			// The node's send buffer is full. Undo this registration
			// and retain the event for a later attempt.
			if inflight, ok := c.inFlight[ev.Meta.ID]; ok &&
				inflight.NodeID == client.ID &&
				inflight.SentAt.Equal(sentAt) {
				delete(c.inFlight, ev.Meta.ID)
			}

			c.enqueueEventLocked(*ev)

			log.Printf(
				"Node send channel full; requeued event %s",
				ev.Meta.ID,
			)
			return
		}
	}
}
