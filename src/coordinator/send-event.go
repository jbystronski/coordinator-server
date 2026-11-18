package coordinator

import (
	"log"

	"coordinator/src/event"
)


func (c *Coordinator) SendEvent(
	ev event.Event[any],
) (int, string, error) {
	if len(ev.Meta.Targets) == 0 {
		log.Printf(
			"Event %s has no addressing information; dropping",
			ev.Meta.ID,
		)
		return 0, ev.Meta.ID, nil
	}

	switch ev.Meta.Delivery {
	case event.Single, event.Broadcast:
		// Valid delivery modes.
	default:
		log.Printf(
			"Unknown delivery mode %q for event %s",
			ev.Meta.Delivery,
			ev.Meta.ID,
		)
		return 0, ev.Meta.ID, nil
	}

	clients := c.matchingNodes(
		ev.Meta.Targets,
	)

	if ev.Meta.Delivery == event.Broadcast {
		if len(clients) == 0 {
			return 0, ev.Meta.ID,nil
		}

		return c.SendEventToAll(ev, clients), ev.Meta.ID,nil
	}

	// Single: retain until an eligible node can receive it.
	if len(clients) == 0 {
		c.mu.Lock()
		_, eventID,err := c.enqueueEventLocked(ev)
		c.mu.Unlock()

		return 0, eventID,err
	}

	count := c.SendEventToSingle(ev, clients)

	if count == 0 {
		c.mu.Lock()
		_, eventID,err := c.enqueueEventLocked(ev)
		c.mu.Unlock()

		return 0, eventID,err
	}

	return count, ev.Meta.ID, nil
}
