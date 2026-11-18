package coordinator

import (
	"encoding/json"
	"log"

	"coordinator/src/event"
	"coordinator/src/node"
)




func (c *Coordinator) SendEventToAll(ev event.Event[any], clients []*node.Client) int {
	data, err := json.Marshal(ev)
	if err != nil {
		log.Println("failed to marshal event:", err)
		return 0
	}


	sent := 0

	for _, client := range clients {
		select {
		case client.Send <- data:
			sent++
		default:
			log.Printf(
				"Send channel full; event %s not sent to node %s",
				ev.Meta.ID,
				client.ID,
			)
		}
	}

	return sent
}
