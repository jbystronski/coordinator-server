package coordinator

import (
	"encoding/json"
	"log"

	"coordinator/src/event"
)

func (c *Coordinator) SendEventToAll(ev event.Event[any]) int {
	data, err := json.Marshal(ev)
	if err != nil {
		log.Println("failed to marshal event:", err)
		return 0
	}

	sent := 0
	for nodeID, client := range c.nodes {
		select {
		case client.Send <- data:
			sent++
		default:
			log.Println("send channel full, dropping event for node", nodeID)
		}
	}
	if sent == 0 {
		log.Println("All nodes busy, event dropped:", ev.EventID)
	}
	return sent
}
