package coordinator

import (
	"encoding/json"
	"log"
	"time"

	"coordinator/src/event"

	"github.com/google/uuid"
)

func (c *Coordinator) forwardToNode(e event.Event[event.NodeMessageEventPayload]) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now()
	}
	if e.Payload.MessageID == "" {
		e.Payload.MessageID = uuid.New().String()
	}
	target, ok := c.nodes[e.Payload.ToNode]
	if !ok {
		log.Printf("Target node %s not connected, cannot deliver message from %s", e.Payload.ToNode, e.Payload.FromNode)
		return
	}

	encoded, err := json.Marshal(e)
	if err != nil {
		log.Println("Failed to encode node message:", err)
		return
	}

	select {
	case target.Send <- encoded:
	default:
		log.Printf("Send buffer full for node %s, dropping message", target.ID)
	}
}
