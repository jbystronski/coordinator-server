package coordinator

import (
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
)

func (c *Coordinator) forwardToNode(msg NodeMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	msg.CreatedOn = time.Now()
	msg.MessageID = uuid.New().String()
	target, ok := c.nodes[msg.ToNode]
	if !ok {
		log.Printf("Target node %s not connected, cannot deliver message from %s", msg.ToNode, msg.FromNode)
		return
	}

	encoded, err := json.Marshal(msg)
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
