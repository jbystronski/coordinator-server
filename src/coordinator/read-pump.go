package coordinator

import (
	"encoding/json"
	"log"
	"time"

	"coordinator/src/event"
	"coordinator/src/node"
)

func (c *Coordinator) readPump(client *node.Client) {
	defer func() {
		c.disconnectNode(client.ID)
		client.Conn.Close()
	}()

	client.Conn.SetReadLimit(maxMessageSize)
	client.Conn.SetReadDeadline(time.Now().Add(pongWait))
	client.Conn.SetPongHandler(func(string) error {
		client.Conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, msg, err := client.Conn.ReadMessage()
		if err != nil {
			log.Printf("Read error from node %s: %v\n", client.ID, err)
			break
		}

		// Peek type only
		var peek struct {
			Type event.EventType `json:"type"`
		}
		if err := json.Unmarshal(msg, &peek); err != nil {
			log.Printf("Invalid event from node %s: %v\n", client.ID, err)
			continue
		}

		switch peek.Type {

		case event.Ping:
			var e event.Event[event.PingPayload]
			if err := json.Unmarshal(msg, &e); err != nil {
				log.Printf("Failed to parse Ping from %s: %v", client.ID, err)
				continue
			}
			// Optional: reply with a pong (or log it)
			log.Printf("Received ping from %s at %s", client.ID, e.Payload.Timestamp)

		case event.ACK:
			var e event.Event[event.AckPayload]
			if err := json.Unmarshal(msg, &e); err != nil {
				log.Printf("Failed to parse ACK from %s: %v", client.ID, err)
				continue
			}
			c.handleAck(client.ID, e.Payload.AckedEventID)

		case event.NodeMessage:
			var e event.Event[event.NodeMessageEventPayload]
			if err := json.Unmarshal(msg, &e); err != nil {
				log.Printf("Failed to parse NodeMessage from %s: %v", client.ID, err)
				continue
			}
			c.forwardToNode(e)
		}
	}
}
