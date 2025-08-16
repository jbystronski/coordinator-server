package coordinator

import (
	"coordinator/src/node"
	"encoding/json"
	"log"
	"time"
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

		// var m map[string]string
		var m NodeMessage
		if err := json.Unmarshal(msg, &m); err != nil {
			log.Println("Invalid message from node", client.ID)
			continue
		}

		switch m.Type {
		case "ack":
			c.handleAck(client.ID, m.EventID)
		case "nodeMesage":
			c.forwardToNode(m)
		}

		// if m["type"] == "ack" {
		// 	eventID := m["eventId"]
		//
		// 	c.mu.Lock()
		// 	inflight, ok := c.inFlight[eventID]
		// 	if ok && inflight.Event.Delivery == event.Single {
		// 		delete(c.inFlight, eventID)
		// 		log.Printf("ACK received for event %s from node %s", eventID, client.ID)
		// 	}
		// 	c.mu.Unlock()
		// }

	}
}
