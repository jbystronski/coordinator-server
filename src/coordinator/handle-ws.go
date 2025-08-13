package coordinator

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 1 << 20 // 1MB
)

func (c *Coordinator) HandleWS(w http.ResponseWriter, r *http.Request) {
	nodeId := r.URL.Query().Get("nodeId")

	nodeName := r.URL.Query().Get("nodeName")
	if nodeId == "" {
		http.Error(w, "missing node id", http.StatusBadRequest)
		return
	}

	conn, err := c.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error for node %s: %v\n", nodeId, err)
		return
	}

	client := &NodeClient{
		id:   nodeId,
		name: nodeName,
		conn: conn,
		send: make(chan []byte, 256), // buffered channel
	}

	// Register client + flush pending
	c.mu.Lock()
	c.nodes[nodeId] = client
	// if len(c.pending) > 0 {
	// 	remaining := c.pending[:0]
	// 	for _, ev := range c.pending {
	// 		data, _ := json.Marshal(ev)
	// 		select {
	// 		case client.send <- data:
	// 			log.Println("Pending event", ev.ID, "delivered to node", nodeId)
	// 		default:
	// 			remaining = append(remaining, ev)
	// 		}
	// 	}
	// 	c.pending = remaining
	// }

	if len(c.pending) > 0 {
		remaining := c.pending[:0]
		for _, ev := range c.pending {
			data, _ := json.Marshal(ev)

			select {
			case client.send <- data:
				if ev.Delivery == Single { // only track single-target events
					if c.inFlight == nil {
						c.inFlight = make(map[string]InFlightEvent)
					}
					c.inFlight[ev.ID] = InFlightEvent{
						Event:  ev,
						NodeID: nodeId,
						SentAt: time.Now(),
					}

					go c.waitForAck(ev.ID, 15*time.Second)
				}
				log.Println("Pending event", ev.ID, "delivered to node", nodeId)
			default:
				remaining = append(remaining, ev)
			}
		}
		c.pending = remaining
	}
	c.mu.Unlock()

	log.Printf("Node connected: %s (name=%s)\n", nodeId, nodeName)
	// Start pumps
	go c.writePump(client)
	go c.readPump(client)
}

func (c *Coordinator) readPump(client *NodeClient) {
	defer func() {
		c.disconnectNode(client.id)
		client.conn.Close()
	}()

	client.conn.SetReadLimit(maxMessageSize)
	client.conn.SetReadDeadline(time.Now().Add(pongWait))
	client.conn.SetPongHandler(func(string) error {
		client.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	for {
		_, msg, err := client.conn.ReadMessage()
		if err != nil {
			log.Printf("Read error from node %s: %v\n", client.id, err)
			break
		}

		var m map[string]string
		if err := json.Unmarshal(msg, &m); err != nil {
			log.Println("Invalid message from node", client.id)
			continue
		}

		if m["type"] == "ack" {
			eventID := m["eventId"]

			c.mu.Lock()
			inflight, ok := c.inFlight[eventID]
			if ok && inflight.Event.Delivery == Single {
				delete(c.inFlight, eventID)
				log.Printf("ACK received for event %s from node %s", eventID, client.id)
			}
			c.mu.Unlock()
		}

		// if m["type"] == "ack" {
		// 	eventID := m["eventId"]
		// 	c.mu.Lock()
		// 	delete(c.inFlight, eventID)
		// 	c.mu.Unlock()
		// 	log.Println("Event", eventID, "acknowledged by node", client.id)
		// }
	}
	// for {
	// 	if _, _, err := client.conn.ReadMessage(); err != nil {
	// 		log.Printf("Read error from node %s: %v\n", client.id, err)
	// 		break
	// 	}
	// }
}

func (c *Coordinator) writePump(client *NodeClient) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.disconnectNode(client.id)
		client.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-client.send:
			client.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// Channel closed — send close frame
				client.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := client.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				log.Printf("Write error to node %s: %v\n", client.id, err)
				return
			}

		case <-ticker.C:
			// Keepalive ping
			client.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := client.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("Ping error to node %s: %v\n", client.id, err)
				return
			}
		}
	}
}

func (c *Coordinator) disconnectNode(nodeId string) {
	c.mu.Lock()
	if client, ok := c.nodes[nodeId]; ok {
		close(client.send)
		delete(c.nodes, nodeId)
	}

	// Requeue unacknowledged events sent to this node
	for id, inflight := range c.inFlight {
		if inflight.NodeID == nodeId {
			c.pending = append(c.pending, inflight.Event)
			delete(c.inFlight, id)
			log.Println("Requeued event", id, "after node disconnect", nodeId)
		}
	}
	c.mu.Unlock()
	log.Println("Node disconnected:", nodeId)
}

// func (c *Coordinator) HandleWS(w http.ResponseWriter, r *http.Request) {
// 	nodeId := r.URL.Query().Get("nodeId")
// 	nodeName := r.URL.Query().Get("nodeName")
// 	if nodeId == "" {
// 		http.Error(w, "missing node id", http.StatusBadRequest)
// 		return
// 	}
//
// 	conn, err := c.upgrader.Upgrade(w, r, nil)
// 	if err != nil {
// 		log.Printf("WebSocket upgrade error for node %s: %v\n", nodeId, err)
// 		return
// 	}
//
// 	client := &NodeClient{
// 		id:   nodeId,
// 		name: nodeName,
// 		conn: conn,
// 		send: make(chan []byte, 256), // buffered channel
// 	}
//
// 	// Register client and immediately flush pending events
// 	c.mu.Lock()
// 	c.nodes[nodeId] = client
//
// 	if len(c.pending) > 0 {
// 		remaining := c.pending[:0]
// 		for _, ev := range c.pending {
// 			data, _ := json.Marshal(ev)
// 			select {
// 			case client.send <- data:
// 				log.Println("Pending event", ev.ID, "delivered to node", nodeId)
// 			default:
// 				remaining = append(remaining, ev) // still cannot deliver
// 			}
// 		}
// 		c.pending = remaining
// 	}
// 	c.mu.Unlock()
// 	log.Printf("Node connected: %s (name=%s)\n", nodeId, nodeName)
//
// 	// Write pump: sends messages from send channel to WebSocket
// 	go func() {
// 		defer func() {
// 			conn.Close()
// 			c.mu.Lock()
// 			delete(c.nodes, nodeId)
// 			c.mu.Unlock()
// 			log.Println("Node disconnected:", nodeId)
// 		}()
//
// 		for msg := range client.send {
// 			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
// 				log.Printf("Write error to node %s: %v\n", nodeId, err)
// 				return
// 			}
// 		}
// 	}()
//
// 	// Read pump: reads messages just to detect disconnection
// 	go func() {
// 		defer close(client.send)
// 		for {
// 			if _, _, err := conn.ReadMessage(); err != nil {
// 				log.Printf("Read error from node %s: %v\n", nodeId, err)
// 				return
// 			}
// 		}
// 	}()
// }
