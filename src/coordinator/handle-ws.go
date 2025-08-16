package coordinator

import (
	"coordinator/src/node"
	"log"
	"net/http"
	"time"
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

	client := &node.Client{
		ID:   nodeId,
		Name: nodeName,
		Conn: conn,
		Send: make(chan []byte, 256), // buffered channel
	}

	// Register client + flush pending
	c.mu.Lock()
	c.nodes[nodeId] = client
	c.flushPendingToNode(client)
	c.mu.Unlock()

	log.Printf("Node connected: %s (name=%s)\n", nodeId, nodeName)

	// Start pumps
	go c.writePump(client)
	go c.readPump(client)
}
