// package coordinator

// import (
// 	"coordinator/src/event"
// 	"coordinator/src/node"
// 	"log"
// 	"net/http"
// 	"time"
// )

// const (
// 	writeWait      = 10 * time.Second
// 	pongWait       = 60 * time.Second
// 	pingPeriod     = (pongWait * 9) / 10
// 	maxMessageSize = 1 << 20 // 1MB
// )

// func (c *Coordinator) HandleWS(w http.ResponseWriter, r *http.Request) {
// 	nodeID := r.URL.Query().Get("nodeId")
// 	nodeName := r.URL.Query().Get("nodeName")

// 	if !event.ValidNodeID(nodeID) {
// 		http.Error(w, "invalid node id", http.StatusBadRequest)
// 		return
// 	}

// 	conn, err := c.upgrader.Upgrade(w, r, nil)
// 	if err != nil {
// 		log.Printf("WebSocket upgrade error for node %s: %v\n", nodeID, err)
// 		return
// 	}

// 	client := &node.Client{
// 		ID:   nodeID,
// 		Name: nodeName,
// 		Conn: conn,
// 		Send: make(chan []byte, 256), // buffered channel
// 	}

// 	// Register client + flush pending
// 	c.mu.Lock()
// 	c.nodes[nodeID] = client
// 	c.flushPendingToNodeLocked(client)
// 	c.mu.Unlock()

// 	log.Printf("Node connected: %s (name=%s)\n", nodeID, nodeName)

// 	// Start pumps
// 	go c.writePump(client)
// 	go c.readPump(client)
// }

package coordinator

import (
	"coordinator/src/event"
	"coordinator/src/node"
	"fmt"
	"log"
	"net/http"
	"time"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 1 << 20 // 1 MB
)

// HandleWS is for trusted server clients authenticated by API-key middleware.
func (c *Coordinator) HandleWS(w http.ResponseWriter, r *http.Request) {
	nodeID := r.URL.Query().Get("nodeId")

	fmt.Println("in handleWS")

	if !event.ValidNodeID(nodeID) {
		http.Error(w, "invalid node id", http.StatusBadRequest)
		return
	}

	c.HandleWSConnection(w, r, nodeID, true)
}

// HandleWSConnection upgrades and registers a node whose identity
// has already been validated by the caller.
func (c *Coordinator) HandleWSConnection(
	w http.ResponseWriter,
	r *http.Request,
	nodeID string,
		flushPending bool,
) {
	nodeName := r.URL.Query().Get("nodeName")

	conn, err := c.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error for node %s: %v", nodeID, err)
		return
	}

	client := &node.Client{
		ID:   nodeID,
		Name: nodeName,
		Conn: conn,
		Send: make(chan []byte, 256),
	}

	c.mu.Lock()
	c.nodes[nodeID] = client

	if domain, ok := domainOf(nodeID); ok {
		c.ensureMasterLocked(domain)
	}


	if flushPending {
			c.flushPendingToNodeLocked(client)
		}
	c.mu.Unlock()

	log.Printf("Node connected: %s (name=%s)", nodeID, nodeName)

	go c.writePump(client)
	go c.readPump(client)
}


func (c *Coordinator) HandleWSBrowser(w http.ResponseWriter, r *http.Request) {
	nodeID := r.URL.Query().Get("nodeId")
	ticket := r.URL.Query().Get("ticket")

	if !event.ValidNodeID(nodeID) {
		http.Error(w, "invalid node id", http.StatusBadRequest)
		return
	}

	if err := c.wsTickets.Consume(ticket, nodeID); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	c.HandleWSConnection(w, r, nodeID, false)
}
