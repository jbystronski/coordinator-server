package coordinator

import (
	"coordinator/src/node"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

func (c *Coordinator) writePump(client *node.Client) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.disconnectNode(client.ID)
		client.Conn.Close()
	}()

	for {
		select {
		case msg, ok := <-client.Send:
			client.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// Channel closed — send close frame
				client.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := client.Conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				log.Printf("Write error to node %s: %v\n", client.ID, err)
				return
			}

		case <-ticker.C:
			// Keepalive ping
			client.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := client.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("Ping error to node %s: %v\n", client.ID, err)
				return
			}
		}
	}
}
