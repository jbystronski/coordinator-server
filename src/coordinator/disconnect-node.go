package coordinator

import "log"

func (c *Coordinator) disconnectNode(nodeId string) {
	c.mu.Lock()

	defer c.mu.Unlock()

	if _, ok := c.nodes[nodeId]; !ok {
		return
	}
	if client, ok := c.nodes[nodeId]; ok {
		close(client.Send)
		delete(c.nodes, nodeId)
	}

	// Requeue unacknowledged events sent to this node
	for id, inflight := range c.inFlight {
		if inflight.NodeID == nodeId {
			if queued, _ := c.enqueueEventLocked(inflight.Event); queued {
				log.Println("Requeued event", id, "in memory after node disconnect", nodeId)
			} else {
				log.Println("Requeued event", id, "saved to store after node disconnect", nodeId)
			}
			delete(c.inFlight, id)

		}
	}

	log.Println("Node disconnected:", nodeId)
}
