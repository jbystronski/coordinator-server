package coordinator

import (
	"encoding/json"
	"net/http"
)

func (c *Coordinator) HandleNodes(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var nodeList []map[string]string
	for _, node := range c.nodes {
		nodeList = append(nodeList, map[string]string{
			"id":   node.id,
			"name": node.name,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nodeList)
}
