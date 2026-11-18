// coordinator/ws_ticket_handler.go
package coordinator

import (
	"encoding/json"
	"net/http"

	"coordinator/src/event"
)

func (c *Coordinator) HandleWSTicket(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		NodeID string `json:"nodeId"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	if !event.ValidNodeID(request.NodeID) {
		http.Error(w, "invalid node id", http.StatusBadRequest)
		return
	}

	ticket, err := c.wsTickets.Issue(request.NodeID)
	if err != nil {
		http.Error(w, "could not issue ticket", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"ticket": ticket,
	})
}
