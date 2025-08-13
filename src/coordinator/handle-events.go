package coordinator

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
)

func (c *Coordinator) HandleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var incoming Event
	if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	// Assign a new ID if not provided
	if incoming.ID == "" {
		incoming.ID = uuid.New().String()
	}

	incoming.CreatedAt = time.Now()
	incoming.TTL = 6 * time.Hour

	log.Println("New event queued:", incoming.ID, incoming.Type)

	// Broadcast to all connected nodes
	count := c.SendEvent(incoming) // <-- returns number of nodes message was sent to
	log.Printf("Event %s broadcasted to %d nodes\n", incoming.ID, count)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "queued",
		"id":     incoming.ID,
	})
}
