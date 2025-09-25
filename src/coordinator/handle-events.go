package coordinator

import (
	"encoding/json"
	"log"
	"net/http"

	"coordinator/src/event"
)

func (c *Coordinator) HandleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var incoming event.Event[any]
	if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	// Broadcast to all connected nodes
	count, eventId := c.SendEvent(incoming) // <-- returns number of nodes message was sent to
	log.Printf("Event %s broadcasted to %d nodes\n", eventId, count)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "queued",
		"eventId": eventId,
	})
}
