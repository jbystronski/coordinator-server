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
		log.Printf("Decode event JSON failed: %v", err)
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	count, eventID, err := c.SendEvent(incoming)

	if err != nil {
		log.Printf("Failed to accept event %s: %v", eventID, err)
		http.Error(w, "failed to queue event", http.StatusInternalServerError)
		return
	}

	log.Printf("Event %s sent to %d nodes", eventID, count)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)

	if err := json.NewEncoder(w).Encode(map[string]string{
		"status":  "accepted",
		"eventId": eventID,
	}); err != nil {
		log.Printf("Failed to write event response: %v", err)
	}
}
