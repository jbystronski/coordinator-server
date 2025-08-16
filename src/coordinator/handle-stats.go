package coordinator

import (
	"encoding/json"
	"net/http"
	"time"
)

func (c *Coordinator) HandleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	storeCount, err := c.store.Count()
	if err != nil {
		http.Error(w, "failed to get store count", http.StatusInternalServerError)
		return
	}

	stats := map[string]any{
		"storeCount":    storeCount,
		"activeNodes":   len(c.nodes),
		"pendingCount":  len(c.pending),
		"timestamp":     time.Now().UTC().Format(time.RFC3339),
		"uptimeSeconds": int(time.Since(c.startTime).Seconds()),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}
