package main

import (
	"coordinator-v0/src/coordinator"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func main() {
	// ctx := context.Background()
	coordinator := coordinator.NewCoordinator(&upgrader)
	go coordinator.CleanupPendingLoop(10 * time.Minute)
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	http.HandleFunc("/ws", coordinator.HandleWS)
	http.HandleFunc("/nodes", coordinator.HandleNodes)
	http.HandleFunc("/events", coordinator.HandleEvents)

	log.Println("Coordinator v0 running on :8090")
	log.Fatal(http.ListenAndServe(":8090", nil))
}
