package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"coordinator/src/coordinator"
	"coordinator/src/event"
	"coordinator/src/middleware"
	"coordinator/src/utils"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func main() {
	store, err := event.NewEventStore("coordinator_events.db")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	apiKey := os.Getenv("COORDINATOR_API_KEY")

	if apiKey == "" {
		log.Fatal("COORDINATOR_API_KEY is required")
	}

	coordinator := coordinator.NewCoordinator(&upgrader, store)

	coordinator.StartTrimScheduler(utils.GetIntEnv("COORDINATOR_TRIM_KEEP_COUNT", 500), coordinator.GetTrimIntervalInMinutes())


	coordinator.StartExpiredEventCleanup(time.Minute)

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	http.Handle("/ws", middleware.AuthorizeByToken(apiKey, http.HandlerFunc(coordinator.HandleWS)))


	http.Handle(
		"/ws-ticket",
		middleware.AuthorizeByToken(
			apiKey,
			http.HandlerFunc(coordinator.HandleWSTicket),
		),
	)

	http.Handle("/ws-browser", http.HandlerFunc(coordinator.HandleWSBrowser))

	http.Handle("/nodes", middleware.AuthorizeByToken(apiKey, http.HandlerFunc(coordinator.HandleNodes)))
	http.Handle("/events", middleware.AuthorizeByToken(apiKey, http.HandlerFunc(coordinator.HandleEvents)))
	http.Handle("/stats", middleware.AuthorizeByToken(apiKey, http.HandlerFunc(coordinator.HandleStats)))





	log.Println("Coordinator v0 running on :8090")
	log.Fatal(http.ListenAndServe(":8090", nil))
}
