package coordinator

import (
	"time"

	"coordinator/src/event"
	"coordinator/src/node"
	"coordinator/src/utils"

	"github.com/gorilla/websocket"
)

func NewCoordinator(upgrader *websocket.Upgrader, store *event.Store) *Coordinator {
	maxPending := utils.GetIntEnv("COORDINATOR_MAX_PENDING", 1000)
	return &Coordinator{
		nodes:      make(map[string]*node.Client),
		upgrader:   upgrader,
		inFlight:   make(map[string]event.InFlightEvent[any]),
		pending:    []event.Event[any]{},
		lastIndex:  0,
		maxPending: maxPending,
		store:      store,
		startTime:  time.Now(),
		wsTickets: NewWSTicketStore(),
	}
}
