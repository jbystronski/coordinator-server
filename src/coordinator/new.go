package coordinator

import (
	"github.com/gorilla/websocket"
)

func NewCoordinator(upgrader *websocket.Upgrader) *Coordinator {
	return &Coordinator{
		nodes:      make(map[string]*NodeClient),
		upgrader:   upgrader,
		inFlight:   make(map[string]InFlightEvent),
		pending:    []Event{},
		lastIndex:  0,
		maxPending: 1000,
	}
}
