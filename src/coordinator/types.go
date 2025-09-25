package coordinator

import (
	"sync"
	"time"

	"coordinator/src/event"
	"coordinator/src/node"

	"github.com/gorilla/websocket"
)

type Coordinator struct {
	mu         sync.Mutex
	nodes      map[string]*node.Client
	upgrader   *websocket.Upgrader
	pending    []event.Event[any]
	inFlight   map[string]event.InFlightEvent[any]
	lastIndex  int
	maxPending int
	store      *event.Store
	startTime  time.Time
}
