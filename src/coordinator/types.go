package coordinator

import (
	"coordinator/src/event"
	"coordinator/src/node"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Coordinator struct {
	mu         sync.Mutex
	nodes      map[string]*node.Client
	upgrader   *websocket.Upgrader
	pending    []event.Event
	inFlight   map[string]event.InFlightEvent
	lastIndex  int
	maxPending int
	store      *event.Store
	startTime  time.Time
}

type NodeMessage struct {
	Type     string `json:"type"`
	FromNode string `json:"fromNode"`
	ToNode   string `json:"toNode"`
	EventID  string `json:"eventId,omitempty"`
	Payload  string `json:"payload"`
	Delivery string `json:"delivery,omitempty"`
}
