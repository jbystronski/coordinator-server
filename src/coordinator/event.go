package coordinator

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type DeliveryMode string

const (
	Broadcast DeliveryMode = "broadcast"
	Single    DeliveryMode = "single"
)

type InFlightEvent struct {
	Event  Event
	NodeID string
	SentAt time.Time
}

type Event struct {
	ID        string        `json:"id"`
	Type      string        `json:"type"`
	Payload   any           `json:"payload"`
	Delivery  DeliveryMode  `json:"delivery"`
	CreatedAt time.Time     `json:"createdAt"`
	TTL       time.Duration `json:"_"`
}

type Coordinator struct {
	mu         sync.Mutex
	nodes      map[string]*NodeClient
	upgrader   *websocket.Upgrader
	pending    []Event
	inFlight   map[string]InFlightEvent
	lastIndex  int
	maxPending int
}

type NodeClient struct {
	id   string
	name string
	conn *websocket.Conn
	send chan []byte
}
