package event

import "time"

type EventType string

const (
	ACK           EventType = "Ack"
	Ping          EventType = "Ping"
	MasterSet     EventType = "MasterSet"
)

type Meta struct {
	Timestamp      time.Time     `json:"timestamp"`
	TTL            time.Duration `json:"ttl"`
	Delivery       DeliveryMode  `json:"deliveryType"`
	ID             string        `json:"id"`
	Status         string        `json:"status"`
	OriginNodeID string `json:"originNodeId,omitempty"`
	Targets []string `json:"targets"`

}

type Event[T any] struct {
	Meta    Meta      `json:"meta"`
	Type    EventType `json:"type"`
	Payload T         `json:"payload"`
}


// AckPayload is the payload for Ack events.
type AckPayload struct {
	AckedEventID string `json:"ackedEventId"`
	// Status       string `json:"status"`
}

type MasterSetPayload struct {
	NodeID string `json:"nodeId"`
}


type PingPayload struct {
	Timestamp time.Time `json:"timestamp"`
}

type DeliveryMode string

const (
	Broadcast DeliveryMode = "broadcast"
	Single    DeliveryMode = "single"
)

type InFlightEvent[T any] struct {
	Event  Event[T]
	NodeID string
	SentAt time.Time
}
