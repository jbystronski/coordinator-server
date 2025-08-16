package event

import "time"

type Event struct {
	ID        string        `json:"id"`
	Type      string        `json:"type"`
	Payload   any           `json:"payload"`
	Delivery  DeliveryMode  `json:"delivery"`
	CreatedAt time.Time     `json:"createdAt"`
	TTL       time.Duration `json:"_"`
}

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
