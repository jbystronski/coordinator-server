package event

import "time"

type EventType string

const (
	ACK         EventType = "Ack"
	NodeMessage EventType = "NodeMessage"
	Ping        EventType = "Ping"
)

type Event[T any] struct {
	EventID   string        `json:"eventId"`
	Type      EventType     `json:"type"`
	Payload   T             `json:"payload"`
	Delivery  DeliveryMode  `json:"delivery"`
	CreatedAt time.Time     `json:"createdAt"`
	TTL       time.Duration `json:"ttl"`
}

type NodeMessageDirection string

const (
	Out NodeMessageDirection = "out"
	In  NodeMessageDirection = "in"
)

type NodeMessageContentType string

const (
	NodeTextMessage = "text"
	NodeFileMessage = "file"
)

type NodeMessageContent struct {
	Type     NodeMessageContentType `json:"type"`
	Content  string                 `json:"content"`
	MimeType string                 `json:"mime,omitempty"`
}

type NodeMessageEventPayload struct {
	FromNode     string               `json:"fromNode"`
	FromNodeName string               `json:"fromNodeName,omitempty"`
	ToNode       string               `json:"toNode"`
	Direction    NodeMessageDirection `json:"direction"`
	MessageID    string               `json:"messageId,omitempty"`
	Message      NodeMessageContent   `json:"message"`
	CreatedAt    time.Time            `json:"createdAt"`
	ReceivedAt   time.Time            `json:"receivedAt,omitempty"`
}

// AckPayload is the payload for Ack events.
type AckPayload struct {
	AckedEventID string `json:"ackedEventId"`
	// Status       string `json:"status"`
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
