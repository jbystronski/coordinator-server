package coordinator

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

const wsTicketTTL = 30 * time.Second

var (
	ErrInvalidWSTicket = errors.New("invalid or expired WebSocket ticket")
)

type wsTicket struct {
	nodeID    string
	expiresAt time.Time
}

type WSTicketStore struct {
	mu      sync.Mutex
	tickets map[string]wsTicket
}

func NewWSTicketStore() *WSTicketStore {
	return &WSTicketStore{
		tickets: make(map[string]wsTicket),
	}
}

// Issue creates a ticket bound to one specific node ID.
func (s *WSTicketStore) Issue(nodeID string) (string, error) {
	randomBytes := make([]byte, 32)

	if _, err := rand.Read(randomBytes); err != nil {
		return "", err
	}

	token := hex.EncodeToString(randomBytes)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.deleteExpiredLocked(time.Now())

	s.tickets[token] = wsTicket{
		nodeID:    nodeID,
		expiresAt: time.Now().Add(wsTicketTTL),
	}

	return token, nil
}

// Consume validates and deletes a ticket in one atomic operation.
func (s *WSTicketStore) Consume(token, nodeID string) error {
	if token == "" || nodeID == "" {
		return ErrInvalidWSTicket
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	ticket, exists := s.tickets[token]
	if !exists {
		return ErrInvalidWSTicket
	}

	// Always consume a presented ticket, even if its node ID is wrong.
	delete(s.tickets, token)

	if time.Now().After(ticket.expiresAt) || ticket.nodeID != nodeID {
		return ErrInvalidWSTicket
	}

	return nil
}

func (s *WSTicketStore) deleteExpiredLocked(now time.Time) {
	for token, ticket := range s.tickets {
		if now.After(ticket.expiresAt) {
			delete(s.tickets, token)
		}
	}
}
