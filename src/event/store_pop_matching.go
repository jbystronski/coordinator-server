package event

import (
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// PopMatchingForNode removes and returns the oldest valid Single event
// addressable to the given node.
//
// Expired, corrupt, and legacy non-Single entries are removed.
// Events for other nodes remain in the store.
func (s *Store) PopMatchingForNode(
	nodeID string,
	now time.Time,
) (*Event[any], error) {
	if nodeID == "" {
		return nil, fmt.Errorf("node ID cannot be empty")
	}

	var out *Event[any]

	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("events"))
		if b == nil {
			return fmt.Errorf("events bucket missing")
		}

		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			var ev Event[any]

			if err := json.Unmarshal(v, &ev); err != nil {
				// Corrupt event: it cannot be delivered.
				if err := c.Delete(); err != nil {
					return err
				}
				continue
			}

			// Remove expired events regardless of destination.
			if 	IsExpired(ev,now)  {
				if err := c.Delete(); err != nil {
					return err
				}
				continue
			}

			// The pending queue is only for deferred Single delivery.
			// Remove legacy entries that should not be in this queue.
			if ev.Meta.Delivery != Single {
				if err := c.Delete(); err != nil {
					return err
				}
				continue
			}

			// Explicit targets take precedence over contract.
			matches := false



			for _, target := range ev.Meta.Targets {
				if MatchesTarget(target, nodeID) {
					matches = true
					break
				}
			}


			if !matches {
				continue
			}

			// Delete and return the oldest matching event.
			if err := c.Delete(); err != nil {
				return err
			}

			out = &ev
			return nil
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return out, nil
}
