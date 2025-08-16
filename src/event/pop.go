package event

import (
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

func (s *Store) Pop(now time.Time) (*Event, error) {
	var out *Event
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("events"))
		if b == nil {
			return fmt.Errorf("events bucket missing")
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var ev Event
			if err := json.Unmarshal(v, &ev); err != nil {
				// Corrupt entry → drop it and continue
				if delErr := b.Delete(k); delErr != nil {
					return delErr
				}
				continue
			}
			// TTL check (optional, but handy here)
			if !ev.CreatedAt.IsZero() && ev.TTL > 0 && now.After(ev.CreatedAt.Add(ev.TTL)) {
				// Expired → drop and continue
				if delErr := b.Delete(k); delErr != nil {
					return delErr
				}
				continue
			}
			// Found a valid oldest; delete and return it
			if err := b.Delete(k); err != nil {
				return err
			}
			tmp := ev
			out = &tmp
			return nil
		}
		return nil
	})
	return out, err
}
