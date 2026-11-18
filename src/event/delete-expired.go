package event

import (
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

func (s *Store) DeleteExpired(now time.Time) (int, error) {
	deleted := 0

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
				deleted++
				continue
			}

			if IsExpired(ev,now) {
				if err := c.Delete(); err != nil {
					return err
				}
				deleted++
			}
		}

		return nil
	})

	return deleted, err
}
