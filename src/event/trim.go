package event

import (
	"log"

	bolt "go.etcd.io/bbolt"
)

func (s *Store) Trim(keepCount int) error {
	const batchSize = 1000 // how many to delete per transaction

	stats := func() (keyCount int, err error) {
		err = s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("events"))
			if b != nil {
				keyCount = b.Stats().KeyN
			}
			return nil
		})
		return
	}

	keyCount, err := stats()
	if err != nil {
		return err
	}

	extra := keyCount - keepCount
	if extra <= 0 {
		return nil // nothing to trim
	}

	deletedTotal := 0
	for extra > 0 {
		toDelete := min(extra, batchSize)

		err := s.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("events"))
			if b == nil {
				return nil
			}

			c := b.Cursor()
			count := 0
			for k, _ := c.First(); k != nil && count < toDelete; k, _ = c.Next() {
				if err := c.Delete(); err != nil {
					log.Println("failed to delete old event key:", string(k), err)
				} else {
					count++
				}
			}

			deletedTotal += count
			return nil
		})
		if err != nil {
			return err
		}

		extra -= toDelete
	}

	log.Printf("Trimmed %d old events from Bolt queue\n", deletedTotal)
	return nil
}
