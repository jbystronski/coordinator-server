package event

import (
	"coordinator/src/utils"
	"encoding/json"
	"fmt"

	"go.etcd.io/bbolt"
)

func (s *Store) Put(ev Event) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("events"))
		if b == nil {
			return fmt.Errorf("events bucket missing")
		}
		seq, err := b.NextSequence()
		if err != nil {
			return err
		}
		return b.Put(utils.U64b(seq), data)
	})
}
