package event

import (
	"encoding/json"
	"fmt"

	"coordinator/src/utils"

	"go.etcd.io/bbolt"
)

func (s *Store) Put(ev Event[any]) error {
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
