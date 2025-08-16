package event

func (s *Store) Close() error { return s.db.Close() }
