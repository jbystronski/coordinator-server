package event

import (
	"time"
)
func IsExpired(
	ev Event[any],
	now time.Time,
) bool {
	return !ev.Meta.Timestamp.IsZero() &&
		ev.Meta.TTL > 0 &&
		!now.Before(ev.Meta.Timestamp.Add(ev.Meta.TTL))
}
