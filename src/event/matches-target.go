package event

import "strings"

func MatchesTarget(target, nodeID string) bool {
	switch {
	case target == nodeID:
		// Exact fully qualified ID: "v1::nxasjds7"
		return true

	case strings.HasPrefix(target, "::"):
		// Identity selector: "::nxasjds7"
		return strings.HasSuffix(nodeID, target)

	case strings.HasSuffix(target, "::"):
		// Domain selector: "v1::"
		return strings.HasPrefix(nodeID, target)

	default:
		return false
	}
}
