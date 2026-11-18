package event

import "strings"


func ValidNodeID(id string) bool {
	domain, nodeID, ok := strings.Cut(id, "::")
	return ok &&
		domain != "" &&
		nodeID != "" &&
		!strings.Contains(nodeID, "::") &&
		!strings.Contains(domain, ":")
}
