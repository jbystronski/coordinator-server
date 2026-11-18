package coordinator

import "strings"

func domainOf(nodeID string) (string, bool) {
	domain, _, ok := strings.Cut(nodeID, "::")
	return domain, ok && domain != ""
}
