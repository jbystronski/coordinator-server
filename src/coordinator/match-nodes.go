package coordinator

import (
	"sort"
	"strings"

	"coordinator/src/event"
	"coordinator/src/node"
)




func (c *Coordinator) matchingNodes(targets []string) []*node.Client {
	c.mu.Lock()
	defer c.mu.Unlock()

	matched := make(map[string]*node.Client)

	for _, target := range targets {
		switch {
		case strings.HasSuffix(target, "::"):
			// Domain selector: "v1::"
			for id, client := range c.nodes {
				if strings.HasPrefix(id, target) {
					matched[id] = client
				}
			}

		case strings.HasPrefix(target, "::"):
			// Identity selector: "::nxasjds7"
			for id, client := range c.nodes {
				if strings.HasSuffix(id, target) {
					matched[id] = client
				}
			}

		default:
			// Fully qualified ID: "v1::nxasjds7"
			if client, ok := c.nodes[target]; ok {
				matched[target] = client
			}
		}
	}

	clients := make([]*node.Client, 0, len(matched))
	for _, client := range matched {
		clients = append(clients, client)
	}

	sort.Slice(clients, func(i, j int) bool {
		return clients[i].ID < clients[j].ID
	})

	return clients
}




func matchesNode(
	ev event.Event[any],
	client *node.Client,
) bool {
	for _, target := range ev.Meta.Targets {

		if event.MatchesTarget(target, client.ID) {
			return true
		}


	}

	return false
}
