package coordinator

import (
	"coordinator/src/node"
	"sort"
	"strings"
)

// electMasterLocked always elects a master from the currently connected
// nodes in the given domain. Caller must hold c.mu.
func (c *Coordinator) electMasterLocked(domain string) *node.Client {
	prefix := domain + "::"

	var candidates []*node.Client

	for id, client := range c.nodes {
		if strings.HasPrefix(id, prefix) {
			client.IsMaster = false
			candidates = append(candidates, client)
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].ID < candidates[j].ID
	})

	candidates[0].IsMaster = true

	return candidates[0]
}

// ensureMasterLocked preserves the current master, electing one only
// when the domain does not already have one. Caller must hold c.mu.
func (c *Coordinator) ensureMasterLocked(domain string) *node.Client {
	prefix := domain + "::"

	var candidates []*node.Client
	var currentMaster *node.Client

	for id, client := range c.nodes {
		if strings.HasPrefix(id, prefix) {
			candidates = append(candidates, client)

			if client.IsMaster {
				if currentMaster == nil || client.ID < currentMaster.ID {
					currentMaster = client
				}
			}
		}
	}

	if currentMaster != nil {
		// Normalize in case multiple masters somehow exist.
		for _, client := range candidates {
			client.IsMaster = client == currentMaster
		}
		return currentMaster
	}

	return c.electMasterLocked(domain)
}
