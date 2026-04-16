package node

import (
	"context"
	"sync"
	"time"

	"github.com/nikitakosatka/hive/pkg/hive"
)

// Version is a logical LWW version for one key.
// Ordering is lexicographic: (Counter, NodeID).
type Version struct {
	Counter uint64
	NodeID  string
}

func (v Version) isNewerThan(other Version) bool {
	if v.Counter != other.Counter {
		return v.Counter > other.Counter
	}
	return v.NodeID > other.NodeID
}

// StateEntry stores one OR-Map key state.
type StateEntry struct {
	Value     string
	Tombstone bool
	Version   Version
}

// MapState is an exported snapshot representation used by Merge.
type MapState map[string]StateEntry

// CRDTMapNode is a state-based OR-Map with LWW values.
type CRDTMapNode struct {
	*hive.BaseNode
	mu         sync.RWMutex
	state      MapState
	counter    uint64
	allNodeIDs []string
}

// NewCRDTMapNode creates a CRDT map node for the provided peer set.
func NewCRDTMapNode(id string, allNodeIDs []string) *CRDTMapNode {
	return &CRDTMapNode{
		BaseNode:   hive.NewBaseNode(id),
		state:      make(MapState),
		allNodeIDs: allNodeIDs,
	}
}

// Start starts message processing and anti-entropy broadcast (flood/gossip).
func (n *CRDTMapNode) Start(ctx context.Context) error {
	if err := n.BaseNode.Start(ctx); err != nil {
		return err
	}
	go n.antiEntropy(n.Context())
	return nil
}

func (n *CRDTMapNode) antiEntropy(ctx context.Context) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snapshot := n.State()
			for _, peer := range n.allNodeIDs {
				if peer == n.ID() {
					continue
				}
				_ = n.Send(peer, snapshot)
			}
		}
	}
}

func (n *CRDTMapNode) nextVersion() Version {
	n.counter++
	return Version{Counter: n.counter, NodeID: n.ID()}
}

// Put writes a value with a fresh local version.
func (n *CRDTMapNode) Put(k, v string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state[k] = StateEntry{
		Value:     v,
		Tombstone: false,
		Version:   n.nextVersion(),
	}
}

// Get returns the current visible value for key k.
func (n *CRDTMapNode) Get(k string) (string, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	e, ok := n.state[k]
	if !ok || e.Tombstone {
		return "", false
	}
	return e.Value, true
}

// Delete marks the key as removed via a tombstone.
func (n *CRDTMapNode) Delete(k string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state[k] = StateEntry{
		Tombstone: true,
		Version:   n.nextVersion(),
	}
}

// Merge joins local state with a remote state snapshot.
func (n *CRDTMapNode) Merge(remote MapState) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for k, remoteEntry := range remote {
		localEntry, exists := n.state[k]
		if !exists || remoteEntry.Version.isNewerThan(localEntry.Version) {
			n.state[k] = remoteEntry
		}
	}
}

// State returns a copy of the full CRDT state.
func (n *CRDTMapNode) State() MapState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	snapshot := make(MapState, len(n.state))
	for k, v := range n.state {
		snapshot[k] = v
	}
	return snapshot
}

// ToMap returns a value-only map view without tombstoned keys.
func (n *CRDTMapNode) ToMap() map[string]string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	result := make(map[string]string)
	for k, e := range n.state {
		if !e.Tombstone {
			result[k] = e.Value
		}
	}
	return result
}

// Receive applies remote state snapshots.
func (n *CRDTMapNode) Receive(msg *hive.Message) error {
	if payload, ok := msg.Payload.(MapState); ok {
		n.Merge(payload)
	}
	return nil
}
