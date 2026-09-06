package transaction

import "time"

// RuntimeSnapshot is a point-in-time, read-only view used by diagnostics.
type RuntimeSnapshot struct {
	Total                  int
	ByState                map[State]int
	Expired                int
	OldestActiveAgeSeconds float64
	RecoveryReady          bool
	RetainedBytes          int64
}

func (m *Manager) RuntimeSnapshot() RuntimeSnapshot {
	result := RuntimeSnapshot{ByState: make(map[State]int), RecoveryReady: true}
	if m == nil {
		result.RecoveryReady = false
		return result
	}
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	result.RetainedBytes = m.totalBytes
	for _, tx := range m.txns {
		if tx == nil {
			continue
		}
		result.Total++
		if tx.Expired {
			result.Expired++
			continue
		}
		result.ByState[tx.State]++
		if tx.State == StateOpen || tx.State == StateCommitting {
			age := now.Sub(tx.UpdatedAt).Seconds()
			if age > result.OldestActiveAgeSeconds {
				result.OldestActiveAgeSeconds = age
			}
		}
	}
	return result
}
