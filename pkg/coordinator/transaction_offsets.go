package coordinator

import "fmt"

// WithTransactionPrepareFence pins ownership until the prepare decision is persisted.
func (c *Coordinator) WithTransactionPrepareFence(groupName, topic, member string, generation int, offsets []OffsetItem, prepare func(uint64) error) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.lifecyclePending[groupName] {
		return fmt.Errorf("group %q lifecycle update in progress", groupName)
	}
	if failure := c.validateMemberGenerationLocked(groupName, member, generation); failure != "" {
		return fmt.Errorf("%s", failure)
	}
	group := c.groups[groupName]
	group.mu.Lock()
	defer group.mu.Unlock()
	if err := validateOffsetBatchLocked(group, groupName, topic, offsets); err != nil {
		return err
	}
	for _, item := range offsets {
		if !contains(group.Members[member].Assignments, item.Partition) {
			return fmt.Errorf("ERROR: NOT_OWNER partition=%d member=%s group=%s generation=%d", item.Partition, member, groupName, generation)
		}
	}
	if group.RegistrationEpoch == 0 {
		return fmt.Errorf("group %q has no durable registration epoch", groupName)
	}
	return prepare(group.RegistrationEpoch)
}

// CommitPreparedOffsets replays an authorized prepare without rewinding newer commits.
func (c *Coordinator) CommitPreparedOffsets(groupName, topic string, epoch uint64, offsets []OffsetItem) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.lifecyclePending[groupName] {
		return fmt.Errorf("group %q lifecycle update in progress", groupName)
	}
	group := c.groups[groupName]
	if group == nil || epoch == 0 || group.RegistrationEpoch != epoch {
		return fmt.Errorf("prepared transaction group incarnation mismatch group=%s epoch=%d", groupName, epoch)
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	merged := append([]OffsetItem(nil), offsets...)
	for i := range merged {
		if current, ok := group.getOffsetSafe(topic, merged[i].Partition); ok && current > merged[i].Offset {
			merged[i].Offset = current
		}
	}
	return c.commitOffsetsBulkForGroupLocked(group, groupName, topic, merged)
}
