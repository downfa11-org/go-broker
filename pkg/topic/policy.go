package topic

import (
	"fmt"
	"strings"

	"github.com/cursus-io/cursus/pkg/config"
)

const (
	PartitionerHashKey    = "hash_key"
	PartitionerRoundRobin = "round_robin"
	AuthPolicyOpen        = "open"
	AuthPolicyDenyWrite   = "deny_write"
	AuthPolicyDenyRead    = "deny_read"
	AuthPolicyACL         = "acl"
)

type Policy struct {
	RetentionHours    int      `json:"retention_hours,omitempty"`
	RetentionBytes    int64    `json:"retention_bytes,omitempty"`
	CleanupPolicy     string   `json:"cleanup_policy"`
	Partitioner       string   `json:"partitioner"`
	AuthPolicy        string   `json:"auth_policy"`
	ReadACL           []string `json:"read_acl,omitempty"`
	WriteACL          []string `json:"write_acl,omitempty"`
	MinInSyncReplicas *int     `json:"min_in_sync_replicas,omitempty"`
}

func DefaultPolicy() Policy {
	return Policy{
		CleanupPolicy: config.CleanupPolicyDelete,
		Partitioner:   PartitionerHashKey,
		AuthPolicy:    AuthPolicyOpen,
	}
}

// ConsumerMetadataPolicy is broker-owned and cannot inherit application retention.
// Retention fields remain zero in the manifest for wire compatibility; DiskHandler
// interprets the internal topic as infinite-retention compact storage.
func ConsumerMetadataPolicy() Policy {
	return Policy{
		CleanupPolicy: config.CleanupPolicyCompact,
		Partitioner:   PartitionerHashKey,
		AuthPolicy:    AuthPolicyOpen,
	}
}

func (p Policy) Normalize() (Policy, error) {
	p = p.Clone()
	if p.CleanupPolicy == "" {
		p.CleanupPolicy = config.CleanupPolicyDelete
	}
	normalizedCleanup, ok := config.NormalizeCleanupPolicy(p.CleanupPolicy)
	if !ok {
		return p, fmt.Errorf("invalid cleanup policy %q", p.CleanupPolicy)
	}
	p.CleanupPolicy = normalizedCleanup

	if p.Partitioner == "" {
		p.Partitioner = PartitionerHashKey
	}
	p.Partitioner = strings.ToLower(strings.TrimSpace(p.Partitioner))
	switch p.Partitioner {
	case PartitionerHashKey, PartitionerRoundRobin:
	default:
		return p, fmt.Errorf("invalid partitioner %q", p.Partitioner)
	}

	if p.AuthPolicy == "" {
		p.AuthPolicy = AuthPolicyOpen
	}
	p.AuthPolicy = strings.ToLower(strings.TrimSpace(p.AuthPolicy))
	switch p.AuthPolicy {
	case AuthPolicyOpen, AuthPolicyDenyWrite, AuthPolicyDenyRead, AuthPolicyACL:
	default:
		return p, fmt.Errorf("invalid auth policy %q", p.AuthPolicy)
	}

	if p.RetentionHours < 0 {
		return p, fmt.Errorf("retention_hours must be >= 0")
	}
	if p.RetentionBytes < 0 {
		return p, fmt.Errorf("retention_bytes must be >= 0")
	}
	if p.MinInSyncReplicas != nil && *p.MinInSyncReplicas < 1 {
		return p, fmt.Errorf("min_in_sync_replicas must be >= 1")
	}
	return p, nil
}

// Clone returns a detached policy so optional values and ACL slices cannot
// alias durable topic state.
func (p Policy) Clone() Policy {
	p.ReadACL = append([]string(nil), p.ReadACL...)
	p.WriteACL = append([]string(nil), p.WriteACL...)
	if p.MinInSyncReplicas != nil {
		value := *p.MinInSyncReplicas
		p.MinInSyncReplicas = &value
	}
	return p
}

// EffectiveMinInSyncReplicas resolves the topic override against the broker
// default. Invalid broker defaults are treated as one at the usage boundary.
func (p Policy) EffectiveMinInSyncReplicas(brokerDefault int) int {
	if p.MinInSyncReplicas != nil {
		return *p.MinInSyncReplicas
	}
	if brokerDefault < 1 {
		return 1
	}
	return brokerDefault
}

func validateCleanupPolicyForTopic(policy Policy, cfg *config.Config, eventSourcing bool) error {
	if err := validateEventRetention(policy, eventSourcing); err != nil {
		return err
	}
	if !config.HasCleanupPolicy(policy.CleanupPolicy, config.CleanupPolicyCompact) {
		return nil
	}
	if eventSourcing {
		return fmt.Errorf("cleanup policy compact is not supported for event-sourcing topics")
	}
	return nil
}

func validateEventRetention(policy Policy, eventSourcing bool) error {
	if eventSourcing && (policy.RetentionHours > 0 || policy.RetentionBytes > 0) {
		return fmt.Errorf("retention limits are not supported for event-sourcing topics; complete event history is required")
	}
	return nil
}

func storagePolicyForTopic(policy Policy, eventSourcing bool) Policy {
	if eventSourcing {
		// Event indexes are rebuilt from the complete log, not from snapshots.
		policy.RetentionHours = -1
		policy.RetentionBytes = -1
	}
	return policy
}

func (p Policy) CanRead() bool {
	return p.AuthPolicy != AuthPolicyDenyRead
}

func (p Policy) CanWrite() bool {
	return p.AuthPolicy != AuthPolicyDenyWrite
}

func (p Policy) CanReadPrincipal(principal string) bool {
	if p.AuthPolicy == AuthPolicyDenyRead {
		return false
	}
	if p.AuthPolicy != AuthPolicyACL {
		return true
	}
	return aclContains(p.ReadACL, principal)
}

func (p Policy) CanWritePrincipal(principal string) bool {
	if p.AuthPolicy == AuthPolicyDenyWrite {
		return false
	}
	if p.AuthPolicy != AuthPolicyACL {
		return true
	}
	return aclContains(p.WriteACL, principal)
}

func aclContains(acl []string, principal string) bool {
	principal = strings.TrimSpace(principal)
	if principal == "" {
		return false
	}
	for _, item := range acl {
		item = strings.TrimSpace(item)
		if item == "*" || item == principal {
			return true
		}
	}
	return false
}
