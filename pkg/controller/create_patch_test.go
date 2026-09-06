package controller

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/stretchr/testify/require"
)

func TestExplicitEmptyACLSurvivesDistributedPatchEncoding(t *testing.T) {
	patch, response := parseTopicDefinitionPatch(map[string]string{"read_acl": ""})
	require.Empty(t, response)
	require.NotNil(t, patch.ReadACL)

	payload, err := json.Marshal(patch)
	require.NoError(t, err)
	require.Contains(t, string(payload), `"read_acl":[]`)
	var restored topic.DefinitionPatch
	require.NoError(t, json.Unmarshal(payload, &restored))
	require.NotNil(t, restored.ReadACL)
	require.Empty(t, *restored.ReadACL)
}

func TestDistributedTopicCommandPayloadUsesCanonicalDefinitionPatch(t *testing.T) {
	current := topic.DefaultDefinition("orders", nil)
	current.Partitions = 2
	current.Policy.RetentionHours = 168
	current.Policy.ReadACL = []string{"reader"}
	retentionBytes := int64(8192)
	payload, err := distributedTopicCommandPayload(
		topic.DefaultDefinition("orders", nil),
		topic.DefinitionPatch{RetentionBytes: &retentionBytes},
		&current,
	)
	require.NoError(t, err)

	encoded, err := json.Marshal(payload)
	require.NoError(t, err)
	var command struct {
		Definition topic.Definition      `json:"definition"`
		Patch      topic.DefinitionPatch `json:"patch"`
	}
	require.NoError(t, json.Unmarshal(encoded, &command))
	require.Equal(t, "orders", command.Definition.Name)
	require.Equal(t, topic.DefaultPartitionCount, command.Definition.Partitions)
	require.NotNil(t, command.Patch.RetentionBytes)
	require.Equal(t, int64(8192), *command.Patch.RetentionBytes)
	require.NotContains(t, payload, "name")
	require.NotContains(t, payload, "partitions")
	require.NotContains(t, payload, "replication_factor")
	require.NotContains(t, payload, "policy")
	require.NotNil(t, payload["definition"])
	require.NotNil(t, payload["patch"])
}

func TestTopicDefinitionResponsesAppendFieldsAfterLegacyPrefix(t *testing.T) {
	handler, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)
	create := handler.HandleCommand("CREATE topic=compat partitions=1", ctx)
	require.Equal(t, []string{
		"OK", "topic", "partitions", "cleanup_policy", "partitioner", "auth_policy", "read_acl", "write_acl",
		"retention_hours", "retention_bytes", "revision", "replication_factor", "idempotent", "event_sourcing", "lifecycle_epoch",
		"min_in_sync_replicas", "effective_min_in_sync_replicas",
	}, responseFieldNames(create))

	metadata := handler.HandleCommand("METADATA topic=compat", ctx)
	require.Equal(t, []string{
		"OK", "topic", "partitions", "leaders", "epochs", "cleanup_policy", "partitioner", "auth_policy", "read_acl",
		"write_acl", "retention_hours", "retention_bytes", "revision", "replication_factor", "idempotent", "event_sourcing", "lifecycle_epoch",
		"min_in_sync_replicas", "effective_min_in_sync_replicas",
	}, responseFieldNames(metadata))
}

func responseFieldNames(response string) []string {
	fields := strings.Fields(response)
	result := make([]string, 0, len(fields))
	for _, field := range fields {
		name, _, _ := strings.Cut(field, "=")
		result = append(result, name)
	}
	return result
}

func TestRepeatedCreatePreservesOmittedTopicDefinitionFields(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	created := ch.HandleCommand(
		"CREATE topic=commerce.experiment.events partitions=1 replication_factor=3 "+
			"idempotent=true event_sourcing=false cleanup_policy=delete retention_hours=168 "+
			"retention_bytes=4096 partitioner=round_robin auth_policy=acl "+
			"read_acl=commerce-reader write_acl=commerce-writer",
		ctx,
	)
	require.True(t, strings.HasPrefix(created, "OK "), created)

	repeated := ch.HandleCommand("CREATE topic=commerce.experiment.events partitions=1", ctx)
	require.True(t, strings.HasPrefix(repeated, "OK "), repeated)

	definition := ch.TopicManager.GetTopic("commerce.experiment.events").Definition()
	require.Equal(t, 1, definition.Partitions)
	require.Equal(t, 3, definition.ReplicationFactor)
	require.True(t, definition.Idempotent)
	require.False(t, definition.EventSourcing)
	require.Equal(t, config.CleanupPolicyDelete, definition.Policy.CleanupPolicy)
	require.Equal(t, 168, definition.Policy.RetentionHours)
	require.Equal(t, int64(4096), definition.Policy.RetentionBytes)
	require.Equal(t, topic.PartitionerRoundRobin, definition.Policy.Partitioner)
	require.Equal(t, topic.AuthPolicyACL, definition.Policy.AuthPolicy)
	require.Equal(t, []string{"commerce-reader"}, definition.Policy.ReadACL)
	require.Equal(t, []string{"commerce-writer"}, definition.Policy.WriteACL)
	require.Equal(t, uint64(1), definition.Revision, "a no-op create must not advance the definition revision")

	for _, field := range []string{
		"revision=1",
		"partitions=1",
		"replication_factor=3",
		"idempotent=true",
		"event_sourcing=false",
		"retention_hours=168",
		"retention_bytes=4096",
		"partitioner=round_robin",
		"auth_policy=acl",
		"read_acl=commerce-reader",
		"write_acl=commerce-writer",
	} {
		require.Contains(t, repeated, field)
	}

	metadata := ch.HandleCommand("METADATA topic=commerce.experiment.events", ctx)
	for _, field := range []string{
		"revision=1",
		"replication_factor=3",
		"idempotent=true",
		"event_sourcing=false",
		"retention_hours=168",
	} {
		require.Contains(t, metadata, field)
	}
}

func TestRepeatedCreateChangesOnlyExplicitPolicyFields(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand(
		"CREATE topic=state partitions=2 cleanup_policy=compact retention_hours=24 "+
			"retention_bytes=2048 partitioner=round_robin auth_policy=acl "+
			"read_acl=reader write_acl=writer",
		ctx,
	)
	require.True(t, strings.HasPrefix(resp, "OK "), resp)

	resp = ch.HandleCommand("CREATE topic=state cleanup_policy=delete", ctx)
	require.True(t, strings.HasPrefix(resp, "OK "), resp)
	definition := ch.TopicManager.GetTopic("state").Definition()
	require.Equal(t, config.CleanupPolicyDelete, definition.Policy.CleanupPolicy)
	require.Equal(t, 24, definition.Policy.RetentionHours)
	require.Equal(t, int64(2048), definition.Policy.RetentionBytes)
	require.Equal(t, topic.PartitionerRoundRobin, definition.Policy.Partitioner)
	require.Equal(t, topic.AuthPolicyACL, definition.Policy.AuthPolicy)
	require.Equal(t, []string{"reader"}, definition.Policy.ReadACL)
	require.Equal(t, []string{"writer"}, definition.Policy.WriteACL)
	require.Equal(t, uint64(2), definition.Revision)
}

func TestRepeatedCreateDistinguishesExplicitZeroFalseAndEmptyACL(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)

	resp := ch.HandleCommand(
		"CREATE topic=state partitions=1 idempotent=true retention_hours=168 "+
			"retention_bytes=4096 auth_policy=acl read_acl=reader write_acl=writer",
		ctx,
	)
	require.True(t, strings.HasPrefix(resp, "OK "), resp)

	resp = ch.HandleCommand("CREATE topic=state retention_hours=0 retention_bytes=0 read_acl=", ctx)
	require.True(t, strings.HasPrefix(resp, "OK "), resp)
	definition := ch.TopicManager.GetTopic("state").Definition()
	require.Zero(t, definition.Policy.RetentionHours)
	require.Zero(t, definition.Policy.RetentionBytes)
	require.Empty(t, definition.Policy.ReadACL)
	require.Equal(t, []string{"writer"}, definition.Policy.WriteACL, "omitted ACL must be preserved")
	require.True(t, definition.Idempotent, "omitted false-valued field must be preserved")

	resp = ch.HandleCommand("CREATE topic=state idempotent=false", ctx)
	require.Contains(t, resp, "create_topic_failed")
	require.Contains(t, resp, "idempotent")
	require.True(t, ch.TopicManager.GetTopic("state").Definition().Idempotent)

	resp = ch.HandleCommand("CREATE topic=state replication_factor=2", ctx)
	require.Contains(t, resp, "create_topic_failed")
	require.Contains(t, resp, "replication_factor")
	require.Equal(t, 3, ch.TopicManager.GetTopic("state").Definition().ReplicationFactor)

	resp = ch.HandleCommand("CREATE topic=state event_sourcing=not-a-bool", ctx)
	require.Contains(t, resp, "invalid_event_sourcing")
}

func TestRepeatedCreateWithoutPartitionsPreservesExistingCount(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)
	require.True(t, strings.HasPrefix(ch.HandleCommand("CREATE topic=wide partitions=6 retention_hours=12", ctx), "OK "))

	resp := ch.HandleCommand("CREATE topic=wide retention_bytes=1024", ctx)
	require.True(t, strings.HasPrefix(resp, "OK "), resp)
	definition := ch.TopicManager.GetTopic("wide").Definition()
	require.Equal(t, 6, definition.Partitions)
	require.Equal(t, 12, definition.Policy.RetentionHours)
	require.Equal(t, int64(1024), definition.Policy.RetentionBytes)
}

func TestConcurrentRepeatedCreateDoesNotLoseDisjointPatches(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)
	require.True(t, strings.HasPrefix(ch.HandleCommand("CREATE topic=concurrent partitions=1 retention_hours=168", ctx), "OK "))

	commands := []string{
		"CREATE topic=concurrent retention_bytes=8192",
		"CREATE topic=concurrent auth_policy=acl read_acl=reader write_acl=writer",
	}
	responses := make([]string, len(commands))
	var wg sync.WaitGroup
	for i, command := range commands {
		wg.Add(1)
		go func() {
			defer wg.Done()
			responses[i] = ch.HandleCommand(command, NewClientContext("", 0))
		}()
	}
	wg.Wait()
	for _, response := range responses {
		require.True(t, strings.HasPrefix(response, "OK "), response)
	}

	definition := ch.TopicManager.GetTopic("concurrent").Definition()
	require.Equal(t, 168, definition.Policy.RetentionHours)
	require.Equal(t, int64(8192), definition.Policy.RetentionBytes)
	require.Equal(t, topic.AuthPolicyACL, definition.Policy.AuthPolicy)
	require.Equal(t, []string{"reader"}, definition.Policy.ReadACL)
	require.Equal(t, []string{"writer"}, definition.Policy.WriteACL)
	require.Equal(t, uint64(3), definition.Revision)
}

func TestRepeatedCreateStillRejectsCompactEventSourcingTopic(t *testing.T) {
	ch, _ := newTestHandler(t)
	ctx := NewClientContext("", 0)
	require.True(t, strings.HasPrefix(ch.HandleCommand(
		"CREATE topic=events partitions=1 event_sourcing=true cleanup_policy=delete",
		ctx,
	), "OK "))

	resp := ch.HandleCommand("CREATE topic=events cleanup_policy=compact", ctx)
	require.Contains(t, resp, "invalid_topic_policy")
	definition := ch.TopicManager.GetTopic("events").Definition()
	require.True(t, definition.EventSourcing)
	require.Equal(t, config.CleanupPolicyDelete, definition.Policy.CleanupPolicy)
	require.Zero(t, definition.Policy.RetentionHours)
}
