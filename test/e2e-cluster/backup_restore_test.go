package e2e_cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/sdk"
	"github.com/cursus-io/cursus/test/e2e"
	"github.com/stretchr/testify/require"
)

type backupFixture struct {
	compose, root             string
	containers, addrs, health []string
}

func TestFullPersistenceBackupRestore(t *testing.T) {
	if os.Getenv("RUN_E2E_BACKUP_RESTORE") != "1" {
		t.Skip("set RUN_E2E_BACKUP_RESTORE=1 to recreate isolated test containers from full backups")
	}
	for _, mode := range []string{"standalone", "cluster"} {
		t.Run(mode, func(t *testing.T) {
			fixture := backupFixture{compose: "../e2e/docker-compose.yml", root: "broker-logs", containers: []string{"broker"}, addrs: []string{"localhost:10000"}, health: e2e.StandAloneHealthCheckAddr}
			if mode == "cluster" {
				fixture = backupFixture{compose: composeFile, root: "cluster-logs", containers: []string{"broker-1", "broker-2", "broker-3"}, addrs: clusterBrokerAddrs(3), health: clusterHealthCheckAddrs(3)}
			}
			fixture.start(t)
			client := e2e.NewBrokerClient(fixture.addrs)
			defer client.Close()
			if mode == "cluster" {
				requireClusterLifecycleProtocolEventually(t, fixture.addrs, fsm.PreparedTransactionProtocolVersion)
			}
			const topic = "backup-messages"
			const group = "backup-offsets"
			require.NoError(t, client.CreateTopic(topic, 2, true))
			fixture.waitTopic(t, topic)
			for partition := 0; partition < 2; partition++ {
				for sequence := uint64(1); sequence <= 10; sequence++ {
					require.NoError(t, client.PublishIdempotentToPartition(topic, "backup-producer", partition, sequence, 1, fmt.Sprintf("p%d-%d", partition, sequence), "all", true))
				}
			}
			generation, member, err := client.JoinGroup(topic, group)
			require.NoError(t, err)
			_, err = client.SyncGroup(topic, group, generation, member)
			require.NoError(t, err)
			committed := seedBackupTransaction(t, client, "backup-committed", topic)
			require.NoError(t, client.SendOffsetsToTransaction("backup-committed", topic, group, member, generation, committed, map[int]uint64{0: 11, 1: 11}))
			require.NoError(t, client.EndTransaction("backup-committed", committed, "commit"))
			open := seedBackupTransaction(t, client, "backup-open", topic)
			events := sdk.NewEventStore(fixture.addrs[0], "backup-events", "event-producer")
			require.NoError(t, events.CreateTopic(1))
			fixture.waitTopic(t, "backup-events")
			for version := uint64(1); version <= 3; version++ {
				_, err := events.Append("order", version, &sdk.Event{Type: "Changed", Payload: fmt.Sprintf("event-%d", version)})
				require.NoError(t, err)
			}
			require.NoError(t, events.SaveSnapshot("order", 2, "snapshot-2"))
			require.NoError(t, events.Close())
			client.Close()

			fixture.restoreIntoFreshContainers(t)
			fixture.waitTopic(t, topic)
			fixture.waitTopic(t, "backup-events")
			if mode == "cluster" {
				requireClusterLifecycleProtocolEventually(t, fixture.addrs, fsm.PreparedTransactionProtocolVersion)
			}
			restored := e2e.NewBrokerClient(fixture.addrs)
			defer restored.Close()
			status, err := restored.GetTransactionStatus("backup-committed")
			require.NoError(t, err)
			require.Equal(t, "committed", status.State)
			require.NoError(t, restored.EndTransaction("backup-committed", committed, "commit"))
			status, err = restored.GetTransactionStatus("backup-open")
			require.NoError(t, err)
			require.Equal(t, "open", status.State)
			require.Equal(t, 2, status.Messages)
			require.NoError(t, restored.EndTransaction("backup-open", open, "abort"))
			reader, readGeneration, readMember := joinClusterGroup(t, fixture.addrs, topic, "backup-readback")
			defer reader.Close()
			for partition := 0; partition < 2; partition++ {
				offset, err := restored.FetchCommittedOffset(topic, partition, group)
				require.NoError(t, err)
				require.Equal(t, uint64(11), offset)
				require.NoError(t, restored.PublishIdempotentToPartition(topic, "backup-producer", partition, 10, 1, fmt.Sprintf("p%d-10", partition), "all", true))
				messages := consumeFromPartitionLeader(t, fixture.addrs, topic, partition, "backup-readback", readMember, readGeneration)
				expected := make([]string, 0, 11)
				for sequence := 1; sequence <= 10; sequence++ {
					expected = append(expected, fmt.Sprintf("p%d-%d", partition, sequence))
				}
				expected = append(expected, fmt.Sprintf("backup-committed-p%d", partition))
				require.Equal(t, expected, messages)
				require.NoError(t, restored.PublishIdempotentToPartition(topic, "backup-producer", partition, 11, 1, "post-restore", "all", true))
			}
			recoveredEvents := sdk.NewEventStore(fixture.addrs[0], "backup-events", "event-producer")
			defer func() { require.NoError(t, recoveredEvents.Close()) }()
			data, err := recoveredEvents.ReadStream("order")
			require.NoError(t, err)
			require.Equal(t, uint64(3), data.StreamVersion)
			require.Equal(t, &sdk.Snapshot{Version: 2, Payload: "snapshot-2"}, data.Snapshot)
			require.NotEmpty(t, data.Events)
			require.Equal(t, "event-3", data.Events[len(data.Events)-1].Payload)
			appended, err := recoveredEvents.Append("order", 4, &sdk.Event{Type: "Changed", Payload: "event-4"})
			require.NoError(t, err)
			require.Equal(t, uint64(4), appended.Version)
		})
	}
}

func seedBackupTransaction(t *testing.T, client *e2e.BrokerClient, id, topic string) e2e.TransactionProducer {
	t.Helper()
	producer, err := client.InitTransactionProducer(id)
	require.NoError(t, err)
	require.NoError(t, client.BeginTransaction(id, producer))
	for partition := 0; partition < 2; partition++ {
		require.NoError(t, client.TransactionalPublish(id, topic, partition, producer, 1, fmt.Sprintf("%s-p%d", id, partition)))
	}
	return producer
}

func (f backupFixture) waitTopic(t *testing.T, topic string) {
	t.Helper()
	if len(f.containers) == 1 {
		return
	}
	ctx := GivenCluster(t).WithTopic(topic)
	t.Cleanup(func() { ctx.GetClient().Close() })
	ctx.WhenCluster().WaitForTopicMetadata()
	waitForStableFullISRAndZeroUnderReplicated(t, ctx, "backup fixture "+topic)
}

func (f backupFixture) composeCommand(t *testing.T, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	command := e2e.RunComposeContext(ctx, append([]string{"-p", "cursus-backup-restore", "-f", f.compose}, args...)...)
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
	return output
}

func backupDocker(t *testing.T, input io.Reader, output io.Writer, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "docker", args...)
	var stderr, stdout bytes.Buffer
	command.Stdin, command.Stderr, command.Stdout = input, &stderr, &stdout
	if output != nil {
		command.Stdout = output
	}
	err := command.Run()
	require.NoError(t, err, "docker %v: %s", args, stderr.String())
	return stdout.Bytes()
}

func (f backupFixture) start(t *testing.T) {
	t.Helper()
	containers := strings.Fields(string(backupDocker(t, nil, nil, "ps", "-a", "--format", "{{.Names}}")))
	for _, name := range f.containers {
		require.NotContains(t, containers, name, "refusing to replace an existing container")
	}
	project := backupDocker(t, nil, nil, "ps", "-aq", "--filter", "label=com.docker.compose.project=cursus-backup-restore")
	require.Empty(t, strings.TrimSpace(string(project)), "refusing to modify an existing backup test project")
	networks := strings.Fields(string(backupDocker(t, nil, nil, "network", "ls", "--format", "{{.Name}}")))
	network := "test_network"
	if len(f.containers) > 1 {
		network = "cluster_network"
	}
	require.NotContains(t, networks, network, "refusing to reuse an existing test network")
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("backup fixture logs: %s", f.composeCommand(t, "logs", "--no-color", "--tail", "100"))
		}
		f.composeCommand(t, "down", "--remove-orphans")
	})
	f.composeCommand(t, "up", "-d", "--build")
	require.NoError(t, e2e.CheckBrokerHealth(f.health))
}

func (f backupFixture) restoreIntoFreshContainers(t *testing.T) {
	t.Helper()
	f.composeCommand(t, "stop", "-t", "45")
	dir := t.TempDir()
	backups := make(map[string]map[string]backupEntry)
	ids := make(map[string]string)
	for _, name := range f.containers {
		state := backupDocker(t, nil, nil, "inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}", name)
		require.Equal(t, "exited 0", strings.TrimSpace(string(state)), "backup requires successful shutdown")
		ids[name] = string(backupDocker(t, nil, nil, "inspect", "--format", "{{.Id}}", name))
		file, err := os.OpenFile(filepath.Join(dir, name+".tar"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		require.NoError(t, err)
		t.Cleanup(func() { _ = file.Close() })
		backupDocker(t, nil, file, "cp", name+":/app/"+f.root, "-")
		require.NoError(t, file.Sync())
		_, err = file.Seek(0, io.SeekStart)
		require.NoError(t, err)
		backups[name], err = inspectBackup(file, f.root)
		require.NoError(t, err)
		if len(f.containers) > 1 {
			require.Contains(t, backups[name], f.root+"/raft")
		} else {
			require.Contains(t, backups[name], f.root+"/"+config.TopicMetadataFileName)
		}
		require.NoError(t, file.Close())
	}
	f.composeCommand(t, "down", "--remove-orphans")
	f.composeCommand(t, "create")
	for _, name := range f.containers {
		id := backupDocker(t, nil, nil, "inspect", "--format", "{{.Id}}", name)
		require.NotEqual(t, ids[name], string(id), "restore must use a fresh container")
		file, err := os.Open(filepath.Join(dir, name+".tar"))
		require.NoError(t, err)
		backupDocker(t, file, nil, "cp", "-a", "-", name+":/app")
		require.NoError(t, file.Close())
		var restored bytes.Buffer
		backupDocker(t, nil, &restored, "cp", name+":/app/"+f.root, "-")
		entries, err := inspectBackup(&restored, f.root)
		require.NoError(t, err)
		require.Equal(t, backups[name], entries, "restored bytes, modes and ownership must exactly match before startup")
	}
	f.composeCommand(t, "start")
	require.NoError(t, e2e.CheckBrokerHealth(f.health))
}
