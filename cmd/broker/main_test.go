package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
)

func TestRunBrokerUsesDiagnosticsWhenTopicManifestRestoreFails(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = false
	if err := os.WriteFile(filepath.Join(cfg.LogDir, topic.TopicMetadataFileName), []byte(`{"version":`), 0o600); err != nil {
		t.Fatal(err)
	}

	originalDiagnostics := runTopicMetadataDiagnostics
	originalServer := runServerContext
	t.Cleanup(func() {
		runTopicMetadataDiagnostics = originalDiagnostics
		runServerContext = originalServer
	})
	diagnosticsCalled := false
	serverCalled := false
	runTopicMetadataDiagnostics = func(_ context.Context, _ *config.Config, tm *topic.TopicManager, _ *disk.DiskManager) error {
		diagnosticsCalled = true
		if err := tm.MetadataReadinessError(); err == nil {
			t.Error("diagnostics received topic manager without readiness error")
		}
		return nil
	}
	runServerContext = func(context.Context, *config.Config, *topic.TopicManager, *disk.DiskManager, *coordinator.Coordinator, *stream.StreamManager) error {
		serverCalled = true
		return nil
	}

	if err := runBroker(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if !diagnosticsCalled || serverCalled {
		t.Fatalf("diagnosticsCalled=%t serverCalled=%t", diagnosticsCalled, serverCalled)
	}
}

func TestRunBrokerUsesDiagnosticsWhenConsumerMetadataRecoveryFails(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = false

	seedDM := disk.NewDiskManager(cfg)
	seedTM := topic.NewTopicManager(cfg, seedDM, nil)
	seedCoordinator, err := coordinator.NewCoordinatorWithRecovery(context.Background(), cfg, seedTM)
	if err != nil {
		t.Fatal(err)
	}
	if err := seedTM.PublishWithAck(config.ConsumerOffsetsTopicName, &types.Message{Key: "corrupt", Payload: "not-json"}); err != nil {
		t.Fatal(err)
	}
	seedCoordinator.Stop()
	seedTM.Stop()
	seedDM.CloseAllHandlers()

	originalTopicDiagnostics := runTopicMetadataDiagnostics
	originalConsumerDiagnostics := runConsumerMetadataDiagnostics
	originalServer := runServerContext
	t.Cleanup(func() {
		runTopicMetadataDiagnostics = originalTopicDiagnostics
		runConsumerMetadataDiagnostics = originalConsumerDiagnostics
		runServerContext = originalServer
	})
	topicDiagnosticsCalled := false
	consumerDiagnosticsCalled := false
	serverCalled := false
	runTopicMetadataDiagnostics = func(context.Context, *config.Config, *topic.TopicManager, *disk.DiskManager) error {
		topicDiagnosticsCalled = true
		return nil
	}
	runConsumerMetadataDiagnostics = func(_ context.Context, _ *config.Config, _ *topic.TopicManager, dm *disk.DiskManager, cd *coordinator.Coordinator) error {
		consumerDiagnosticsCalled = true
		if cd == nil || cd.RecoveryReadinessError() == nil || cd.RecoverySnapshot().CorruptRecords != 1 {
			t.Fatalf("consumer diagnostics received healthy or missing recovery state: %#v", cd)
		}
		dm.CloseAllHandlers()
		return nil
	}
	runServerContext = func(context.Context, *config.Config, *topic.TopicManager, *disk.DiskManager, *coordinator.Coordinator, *stream.StreamManager) error {
		serverCalled = true
		return nil
	}

	if err := runBroker(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if topicDiagnosticsCalled || !consumerDiagnosticsCalled || serverCalled {
		t.Fatalf("topicDiagnostics=%t consumerDiagnostics=%t server=%t", topicDiagnosticsCalled, consumerDiagnosticsCalled, serverCalled)
	}
}
func TestRunBrokerStartsMainServerAfterSuccessfulTopicRestore(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = false

	originalDiagnostics := runTopicMetadataDiagnostics
	originalServer := runServerContext
	t.Cleanup(func() {
		runTopicMetadataDiagnostics = originalDiagnostics
		runServerContext = originalServer
	})
	diagnosticsCalled := false
	serverCalled := false
	runTopicMetadataDiagnostics = func(context.Context, *config.Config, *topic.TopicManager, *disk.DiskManager) error {
		diagnosticsCalled = true
		return nil
	}
	runServerContext = func(_ context.Context, _ *config.Config, _ *topic.TopicManager, dm *disk.DiskManager, _ *coordinator.Coordinator, _ *stream.StreamManager) error {
		serverCalled = true
		dm.CloseAllHandlers()
		return nil
	}

	if err := runBroker(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if diagnosticsCalled || !serverCalled {
		t.Fatalf("diagnosticsCalled=%t serverCalled=%t", diagnosticsCalled, serverCalled)
	}
}

func TestRunBrokerPreservesStorageCloseFailureWithCancellation(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LogDir = t.TempDir()
	cfg.EnabledDistribution = false
	originalServer := runServerContext
	t.Cleanup(func() { runServerContext = originalServer })
	runServerContext = func(_ context.Context, _ *config.Config, _ *topic.TopicManager, dm *disk.DiskManager, _ *coordinator.Coordinator, _ *stream.StreamManager) error {
		handler, err := dm.GetHandler("close-failure", 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := handler.(*disk.DiskHandler).GetIndexFile().Close(); err != nil {
			t.Fatal(err)
		}
		return context.Canceled
	}
	err := runBroker(context.Background(), cfg)
	if !errors.Is(err, context.Canceled) || onlyCancellation(err) {
		t.Fatalf("expected cancellation joined with storage close error, got %v", err)
	}
}
