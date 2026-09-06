package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/disk"
	"github.com/cursus-io/cursus/pkg/server"
	"github.com/cursus-io/cursus/pkg/stream"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/util"
)

var runServerContext = server.RunServerContext
var runTopicMetadataDiagnostics = server.RunTopicMetadataDiagnostics
var runConsumerMetadataDiagnostics = server.RunConsumerMetadataDiagnostics

func main() {
	// Configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		util.Fatal("❌ Failed to load config: %v", err)
	}

	data, err := config.MarshalRedactedJSON(cfg)
	if err != nil {
		util.Error("Failed to marshal config: %v", err)
	} else {
		util.Info("Configuration:\n%s", string(data))
	}

	fmt.Print(`
                         _______  ______________  _______
                        / ___/ / / / ___/ ___/ / / / ___/
                       / /__/ /_/ / /  (__  ) /_/ (__  )
                       \___/\__,_/_/  /____/\__,_/____/

                                            version.0.1.0
`)

	util.Info("🚀 Starting broker on port %d\n", cfg.BrokerPort)
	util.Info("📊 Exporter: %v\n", cfg.EnableExporter)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	superviseShutdown(ctx, time.Duration(cfg.ShutdownTimeoutMS)*time.Millisecond, os.Exit, func() {
		if err := runBroker(ctx, cfg); err != nil && !onlyCancellation(err) {
			util.Fatal("❌ Broker failed: %v", err)
		}
	})
}

func runBroker(ctx context.Context, cfg *config.Config) (runErr error) {
	dm := disk.NewDiskManager(cfg)
	defer func() { runErr = errors.Join(runErr, dm.Close()) }()
	sm := stream.NewStreamManager(cfg.MaxStreamConnections, cfg.StreamTimeout)
	smAdapter, err := topic.NewStreamManagerAdapter(sm)
	if err != nil {
		return fmt.Errorf("create stream manager adapter: %w", err)
	}

	storageProvider, err := newStorageProvider(dm, cfg.LogDir)
	if err != nil {
		return fmt.Errorf("configure storage provider: %w", err)
	}

	tm := topic.NewTopicManager(cfg, storageProvider, smAdapter)
	defer tm.Stop()
	if err := tm.RestoreTopics(); err != nil {
		util.Error("Failed to restore durable topic metadata; serving diagnostics only: %v", err)
		return runTopicMetadataDiagnostics(ctx, cfg, tm, dm)
	}

	cd, err := coordinator.NewCoordinatorWithRecovery(ctx, cfg, tm)
	if cd != nil {
		defer cd.Stop()
	}
	if err != nil {
		util.Error("Failed to recover durable consumer metadata; serving diagnostics only: %v", err)
		return runConsumerMetadataDiagnostics(ctx, cfg, tm, dm, cd)
	}
	tm.SetCoordinator(cd)
	for _, gcfg := range cfg.StaticConsumerGroups {
		for _, topicName := range gcfg.Topics {
			current := tm.GetTopic(topicName)
			if current == nil {
				util.Error("⚠️ Topic %q does not exist; skipping static consumer group registration", topicName)
				continue
			}
			if _, err := tm.RegisterConsumerGroup(topicName, gcfg.Name, gcfg.ConsumerCount); err != nil {
				util.Error("⚠️ Failed to register static consumer group %q on topic %q: %v", gcfg.Name, topicName, err)
			}
		}
	}

	return runServerContext(ctx, cfg, tm, dm, cd, sm)
}
