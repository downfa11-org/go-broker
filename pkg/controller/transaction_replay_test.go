package controller

import (
	"testing"

	clusterController "github.com/cursus-io/cursus/pkg/cluster/controller"
	"github.com/cursus-io/cursus/pkg/cluster/replication/fsm"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/transaction"
	"github.com/stretchr/testify/require"
)

func TestCommandHandlerPreservesReplayedTransactionManager(t *testing.T) {
	manager := transaction.NewManager()
	producer, epoch, err := manager.InitProducer("recovered")
	require.NoError(t, err)
	require.NoError(t, manager.Begin("recovered", producer, epoch))
	state := fsm.NewBrokerFSM(nil, nil)
	state.SetTransactionManager(manager)
	cluster := &clusterController.ClusterController{RaftManager: &MockRaftManagerForForward{state: state}}
	handler := NewCommandHandler(nil, config.DefaultConfig(), nil, nil, cluster)
	t.Cleanup(func() { require.NoError(t, handler.Close()) })
	require.Same(t, manager, handler.TxnManager)
	require.Same(t, manager, state.TransactionManager())
	restored, err := handler.TxnManager.Status("recovered")
	require.NoError(t, err)
	require.Equal(t, transaction.StateOpen, restored.State)
}
