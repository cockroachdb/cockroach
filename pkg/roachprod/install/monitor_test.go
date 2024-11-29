package install

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/stretchr/testify/require"
)

func TestMonitor(t *testing.T) {
	ctx := context.Background()

	s := newMockSession()
	s.OnCombinedOutput = func(ctx context.Context) ([]byte, error) {
		return []byte("cockroach-tenant_1\n"), nil
	}
	_, err := s.StdoutBuffer.Write([]byte("tenant|1|dead|0\n"))
	require.NoError(t, err)

	c := &SyncedCluster{
		Cluster: cloud.Cluster{
			Name: "testCluster",
			VMs: vm.List{
				vm.VM{},
			},
		},
		Nodes: []Node{1},
		sessionProvider: func() session {
			return s
		},
	}

	monitorChan := c.Monitor(nilLogger(), ctx, MonitorOpts{})
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		e := <-monitorChan
		require.IsType(t, MonitorProcessDead{}, e.Event)
		s.Close()
	}()
	wg.Wait()
}
