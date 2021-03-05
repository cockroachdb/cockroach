package repro_test

import (
	"context"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(m.Run())
}

func gc() {
	debug.SetGCPercent(1) // default is 100, so this makes it more aggressive
	for i := 0; i < 5; i++ {
		debug.FreeOSMemory()
		runtime.GC()
		runtime.GC()
		runtime.GC()
		debug.FreeOSMemory()
	}
}

func TestFoo(t *testing.T) {
	for i := 0; i < 100; i++ {
		work(t)
		gc()
	}
	for i := 0; i < 5; i++ {
		gc()
		dump(t)
		time.Sleep(time.Second)
	}
}

func work(t *testing.T) {
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		defer stop.PrintLeakedStoppers(t)
		defer leaktest.AfterTest(t)()

		args := base.TestClusterArgs{}
		args.ServerArgs.Insecure = true
		args.ReplicationMode = base.ReplicationManual
		ts := serverutils.NewTestCluster(t, 1, args)
		ts.Start(t)
		ts.Stopper().Stop(context.Background())
	}()
	<-ch

}

func dump(t *testing.T) {
	f, err := os.Create("heap.pprof")
	require.NoError(t, err)
	require.NoError(t, pprof.WriteHeapProfile(f))
	require.NoError(t, f.Close())
	time.Sleep(time.Second)
}
