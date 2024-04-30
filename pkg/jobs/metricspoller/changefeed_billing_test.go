package metricspoller_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/metricspoller"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestFetchChangefeedBillingBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params := base.TestServerArgs{}
	s := serverutils.StartServerOnly(t, params)
	defer s.Stopper().Stop(ctx)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	execCtx, close := sql.MakeJobExecContext(ctx, "test", username.NodeUserName(), &sql.MemoryMetrics{}, &execCfg)
	defer close()

	res, err := metricspoller.FetchChangefeedBillingBytes(ctx, execCtx)
	require.NoError(t, err)
	_ = res
}
