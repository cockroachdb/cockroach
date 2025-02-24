// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// SystemInterfaceSystemdUnitName is a convenience function that
// returns the systemd unit name for the system interface
func SystemInterfaceSystemdUnitName() string {
	return install.VirtualClusterLabel(install.SystemInterfaceName, 0)
}

// SetDefaultSQLPort sets the SQL port to the default of 26257 if it is
// a non-local cluster. Local clusters don't support changing the port.
func SetDefaultSQLPort(c cluster.Cluster, opts *install.StartOpts) {
	if !c.IsLocal() {
		opts.SQLPort = config.DefaultSQLPort
	}
}

// SetDefaultAdminUIPort sets the AdminUI port to the default of 26258 if it is
// a non-local cluster. Local clusters don't support changing the port.
func SetDefaultAdminUIPort(c cluster.Cluster, opts *install.StartOpts) {
	if !c.IsLocal() {
		opts.AdminUIPort = config.DefaultAdminUIPort
	}
}

// EveryN provides a way to rate limit noisy log messages. It tracks how
// recently a given log message has been emitted so that it can determine
// whether it's worth logging again.
type EveryN struct {
	util.EveryN
}

// Every is a convenience constructor for an EveryN object that allows a log
// message every n duration.
func Every(n time.Duration) EveryN {
	return EveryN{EveryN: util.Every(n)}
}

// ShouldLog returns whether it's been more than N time since the last event.
func (e *EveryN) ShouldLog() bool {
	return e.ShouldProcess(timeutil.Now())
}

// WaitForSQLReady waits until the corresponding node's SQL subsystem is fully initialized and ready
// to serve SQL clients.
// N.B. The fact that we have a live db connection doesn't imply that the SQL subsystem is ready to serve. E.g.,
// a SQL session cannot be authenticated until after `SyntheticPrivilegeCache` is initialized, which is done
// asynchronously at server startup.
// (See "Root Cause" in https://github.com/cockroachdb/cockroach/issues/137988)
func WaitForSQLReady(ctx context.Context, db *gosql.DB) error {
	retryOpts := retry.Options{MaxRetries: 5}
	return retryOpts.Do(ctx, func(ctx context.Context) error {
		_, err := db.ExecContext(ctx, "SELECT 1")
		return err
	})
}

// WaitForReady waits until the given nodes report ready via health checks.
// This implies that the node has completed server startup, is heartbeating its
// liveness record, and can serve SQL clients.
// FIXME(srosenberg): This function is a bit of a misnomer. It doesn't actually ensure that SQL is ready to serve, only
// that the admin UI is ready to serve. We should consolidate this with WaitForSQLReady.
func WaitForReady(
	ctx context.Context, t test.Test, c cluster.Cluster, nodes option.NodeListOption,
) {
	client := DefaultHTTPClient(c, t.L())
	checkReady := func(ctx context.Context, url string) error {
		resp, err := client.client.Get(ctx, url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return errors.Errorf("HTTP %d: %s", resp.StatusCode, body)
		}
		return nil
	}

	adminAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), nodes)
	require.NoError(t, err)

	require.NoError(t, timeutil.RunWithTimeout(
		ctx, "waiting for ready", time.Minute, func(ctx context.Context) error {
			for i, adminAddr := range adminAddrs {
				url := fmt.Sprintf(`https://%s/health?ready=1`, adminAddr)

				for err := checkReady(ctx, url); err != nil && ctx.Err() == nil; err = checkReady(ctx, url) {
					t.L().Printf("n%d not ready, retrying: %s", nodes[i], err)
					time.Sleep(time.Second)
				}
				t.L().Printf("n%d is ready", nodes[i])
			}
			return ctx.Err()
		},
	))
}

// SetAdmissionControl sets the admission control cluster settings on the
// given cluster.
func SetAdmissionControl(ctx context.Context, t test.Test, c cluster.Cluster, enabled bool) {
	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()
	val := "true"
	if !enabled {
		val = "false"
	}
	for _, setting := range []string{
		"admission.kv.enabled",
		"admission.sql_kv_response.enabled",
		"admission.sql_sql_response.enabled",
		"admission.elastic_cpu.enabled",
	} {
		if _, err := db.ExecContext(
			ctx, "SET CLUSTER SETTING "+setting+" = '"+val+"'"); err != nil {
			t.Fatalf("failed to set admission control to %t: %v", enabled, err)
		}
	}
	if !enabled {
		if _, err := db.ExecContext(
			ctx, "SET CLUSTER SETTING admission.kv.pause_replication_io_threshold = 0.0"); err != nil {
			t.Fatalf("failed to set admission control to %t: %v", enabled, err)
		}
	}
}

// UsingRuntimeAssertions returns true if calls to `t.Cockroach()` for
// this test will return the cockroach build with runtime
// assertions.
func UsingRuntimeAssertions(t test.Test) bool {
	return t.Cockroach() == t.RuntimeAssertionsCockroach()
}

// MaybeUseMemoryBudget returns a StartOpts with the specified --max-sql-memory
// if runtime assertions are enabled, and the default values otherwise.
// A scheduled backup will not begin at the start of the roachtest.
func MaybeUseMemoryBudget(t test.Test, budget int) option.StartOpts {
	startOpts := option.NewStartOpts(option.NoBackupSchedule)
	if UsingRuntimeAssertions(t) {
		// When running tests with runtime assertions enabled, increase
		// SQL's memory budget to avoid 'budget exceeded' failures.
		startOpts.RoachprodOpts.ExtraArgs = append(
			startOpts.RoachprodOpts.ExtraArgs,
			fmt.Sprintf("--max-sql-memory=%d%%", budget),
		)
	}
	return startOpts
}

// Returns the mean over the last n samples. If n > len(items), returns the mean
// over the entire items slice.
func GetMeanOverLastN(n int, items []float64) float64 {
	count := n
	if len(items) < n {
		count = len(items)
	}
	sum := float64(0)
	i := 0
	for i < count {
		sum += items[len(items)-1-i]
		i++
	}
	return sum / float64(count)
}

// EnvWorkloadDurationFlag - environment variable to override
// default run time duration of workload set in tests.
// Usage: ROACHTEST_PERF_WORKLOAD_DURATION="5m".
const EnvWorkloadDurationFlag = "ROACHTEST_PERF_WORKLOAD_DURATION"

var workloadDurationRegex = regexp.MustCompile(`^\d+[mhsMHS]$`)

// GetEnvWorkloadDurationValueOrDefault validates EnvWorkloadDurationFlag and
// returns value set if valid else returns default duration.
func GetEnvWorkloadDurationValueOrDefault(defaultDuration string) string {
	envWorkloadDurationFlag := os.Getenv(EnvWorkloadDurationFlag)
	if envWorkloadDurationFlag != "" && workloadDurationRegex.MatchString(envWorkloadDurationFlag) {
		return " --duration=" + envWorkloadDurationFlag
	}
	return " --duration=" + defaultDuration
}

func IfLocal(c cluster.Cluster, trueVal, falseVal string) string {
	if c.IsLocal() {
		return trueVal
	}
	return falseVal
}

// CheckPortBlocked returns true if a connection from a node to a port on another node
// can be established. Requires nmap to be installed.
func CheckPortBlocked(
	ctx context.Context,
	l *logger.Logger,
	c cluster.Cluster,
	fromNode, toNode option.NodeListOption,
	port string,
) (bool, error) {
	// `nmap -oG` example output:
	// Host: {IP} {HOST_NAME}	Status: Up
	// Host: {IP} {HOST_NAME}	Ports: 26257/open/tcp//cockroach///
	// We care about the port scan result and whether it is filtered or open.
	res, err := c.RunWithDetailsSingleNode(ctx, l, option.WithNodes(fromNode), fmt.Sprintf("nmap -p %s {ip%s} -oG - | awk '/Ports:/{print $5}'", port, toNode))
	if err != nil {
		return false, err
	}
	return strings.Contains(res.Stdout, "filtered"), nil
}
