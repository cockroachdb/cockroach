package tests

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/modular"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

// BackupTestConfig holds configuration for backup tests
type BackupTestConfig struct {
	NodeCount       int
	GatewayNode     int
	NodeToShutdown  int
	WatcherNode     int
	MaxConcurrency  int
	UseLargeDataset bool
}

// NewBackupTestConfig creates a default backup test configuration with the specified node count
func NewBackupTestConfig(gatewayNode, nodeCount int) *BackupTestConfig {
	return &BackupTestConfig{
		NodeCount:       nodeCount,
		GatewayNode:     gatewayNode,
		NodeToShutdown:  1 + (gatewayNode)%nodeCount,
		WatcherNode:     1 + ((gatewayNode + 1) % nodeCount),
		MaxConcurrency:  2,
		UseLargeDataset: true,
	}
}

// AddDataLoadingStage adds stages for loading bank data and waiting for replication
func AddDataLoadingStage(mft *modular.Test, config *BackupTestConfig, c cluster.Cluster, t test.Test) {
	mft.Setup("load backup data", func(ctx context.Context, logger *logger.Logger, helper *modular.Helper) error {
		rows := rows15GiB
		if c.IsLocal() {
			rows = rows5GiB
		}
		if !config.UseLargeDataset {
			rows = rows5GiB
		}
		_ = importBankData(ctx, rows, t, c)
		return nil
	})

	mft.Setup("wait for 3x replication", func(ctx context.Context, l *logger.Logger, helper *modular.Helper) error {
		db := c.Conn(ctx, t.L(), 1)
		defer db.Close()

		t.Status("waiting for cluster to be 3x replicated")
		return roachtestutil.WaitFor3XReplication(ctx, t.L(), db)
	})
}

// CreateBackupStage creates a stage that starts and monitors a backup job
func CreateBackupStage(mft *modular.Test, config *BackupTestConfig, c cluster.Cluster, t test.Test, jobID *jobspb.JobID) *modular.Stage {
	backupStage := mft.NewStage("backup")
	backupStage.Execute("start backup", func(ctx context.Context, l *logger.Logger, helper *modular.Helper) error {
		startBackup := func(c cluster.Cluster, l *logger.Logger) (jobspb.JobID, error) {
			gatewayDB := c.Conn(ctx, l, config.GatewayNode)
			defer gatewayDB.Close()

			dest := destinationName(c)
			backupQuery := `BACKUP bank.bank INTO 'nodelocal://1/` + dest + `' WITH DETACHED`
			var id jobspb.JobID
			err := gatewayDB.QueryRowContext(ctx, backupQuery).Scan(&id)
			return id, err
		}

		t.Status("running job")
		id, err := startBackup(c, t.L())
		if err != nil {
			return err
		}
		*jobID = id

		watcherDB := c.Conn(ctx, t.L(), config.WatcherNode)
		defer watcherDB.Close()
		return WaitForRunning(ctx, watcherDB, id, time.Minute)
	}).Then("wait for backup", func(ctx context.Context, l *logger.Logger, helper *modular.Helper) error {
		return waitForBackupCompletion(ctx, config, c, t, *jobID)
	})
	return backupStage
}

// waitForBackupCompletion waits for a backup job to complete successfully
func waitForBackupCompletion(ctx context.Context, config *BackupTestConfig, c cluster.Cluster, t test.Test, jobID jobspb.JobID) error {
	watcherDB := c.Conn(ctx, t.L(), config.WatcherNode)
	defer watcherDB.Close()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	var status string
	for {
		select {
		case <-ticker.C:
			err := watcherDB.QueryRowContext(ctx, `SELECT status FROM [SHOW JOB $1]`, jobID).Scan(&status)
			if err != nil {
				return errors.Wrap(err, "getting the job status")
			}
			jobStatus := jobs.State(status)
			switch jobStatus {
			case jobs.StateSucceeded:
				t.Status("job completed")
				return nil
			case jobs.StateRunning:
				t.L().Printf("job %d still running, waiting to succeed", jobID)
			default:
				return errors.Newf("unexpectedly found job %d in state %s", jobID, status)
			}
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "context canceled while waiting for job to finish")
		}
	}
}

// shutdownNode handles the logic for shutting down a node (gracefully or with SIGKILL)
func shutdownNode(ctx context.Context, config *BackupTestConfig, c cluster.Cluster, t test.Test) error {
	sleepBeforeShutdown := 30 * time.Second
	if config.GatewayNode == config.NodeToShutdown {
		sleepBeforeShutdown = 2 * time.Minute
	}
	time.Sleep(sleepBeforeShutdown)

	rng, _ := randutil.NewTestRand()
	shouldUseSigKill := rng.Float64() > 0.5

	if shouldUseSigKill {
		t.L().Printf(`stopping node (using SIGKILL) %d`, config.NodeToShutdown)
		if err := c.StopE(ctx, t.L(), option.DefaultStopOpts(), c.Node(config.NodeToShutdown)); err != nil {
			return errors.Wrapf(err, "could not stop node %d", config.NodeToShutdown)
		}
	} else {
		t.L().Printf(`stopping node gracefully %d`, config.NodeToShutdown)
		if err := c.StopE(
			ctx, t.L(), option.NewStopOpts(option.Graceful(shutdownGracePeriod)), c.Node(config.NodeToShutdown),
		); err != nil {
			return errors.Wrapf(err, "could not stop node %d", config.NodeToShutdown)
		}
	}
	t.L().Printf("stopped node %d", config.NodeToShutdown)
	return nil
}

// RunModularBackupTest runs a complete modular backup test with the given configuration
func RunModularBackupTest(ctx context.Context, t test.Test, c cluster.Cluster, config *BackupTestConfig) error {
	mft := modular.NewTest(ctx, t.L(), c, c.CRDBNodes())
	mft.SetMaxConcurrency(config.MaxConcurrency)

	// Add data loading stages
	AddDataLoadingStage(mft, config, c, t)

	// Create backup and node shutdown stages
	var jobID jobspb.JobID
	backupStage := CreateBackupStage(mft, config, c, t, &jobID)

	// Add shutdown sequence to backup stage
	backupStage.Execute("shutting down node", func(ctx context.Context, l *logger.Logger, helper *modular.Helper) error {
		return shutdownNode(ctx, config, c, t)
	})

	mft.AfterTest("validate backup", func(ctx context.Context, l *logger.Logger, helper *modular.Helper) error {
		// TODO: implement backup validation logic
		return nil
	})

	t.L().Printf(mft.Plan().String())
	return nil
}

func registerBackupNodeShutdownModular(r registry.Registry) {
	clusterSpec := r.MakeClusterSpec(4)
	r.Add(registry.TestSpec{
		Name:                      fmt.Sprintf("backup/nodeShutdown/modular/%s", clusterSpec),
		Owner:                     registry.OwnerDisasterRecovery,
		Cluster:                   clusterSpec,
		EncryptionSupport:         registry.EncryptionMetamorphic,
		Leases:                    registry.MetamorphicLeases,
		CompatibleClouds:          registry.AllExceptAWS,
		Suites:                    registry.Suites(registry.Nightly),
		TestSelectionOptOutSuites: registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			gatewayNode := rand.Intn(c.Spec().NodeCount)
			config := NewBackupTestConfig(gatewayNode, c.Spec().NodeCount)
			if err := RunModularBackupTest(ctx, t, c, config); err != nil {
				t.Fatal(err)
			}
		},
	})
}
