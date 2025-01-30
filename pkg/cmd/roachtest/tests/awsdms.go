// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	dms "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	dmstypes "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

const (
	awsdmsWaitTimeLimit  = 1 * time.Hour
	awsdmsUser           = "cockroachdbtest"
	awsdmsDatabase       = "rdsdb"
	awsdmsCRDBDatabase   = "defaultdb"
	awsdmsCRDBUser       = "dms"
	awsdmsNumInitialRows = 100000
)

const (
	// This RDS instance for this test is always up in our AWS account. It
	// contains 200M rows so having to generate and import that data each run
	// would take too long. The secret name stored in aws can only be accessed
	// by CRL employees who have proper AWS credentials. The password can't be
	// auto rotated due to constraints with postgres DMS source endpoint restrictions
	// where the password can't contain %, ;, or +.
	awsrdsSecretName     = "rds!db-bf11a38a-4ba2-425d-b2b2-acab58357f2e"
	awsrdsDBIdentifier   = "migrations-dms"
	awsrdsNumInitialRows = 200000000
)

type dmsTask struct {
	tableName           string
	tableMappings       string
	replicationSettings *string
	migrationType       dmstypes.MigrationTypeValue
	sourceEndpoint      *dmsEndpointState
	targetEndpoint      *dmsEndpointState
}

type dmsTaskEndpoints struct {
	defaultSource *dmsEndpointState
	defaultTarget *dmsEndpointState
	largeSource   *dmsEndpointState
}

type dmsEndpointState struct {
	id  string
	arn string
}

const tableRules = `{
	"rules": [
		{
			"rule-type": "selection",
			"rule-id": "1",
			"rule-name": "1",
			"object-locator": {
				"schema-name": "%%",
				"table-name": "%s"
			},
			"rule-action": "include"
		}
	]
}`

const fullLoadTruncate = `{
	"FullLoadSettings":{
	  "TargetTablePrepMode":"TRUNCATE_BEFORE_LOAD"
	}
  }`

func awsdmsVerString(v *version.Version) string {
	if ciBuildID := os.Getenv("TC_BUILD_ID"); ciBuildID != "" {
		ciBuildID = strings.ReplaceAll(ciBuildID, ".", "-")
		return fmt.Sprintf("ci-build-%s", ciBuildID)
	}
	ret := fmt.Sprintf("local-%d-%d-%d", v.Major(), v.Minor(), v.Patch())
	if v.PreRelease() != "" {
		ret += "-" + v.PreRelease()
	}
	ret = strings.ReplaceAll(ret, ".", "-")
	const maxSize = 24
	if len(ret) > maxSize {
		ret = ret[:maxSize]
	}
	return ret
}

func awsdmsRoachtestRDSClusterName(v *version.Version) string {
	return "roachtest-awsdms-rds-cluster-" + awsdmsVerString(v)
}

func awsdmsRoachtestDMSParameterGroup(v *version.Version) string {
	return "roachtest-awsdms-param-group-" + awsdmsVerString(v)
}

func awsdmsRoachtestDMSTaskName(v *version.Version, tableName string) string {
	return fmt.Sprintf("roachtest-awsdms-dms-task-%s-%s", strings.ReplaceAll(tableName, "_", "-"), awsdmsVerString(v))
}

func awsdmsRoachtestDMSReplicationInstanceName(v *version.Version) string {
	return "roachtest-awsdms-replication-instance-" + awsdmsVerString(v)
}

// The largeTask flag is used to create a new source endpoint as it is connecting to a different RDS
// and needs to have a different name from the other tests.
func awsdmsRoachtestDMSRDSEndpointName(v *version.Version, largeTask bool) string {
	if largeTask {
		return "roachtest-awsdms-large-rds-endpoint-" + awsdmsVerString(v)
	}
	return "roachtest-awsdms-rds-endpoint-" + awsdmsVerString(v)
}
func awsdmsRoachtestDMSCRDBEndpointName(v *version.Version) string {
	return "roachtest-awsdms-crdb-endpoint-" + awsdmsVerString(v)
}

func rdsClusterFilters(v *version.Version) []rdstypes.Filter {
	return []rdstypes.Filter{
		{
			Name:   proto.String("db-cluster-id"),
			Values: []string{awsdmsRoachtestRDSClusterName(v)},
		},
	}
}

func rdsDescribeInstancesInput(v *version.Version) *rds.DescribeDBInstancesInput {
	return &rds.DescribeDBInstancesInput{
		Filters: rdsClusterFilters(v),
	}
}

func dmsDescribeInstancesInput(v *version.Version) *dms.DescribeReplicationInstancesInput {
	return &dms.DescribeReplicationInstancesInput{
		Filters: []dmstypes.Filter{
			{
				Name:   proto.String("replication-instance-id"),
				Values: []string{awsdmsRoachtestDMSReplicationInstanceName(v)},
			},
		},
	}
}

func dmsDescribeTasksInput(
	v *version.Version, tableName string,
) *dms.DescribeReplicationTasksInput {
	return &dms.DescribeReplicationTasksInput{
		Filters: []dmstypes.Filter{
			{
				Name:   proto.String("replication-task-id"),
				Values: []string{awsdmsRoachtestDMSTaskName(v, tableName)},
			},
		},
	}
}

func registerAWSDMS(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "awsdms",
		Owner:            registry.OwnerMigrations,
		Cluster:          r.MakeClusterSpec(1),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Weekly),
		Run:              runAWSDMS,
	})
}

// runAWSDMS creates Amazon RDS instances to import into CRDB using AWS DMS.
//
// The RDS and DMS instances are always created with the same names, so that
// we can always start afresh with a new instance and that we can assume
// there is only ever one of these at any time. On startup and teardown,
// we will attempt to delete previously created instances.
func runAWSDMS(ctx context.Context, t test.Test, c cluster.Cluster) {
	if c.IsLocal() {
		t.Fatal("cannot be run in local mode")
	}
	// We may not have the requisite certificates to start DMS/RDS on non-AWS invocations.
	if cloud := c.Cloud(); cloud != spec.AWS {
		t.Skipf("skipping test on cloud %s", cloud)
		return
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithDefaultRegion("us-east-1"))
	if err != nil {
		t.Fatal(err)
	}
	rdsCli := rds.NewFromConfig(awsCfg)
	dmsCli := dms.NewFromConfig(awsCfg)
	smCli := secretsmanager.NewFromConfig(awsCfg)

	// Create the endpoints that we will use but the ARN's are nil
	// and will be filled in when the endpoints are recreated.
	// Deletion of old endpoints can be done using id.
	dmsEndpoints := dmsTaskEndpoints{
		defaultSource: &dmsEndpointState{
			id: awsdmsRoachtestDMSRDSEndpointName(t.BuildVersion(), false),
		},
		defaultTarget: &dmsEndpointState{
			id: awsdmsRoachtestDMSCRDBEndpointName(t.BuildVersion()),
		},
		largeSource: &dmsEndpointState{
			id: awsdmsRoachtestDMSRDSEndpointName(t.BuildVersion(), true),
		},
	}
	dmsTasks := []dmsTask{
		{
			tableName:           "test_table",
			tableMappings:       tableRules,
			replicationSettings: nil,
			migrationType:       dmstypes.MigrationTypeValueFullLoadAndCdc,
			sourceEndpoint:      dmsEndpoints.defaultSource,
			targetEndpoint:      dmsEndpoints.defaultTarget,
		},
		{
			tableName:           "test_table_no_pk",
			tableMappings:       tableRules,
			replicationSettings: proto.String(fullLoadTruncate),
			migrationType:       dmstypes.MigrationTypeValueFullLoadAndCdc,
			sourceEndpoint:      dmsEndpoints.defaultSource,
			targetEndpoint:      dmsEndpoints.defaultTarget,
		},
		{
			tableName:           "test_table_large",
			tableMappings:       tableRules,
			replicationSettings: proto.String(fullLoadTruncate),
			migrationType:       dmstypes.MigrationTypeValueFullLoad,
			sourceEndpoint:      dmsEndpoints.largeSource,
			targetEndpoint:      dmsEndpoints.defaultTarget,
		},
	}

	// Attempt a clean-up of old instances on startup.
	t.L().Printf("attempting to delete old instances")
	if err := tearDownAWSDMS(ctx, t, rdsCli, dmsCli, &dmsTasks); err != nil {
		t.Fatal(err)
	}
	//Attempt a clean-up of old instances on shutdown.
	defer func() {
		if t.IsDebug() {
			t.L().Printf("not deleting old instances as --debug is set")
			return
		}
		t.L().Printf("attempting to cleanup instances")
		// Try to delete from a new context, in case the previous one is cancelled.
		if err := tearDownAWSDMS(context.Background(), t, rdsCli, dmsCli, &dmsTasks); err != nil {
			t.L().Printf("failed to delete old instances on cleanup: %+v", err)
		}
	}()

	sourcePGConn, err := setupAWSDMS(ctx, t, c, rdsCli, dmsCli, smCli, &dmsTasks, &dmsEndpoints)
	if err != nil {
		t.Fatal(err)
	}
	targetPGConn := c.Conn(ctx, t.L(), 1)

	checkDMSReplicated(ctx, t, sourcePGConn, targetPGConn)
	checkDMSNoPKTableError(ctx, t, dmsCli)
	checkFullLargeDataLoad(ctx, t, dmsCli)
	t.L().Printf("testing complete")
}

func checkDMSReplicated(
	ctx context.Context, t test.Test, sourcePGConn *pgx.Conn, targetPGConn *gosql.DB,
) {
	waitForReplicationRetryOpts := retry.Options{
		MaxBackoff: time.Second,
		MaxRetries: 90,
	}

	// Unfortunately validation isn't available in the SDK. For now, just assert
	// both tables have the same number of rows.
	t.L().Printf("testing all data gets replicated")
	if err := func() error {
		for r := retry.StartWithCtx(ctx, waitForReplicationRetryOpts); r.Next(); {
			err := func() error {
				var numRows int
				if err := targetPGConn.QueryRow("SELECT count(1) FROM test_table").Scan(&numRows); err != nil {
					return err
				}
				if numRows == awsdmsNumInitialRows {
					return nil
				}
				return errors.Newf("found %d rows when expecting %d", numRows, awsdmsNumInitialRows)
			}()
			if err == nil {
				return nil
			}
			t.L().Printf("initial replication not up to date, retrying: %+v", err)
		}
		return errors.Newf("failed to find target in sync")
	}(); err != nil {
		t.Fatal(err)
	}

	// Now check an INSERT, UPDATE and DELETE all gets replicated.
	const (
		numExtraRows   = 10
		numDeletedRows = 1
		deleteRowID    = 55
		updateRowID    = 742
		updateRowText  = "from now on the baby sleeps in the crib"
	)

	for _, stmt := range []string{
		fmt.Sprintf(
			`INSERT INTO test_table(id, t) SELECT i, md5(random()::text) FROM generate_series(%d, %d) AS t(i)`,
			awsdmsNumInitialRows+1,
			awsdmsNumInitialRows+numExtraRows,
		),
		fmt.Sprintf(`UPDATE test_table SET t = '%s' WHERE id = %d`, updateRowText, updateRowID),
		fmt.Sprintf(`DELETE FROM test_table WHERE id = %d`, deleteRowID),
	} {
		if _, err := sourcePGConn.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}

	t.L().Printf("testing all subsequent updates get replicated")
	if err := func() error {
		for r := retry.StartWithCtx(ctx, waitForReplicationRetryOpts); r.Next(); {
			err := func() error {
				var countOfDeletedRow int
				if err := targetPGConn.QueryRow("SELECT count(1) FROM test_table WHERE id = $1", deleteRowID).Scan(&countOfDeletedRow); err != nil {
					return err
				}
				if countOfDeletedRow != 0 {
					return errors.Newf("expected row to be deleted, still found")
				}

				var seenText string
				if err := targetPGConn.QueryRow("SELECT t FROM test_table WHERE id = $1", updateRowID).Scan(&seenText); err != nil {
					return err
				}
				if seenText != updateRowText {
					return errors.Newf("expected row to be updated, still found %s", seenText)
				}

				var numRows int
				if err := targetPGConn.QueryRow("SELECT count(1) FROM test_table").Scan(&numRows); err != nil {
					return err
				}
				expectedRows := awsdmsNumInitialRows + numExtraRows - numDeletedRows
				if numRows != expectedRows {
					return errors.Newf("found %d rows when expecting %d, retrying", numRows, expectedRows)
				}
				return nil
			}()
			if err == nil {
				return nil
			}
			t.L().Printf("replication not up to date, retrying: %+v", err)
		}
		return errors.Newf("failed to find target in sync")
	}(); err != nil {
		t.Fatal(err)
	}
}

func checkDMSNoPKTableError(ctx context.Context, t test.Test, dmsCli *dms.Client) {
	waitForTableError := retry.Options{
		MaxBackoff: time.Second,
		MaxRetries: 90,
	}
	t.L().Printf("testing no pk table has a table error")
	if err := func() error {
		for r := retry.StartWithCtx(ctx, waitForTableError); r.Next(); {
			err := func() error {
				dmsTasks, err := dmsCli.DescribeReplicationTasks(ctx, dmsDescribeTasksInput(t.BuildVersion(), "test_table_no_pk"))
				if err != nil {
					if !isDMSResourceNotFound(err) {
						return err
					}
				}
				for _, task := range dmsTasks.ReplicationTasks {
					if task.ReplicationTaskStats != nil {
						if task.ReplicationTaskStats.TablesErrored == 1 {
							t.L().Printf("table error was found")
							return nil
						}
					}
				}
				return errors.New("no table error found yet")
			}()
			if err == nil {
				return nil
			}
			t.L().Printf("table error not found, retrying: %+v", err)
		}
		return errors.Newf("failed to find table error")
	}(); err != nil {
		t.Fatal(err)
	}
}

func checkFullLargeDataLoad(ctx context.Context, t test.Test, dmsCli *dms.Client) {
	closer := make(chan struct{})
	waitForFullLoad := retry.Options{
		InitialBackoff: 10 * time.Second,
		MaxBackoff:     5 * time.Minute,
		Closer:         closer,
	}
	t.L().Printf("testing all rows from test_table_large replicate")
	var numRows, nonUpdate = 0, 0
	for r := retry.StartWithCtx(ctx, waitForFullLoad); r.Next(); {
		err := func() error {
			dmsTasks, err := dmsCli.DescribeReplicationTasks(ctx, dmsDescribeTasksInput(t.BuildVersion(), "test_table_large"))
			if err != nil {
				return err
			}
			for _, task := range dmsTasks.ReplicationTasks {
				tableStats, err := dmsCli.DescribeTableStatistics(context.Background(), &dms.DescribeTableStatisticsInput{
					ReplicationTaskArn: task.ReplicationTaskArn,
					Filters: []dmstypes.Filter{{
						Name:   proto.String("table-name"),
						Values: []string{"test_table_large"},
					}},
				})
				if err != nil {
					return err
				}
				// If the task is stopped and stop reason is full load finished, we have succeeded.
				if *task.Status == "stopped" && *task.StopReason == "Stop Reason FULL_LOAD_ONLY_FINISHED" {
					// Check we full loaded the right number of rows
					if tableStats.TableStatistics[0].FullLoadRows == awsrdsNumInitialRows {
						t.L().Printf("test_table_large successfully replicated all rows")
					} else {
						t.L().Printf("row count mismatch: %d vs %d", tableStats.TableStatistics[0].FullLoadRows, awsrdsNumInitialRows)
						t.Fatal("not enough rows replicated from test_table_large")
					}
					close(closer)
				} else if *task.Status == "running" {
					if tableStats.TableStatistics[0].FullLoadRows != awsrdsNumInitialRows {
						if tableStats.TableStatistics[0].FullLoadRows > int64(numRows) {
							nonUpdate = 0
							numRows = int(tableStats.TableStatistics[0].FullLoadRows)
							t.L().Printf("test_table_large still replicating, rows: %d/%d", tableStats.TableStatistics[0].FullLoadRows, awsrdsNumInitialRows)
						} else {
							nonUpdate++
							// Arbitrarily picked 5 consecutive non updates to indicate stuck progress
							if nonUpdate == 5 {
								t.Fatal(errors.New("replication progress appears to be stuck"))
								close(closer)
							}
						}
					}
				} else {
					// All other statuses should result in a failure
					t.L().Printf("unexpeted task status %s", *task.Status)
					t.Fatal("unexpected task status")
					close(closer)
				}
			}
			return nil
		}()
		if err != nil {
			t.L().Printf("error while checking for full load, retrying: %+v", err)
		}
	}
}

// setupAWSDMS sets up an RDS instance and a DMS instance which sets up a
// migration task from the RDS instance to the CockroachDB cluster.
func setupAWSDMS(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	rdsCli *rds.Client,
	dmsCli *dms.Client,
	smCli *secretsmanager.Client,
	dmsTasks *[]dmsTask,
	endpoints *dmsTaskEndpoints,
) (*pgx.Conn, error) {
	var sourcePGConn *pgx.Conn
	if err := func() error {
		var rdsCluster *rdstypes.DBCluster
		var replicationARN string

		var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
		awsdmsPassword := func() string {
			b := make([]rune, 32)
			for i := range b {
				b[i] = letters[rand.Intn(len(letters))]
			}
			return string(b)
		}()

		g := t.NewErrorGroup(task.WithContext(ctx))
		g.Go(setupRDSCluster(ctx, t, rdsCli, awsdmsPassword, &rdsCluster, &sourcePGConn))
		g.Go(setupCockroachDBCluster(ctx, c))
		g.Go(setupDMSReplicationInstance(ctx, t, dmsCli, &replicationARN))

		if err := g.WaitE(); err != nil {
			return err
		}
		smInput := &secretsmanager.GetSecretValueInput{
			SecretId:     proto.String(awsrdsSecretName),
			VersionStage: proto.String("AWSCURRENT"),
		}
		smo, smErr := smCli.GetSecretValue(ctx, smInput)
		if smErr != nil {
			return smErr
		}
		secrets := map[string]string{}
		err := json.Unmarshal([]byte(*smo.SecretString), &secrets)
		if err != nil {
			return err
		}
		rdsInput := &rds.DescribeDBInstancesInput{
			DBInstanceIdentifier: proto.String(awsrdsDBIdentifier),
		}
		rdso, rdsErr := rdsCli.DescribeDBInstances(ctx, rdsInput)
		if rdsErr != nil {
			return rdsErr
		}
		if len(rdso.DBInstances) != 1 {
			return errors.New("RDS instance for large scale replication not found")
		}
		if err := setupDMSEndpointsAndTask(ctx, t, c, dmsCli, rdsCluster, awsdmsPassword, replicationARN, secrets["password"], &rdso.DBInstances[0], dmsTasks, endpoints); err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return nil, errors.Wrapf(err, "failed to set up AWS DMS")
	}
	return sourcePGConn, nil
}

func setupCockroachDBCluster(
	ctx context.Context, c cluster.Cluster,
) func(context.Context, *logger.Logger) error {
	return func(_ context.Context, l *logger.Logger) error {
		l.Printf("setting up cockroach")
		settings := install.MakeClusterSettings(install.SecureOption(false))
		c.Start(ctx, l, option.DefaultStartOpts(), settings, c.All())

		db := c.Conn(ctx, l, 1)
		for _, stmt := range []string{
			fmt.Sprintf("CREATE USER %s", awsdmsCRDBUser),
			fmt.Sprintf("GRANT admin TO %s", awsdmsCRDBUser),
			fmt.Sprintf("ALTER USER %s SET copy_from_atomic_enabled = false", awsdmsCRDBUser),
			fmt.Sprintf("ALTER USER %s SET copy_from_retries_enabled = true", awsdmsCRDBUser),
		} {
			if _, err := db.Exec(stmt); err != nil {
				return err
			}
		}
		return nil
	}
}

func setupDMSReplicationInstance(
	ctx context.Context, t test.Test, dmsCli *dms.Client, replicationARN *string,
) func(context.Context, *logger.Logger) error {
	return func(_ context.Context, l *logger.Logger) error {
		l.Printf("setting up DMS replication instance")
		createReplOut, err := dmsCli.CreateReplicationInstance(
			ctx,
			&dms.CreateReplicationInstanceInput{
				ReplicationInstanceClass:      proto.String("dms.c5.large"),
				ReplicationInstanceIdentifier: proto.String(awsdmsRoachtestDMSReplicationInstanceName(t.BuildVersion())),
			},
		)
		if err != nil {
			return err
		}
		*replicationARN = *createReplOut.ReplicationInstance.ReplicationInstanceArn
		// Wait for replication instance to become available
		l.Printf("waiting for all replication instance to be available")
		if err := dms.NewReplicationInstanceAvailableWaiter(dmsCli).Wait(ctx, dmsDescribeInstancesInput(t.BuildVersion()), awsdmsWaitTimeLimit); err != nil {
			return err
		}
		return nil
	}
}

func setupRDSCluster(
	ctx context.Context,
	t test.Test,
	rdsCli *rds.Client,
	awsdmsPassword string,
	rdsCluster **rdstypes.DBCluster,
	sourcePGConn **pgx.Conn,
) func(context.Context, *logger.Logger) error {
	return func(_ context.Context, l *logger.Logger) error {
		// Setup AWS RDS.
		l.Printf("setting up new AWS RDS parameter group")
		rdsGroup, err := rdsCli.CreateDBClusterParameterGroup(
			ctx,
			&rds.CreateDBClusterParameterGroupInput{
				DBParameterGroupFamily:      proto.String("aurora-postgresql13"),
				DBClusterParameterGroupName: proto.String(awsdmsRoachtestDMSParameterGroup(t.BuildVersion())),
				Description:                 proto.String("roachtest awsdms parameter groups"),
			},
		)
		if err != nil {
			return err
		}

		if _, err := rdsCli.ModifyDBClusterParameterGroup(
			ctx,
			&rds.ModifyDBClusterParameterGroupInput{
				DBClusterParameterGroupName: rdsGroup.DBClusterParameterGroup.DBClusterParameterGroupName,
				Parameters: []rdstypes.Parameter{
					{
						ParameterName:  proto.String("rds.logical_replication"),
						ParameterValue: proto.String("1"),
						// Using ApplyMethodImmediate will error (it is not accepted for
						// this parameter), so using `ApplyMethodPendingReboot` instead. We
						// haven't started the cluster yet, so we can rely on this being
						// setup on first instantiation of the cluster.
						ApplyMethod: rdstypes.ApplyMethodPendingReboot,
					},
				},
			},
		); err != nil {
			return err
		}

		l.Printf("setting up new AWS RDS cluster")
		rdsClusterOutput, err := rdsCli.CreateDBCluster(
			ctx,
			&rds.CreateDBClusterInput{
				DBClusterIdentifier:         proto.String(awsdmsRoachtestRDSClusterName(t.BuildVersion())),
				Engine:                      proto.String("aurora-postgresql"),
				EngineVersion:               proto.String("13"),
				DBClusterParameterGroupName: proto.String(awsdmsRoachtestDMSParameterGroup(t.BuildVersion())),
				MasterUsername:              proto.String(awsdmsUser),
				MasterUserPassword:          proto.String(awsdmsPassword),
				DatabaseName:                proto.String(awsdmsDatabase),
			},
		)
		if err != nil {
			return err
		}
		*rdsCluster = rdsClusterOutput.DBCluster

		l.Printf("setting up new AWS RDS instance")
		if _, err := rdsCli.CreateDBInstance(
			ctx,
			&rds.CreateDBInstanceInput{
				DBInstanceClass:      proto.String("db.r5.large"),
				DBInstanceIdentifier: proto.String(awsdmsRoachtestRDSClusterName(t.BuildVersion()) + "-1"),
				Engine:               proto.String("aurora-postgresql"),
				DBClusterIdentifier:  proto.String(awsdmsRoachtestRDSClusterName(t.BuildVersion())),
				PubliclyAccessible:   proto.Bool(true),
			},
		); err != nil {
			return err
		}

		l.Printf("waiting for RDS instances to become available")
		if err := rds.NewDBInstanceAvailableWaiter(rdsCli).Wait(ctx, rdsDescribeInstancesInput(t.BuildVersion()), awsdmsWaitTimeLimit); err != nil {
			return err
		}
		pgURL := fmt.Sprintf(
			"postgres://%s:%s@%s:%d/%s",
			awsdmsUser,
			awsdmsPassword,
			*rdsClusterOutput.DBCluster.Endpoint,
			*rdsClusterOutput.DBCluster.Port,
			*rdsClusterOutput.DBCluster.DatabaseName,
		)
		if t.IsDebug() {
			l.Printf("pgurl: %s\n", pgURL)
		}
		pgConn, err := pgx.Connect(ctx, pgURL)
		if err != nil {
			return err
		}
		for _, stmt := range []string{
			`CREATE TABLE test_table(id integer PRIMARY KEY, t TEXT)`,
			fmt.Sprintf(
				`INSERT INTO test_table(id, t) SELECT i, md5(random()::text) FROM generate_series(1, %d) AS t(i)`,
				awsdmsNumInitialRows,
			),
			`CREATE TABLE test_table_no_pk(id integer, t TEXT)`,
			fmt.Sprintf(
				`INSERT INTO test_table_no_pk(id, t) SELECT i, md5(random()::text) FROM generate_series(1, %d) AS t(i)`,
				awsdmsNumInitialRows,
			),
		} {
			if _, err := pgConn.Exec(
				ctx,
				stmt,
			); err != nil {
				return err
			}
		}
		*sourcePGConn = pgConn
		return nil
	}
}

func setupDMSEndpointsAndTask(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	dmsCli *dms.Client,
	rdsCluster *rdstypes.DBCluster,
	awsdmsPassword string,
	replicationARN string,
	rdsPasswordLarge string,
	rdsClusterLarge *rdstypes.DBInstance,
	dmsTasks *[]dmsTask,
	dmsEndpoints *dmsTaskEndpoints,
) error {
	// Setup AWS DMS to replicate to CockroachDB.
	externalCRDBAddr, err := c.ExternalIP(ctx, t.L(), option.NodeListOption{1})
	if err != nil {
		return err
	}
	for _, ep := range []struct {
		in       dms.CreateEndpointInput
		endpoint *dmsEndpointState
	}{
		{
			in: dms.CreateEndpointInput{
				EndpointIdentifier: proto.String(dmsEndpoints.defaultSource.id),
				EndpointType:       dmstypes.ReplicationEndpointTypeValueSource,
				EngineName:         proto.String("aurora-postgresql"),
				DatabaseName:       proto.String(awsdmsDatabase),
				Username:           rdsCluster.MasterUsername,
				Password:           proto.String(awsdmsPassword),
				Port:               rdsCluster.Port,
				ServerName:         rdsCluster.Endpoint,
			},
			endpoint: dmsEndpoints.defaultSource,
		},
		{
			in: dms.CreateEndpointInput{
				EndpointIdentifier: proto.String(dmsEndpoints.defaultTarget.id),
				EndpointType:       dmstypes.ReplicationEndpointTypeValueTarget,
				EngineName:         proto.String("postgres"),
				SslMode:            dmstypes.DmsSslModeValueNone,
				PostgreSQLSettings: &dmstypes.PostgreSQLSettings{
					DatabaseName: proto.String(awsdmsCRDBDatabase),
					Username:     proto.String(awsdmsCRDBUser),
					Password:     proto.String(awsdmsPassword),
					Port:         proto.Int32(26257),
					ServerName:   proto.String(externalCRDBAddr[0]),
				},
			},
			endpoint: dmsEndpoints.defaultTarget,
		},
		{
			in: dms.CreateEndpointInput{
				EndpointIdentifier: proto.String(dmsEndpoints.largeSource.id),
				EndpointType:       dmstypes.ReplicationEndpointTypeValueSource,
				EngineName:         proto.String("postgres"),
				DatabaseName:       proto.String(awsdmsDatabase),
				Username:           rdsClusterLarge.MasterUsername,
				Password:           proto.String(rdsPasswordLarge),
				Port:               rdsClusterLarge.Endpoint.Port,
				ServerName:         rdsClusterLarge.Endpoint.Address,
			},
			endpoint: dmsEndpoints.largeSource,
		},
	} {
		t.L().Printf("creating replication endpoint %s", *ep.in.EndpointIdentifier)
		epOut, err := dmsCli.CreateEndpoint(ctx, &ep.in)
		if err != nil {
			return err
		}
		ep.endpoint.arn = *epOut.Endpoint.EndpointArn

		// Test the connections to see if they are "successful".
		// If not, any subsequence DMS task will fail to startup.
		t.L().Printf("testing replication endpoint %s", *ep.in.EndpointIdentifier)
		if _, err := dmsCli.TestConnection(ctx, &dms.TestConnectionInput{
			EndpointArn:            epOut.Endpoint.EndpointArn,
			ReplicationInstanceArn: proto.String(replicationARN),
		}); err != nil {
			return errors.Wrapf(err, "error initiating a test connection")
		}
		r := retry.StartWithCtx(ctx, retry.Options{
			InitialBackoff: 30 * time.Second,
			MaxBackoff:     time.Minute,
			MaxRetries:     10,
		})
		var lastErr error
		for r.Next() {
			if lastErr = func() error {
				result, err := dmsCli.DescribeConnections(
					ctx,
					&dms.DescribeConnectionsInput{
						Filters: []dmstypes.Filter{
							{
								Name:   proto.String("endpoint-arn"),
								Values: []string{*epOut.Endpoint.EndpointArn},
							},
						},
					},
				)
				if err != nil {
					return err
				}
				if len(result.Connections) != 1 {
					return errors.AssertionFailedf("expected exactly one connection during DescribeConnections, found %d", len(result.Connections))
				}
				conn := result.Connections[0]
				if *conn.Status == "successful" {
					return nil
				}
				retErr := errors.Newf(
					"replication test on %s not successful (%s)",
					*ep.in.EndpointIdentifier,
					*conn.Status,
				)
				return retErr
			}(); lastErr == nil {
				t.L().Printf("test for %s successful", *ep.in.EndpointIdentifier)
				break
			} else {
				t.L().Printf("replication endpoint test failed, retrying: %s", lastErr)
			}
		}
		if lastErr != nil {
			return lastErr
		}
	}

	for _, task := range *dmsTasks {
		t.L().Printf(fmt.Sprintf("creating replication task for %s", task.tableName))
		replTaskOut, err := dmsCli.CreateReplicationTask(
			ctx,
			&dms.CreateReplicationTaskInput{
				MigrationType:             task.migrationType,
				ReplicationInstanceArn:    proto.String(replicationARN),
				ReplicationTaskIdentifier: proto.String(awsdmsRoachtestDMSTaskName(t.BuildVersion(), task.tableName)),
				SourceEndpointArn:         proto.String(task.sourceEndpoint.arn),
				TargetEndpointArn:         proto.String(task.targetEndpoint.arn),
				// TODO(#migrations): when AWS API supports EnableValidation, add it here.
				TableMappings:           proto.String(fmt.Sprintf(task.tableMappings, task.tableName)),
				ReplicationTaskSettings: task.replicationSettings,
			},
		)
		if err != nil {
			return err
		}
		t.L().Printf("waiting for replication task to be ready")
		input := dmsDescribeTasksInput(t.BuildVersion(), task.tableName)
		if err = dmsTaskStatusChecker(ctx, dmsCli, input, "ready"); err != nil {
			return err
		}

		t.L().Printf("starting replication task")
		r := retry.StartWithCtx(ctx, retry.Options{
			InitialBackoff: 10 * time.Second,
			MaxBackoff:     20 * time.Second,
			MaxRetries:     10,
		})
		var lastErr error
		for r.Next() {
			if _, lastErr = dmsCli.StartReplicationTask(
				ctx,
				&dms.StartReplicationTaskInput{
					ReplicationTaskArn:       replTaskOut.ReplicationTask.ReplicationTaskArn,
					StartReplicationTaskType: dmstypes.StartReplicationTaskTypeValueReloadTarget,
				},
			); lastErr == nil {
				break
			}
			t.L().Printf("got error starting DMS task; retrying: %+v", err)
		}
		if lastErr != nil {
			return lastErr
		}

		t.L().Printf("waiting for replication task to be running")
		if err = dmsTaskStatusChecker(ctx, dmsCli, input, "running"); err != nil {
			return err
		}
	}
	return nil
}

func dmsTaskStatusChecker(
	ctx context.Context, dmsCli *dms.Client, input *dms.DescribeReplicationTasksInput, status string,
) error {
	closer := make(chan struct{})
	r := retry.StartWithCtx(ctx, retry.Options{
		InitialBackoff: 10 * time.Second,
		MaxBackoff:     30 * time.Second,
		Closer:         closer,
	})
	timeout := time.After(awsdmsWaitTimeLimit)
	for r.Next() {
		select {
		case <-timeout:
			close(closer)
			// Since we only ever have a unique task returned per filter,
			// it should be safe to direct index for the task name.
			return errors.Newf("exceeded time limit waiting for %s to transition to %s", input.Filters[0].Values[0], status)
		default:
			dmsTasks, err := dmsCli.DescribeReplicationTasks(ctx, input)
			if err != nil {
				return err
			}
			for _, task := range dmsTasks.ReplicationTasks {
				// If we match the status we want, close the retry and exit.
				if *task.Status == status {
					close(closer)
				}
			}
		}
	}
	return nil
}

func isDMSResourceNotFound(err error) bool {
	return errors.HasType(err, &dmstypes.ResourceNotFoundFault{})
}

func isDMSResourceAlreadyDeleting(err error) bool {
	return errors.HasType(err, &dmstypes.InvalidResourceStateFault{})
}

func tearDownAWSDMS(
	ctx context.Context, t test.Test, rdsCli *rds.Client, dmsCli *dms.Client, dmsTasks *[]dmsTask,
) error {
	if err := func() error {
		if err := tearDownDMSTasks(ctx, t, dmsCli, dmsTasks); err != nil {
			return err
		}
		if err := tearDownDMSEndpoints(ctx, t, dmsCli, dmsTasks); err != nil {
			return err
		}

		// Delete the replication and rds instances in parallel.
		g := t.NewErrorGroup(task.WithContext(ctx))
		g.Go(tearDownDMSInstances(ctx, t, dmsCli))
		g.Go(tearDownRDSInstances(ctx, t, rdsCli))
		return g.WaitE()
	}(); err != nil {
		return errors.Wrapf(err, "failed to tear down DMS")
	}
	return nil
}

// tearDownDMSTasks tears down the DMS task, endpoints and replication instance
// that may have been created.
func tearDownDMSTasks(
	ctx context.Context, t test.Test, dmsCli *dms.Client, dmsTasks *[]dmsTask,
) error {
	for _, task := range *dmsTasks {
		dmsTasks, err := dmsCli.DescribeReplicationTasks(ctx, dmsDescribeTasksInput(t.BuildVersion(), task.tableName))
		if err != nil {
			if !isDMSResourceNotFound(err) {
				return err
			}
		} else {
			wasRunning := false
			for _, task := range dmsTasks.ReplicationTasks {
				if *task.Status == "running" {
					t.L().Printf("stopping DMS task %s (arn: %s)", *task.ReplicationTaskIdentifier, *task.ReplicationTaskArn)
					if _, err := dmsCli.StopReplicationTask(ctx, &dms.StopReplicationTaskInput{ReplicationTaskArn: task.ReplicationTaskArn}); err != nil {
						return err
					}
					wasRunning = true
				}
			}
			if wasRunning {
				t.L().Printf("waiting for task to be stopped")
				if err := dms.NewReplicationTaskStoppedWaiter(dmsCli).Wait(ctx, dmsDescribeTasksInput(t.BuildVersion(), task.tableName), awsdmsWaitTimeLimit); err != nil {
					return err
				}
			}
			for _, task := range dmsTasks.ReplicationTasks {
				t.L().Printf("deleting DMS task %s (arn: %s)", *task.ReplicationTaskIdentifier, *task.ReplicationTaskArn)
				if _, err := dmsCli.DeleteReplicationTask(ctx, &dms.DeleteReplicationTaskInput{ReplicationTaskArn: task.ReplicationTaskArn}); err != nil {
					return err
				}
			}
			t.L().Printf("waiting for task to be deleted")
			if err := dms.NewReplicationTaskDeletedWaiter(dmsCli).Wait(ctx, dmsDescribeTasksInput(t.BuildVersion(), task.tableName), awsdmsWaitTimeLimit); err != nil {
				return err
			}
		}
	}
	return nil
}

func tearDownDMSEndpoints(
	ctx context.Context, t test.Test, dmsCli *dms.Client, dmsTasks *[]dmsTask,
) error {
	for _, tk := range *dmsTasks {
		for _, ep := range []string{tk.sourceEndpoint.id, tk.targetEndpoint.id} {
			dmsEndpoints, err := dmsCli.DescribeEndpoints(ctx, &dms.DescribeEndpointsInput{
				Filters: []dmstypes.Filter{
					{
						Name:   proto.String("endpoint-id"),
						Values: []string{ep},
					},
				},
			})

			if err != nil {
				if !isDMSResourceNotFound(err) {
					return err
				}
			} else {
				for _, dmsEndpoint := range dmsEndpoints.Endpoints {
					t.L().Printf("deleting DMS endpoint %s (arn: %s)", *dmsEndpoint.EndpointIdentifier, *dmsEndpoint.EndpointArn)
					if _, err := dmsCli.DeleteEndpoint(ctx, &dms.DeleteEndpointInput{EndpointArn: dmsEndpoint.EndpointArn}); err != nil {
						// Because we reuse the same source and target endpoints in some tasks,
						// we need to check for an InvalidResourceState error meaning that its
						// already being deleted and should not return the error.
						if !isDMSResourceAlreadyDeleting(err) {
							return err
						}
					}
				}
			}

		}
	}
	return nil
}

func tearDownDMSInstances(
	ctx context.Context, t test.Test, dmsCli *dms.Client,
) func(context.Context, *logger.Logger) error {
	return func(_ context.Context, l *logger.Logger) error {
		dmsInstances, err := dmsCli.DescribeReplicationInstances(ctx, dmsDescribeInstancesInput(t.BuildVersion()))
		if err != nil {
			if !isDMSResourceNotFound(err) {
				return err
			}
		} else {
			for _, dmsInstance := range dmsInstances.ReplicationInstances {
				l.Printf("deleting DMS replication instance %s (arn: %s)", *dmsInstance.ReplicationInstanceIdentifier, *dmsInstance.ReplicationInstanceArn)
				if _, err := dmsCli.DeleteReplicationInstance(ctx, &dms.DeleteReplicationInstanceInput{
					ReplicationInstanceArn: dmsInstance.ReplicationInstanceArn,
				}); err != nil {
					return err
				}
			}

			// Wait for the replication instance to be deleted.
			l.Printf("waiting for all replication instances to be deleted")
			if err := dms.NewReplicationInstanceDeletedWaiter(dmsCli).Wait(ctx, dmsDescribeInstancesInput(t.BuildVersion()), awsdmsWaitTimeLimit); err != nil {
				return err
			}
		}
		return nil
	}
}

func tearDownRDSInstances(
	ctx context.Context, t test.Test, rdsCli *rds.Client,
) func(context.Context, *logger.Logger) error {
	return func(_ context.Context, l *logger.Logger) error {
		rdsInstances, err := rdsCli.DescribeDBInstances(ctx, rdsDescribeInstancesInput(t.BuildVersion()))
		if err != nil {
			if !errors.HasType(err, &rdstypes.ResourceNotFoundFault{}) {
				return err
			}
		} else {
			for _, rdsInstance := range rdsInstances.DBInstances {
				l.Printf("attempting to delete instance %s", *rdsInstance.DBInstanceIdentifier)
				if _, err := rdsCli.DeleteDBInstance(
					ctx,
					&rds.DeleteDBInstanceInput{
						DBInstanceIdentifier:   rdsInstance.DBInstanceIdentifier,
						DeleteAutomatedBackups: proto.Bool(true),
						SkipFinalSnapshot:      aws.Bool(true),
					},
				); err != nil {
					return err
				}
			}
			l.Printf("waiting for all cluster db instances to be deleted")
			if err := rds.NewDBInstanceDeletedWaiter(rdsCli).Wait(ctx, rdsDescribeInstancesInput(t.BuildVersion()), awsdmsWaitTimeLimit); err != nil {
				return err
			}
		}

		// Delete RDS clusters that may be created.
		rdsClusters, err := rdsCli.DescribeDBClusters(ctx, &rds.DescribeDBClustersInput{
			Filters: rdsClusterFilters(t.BuildVersion()),
		})
		if err != nil {
			if !errors.HasType(err, &rdstypes.ResourceNotFoundFault{}) {
				return err
			}
		} else {
			for _, rdsCluster := range rdsClusters.DBClusters {
				l.Printf("attempting to delete cluster %s", *rdsCluster.DBClusterIdentifier)
				if _, err := rdsCli.DeleteDBCluster(
					ctx,
					&rds.DeleteDBClusterInput{
						DBClusterIdentifier: rdsCluster.DBClusterIdentifier,
						SkipFinalSnapshot:   aws.Bool(true),
					},
				); err != nil {
					return err
				}
			}
		}
		rdsParamGroups, err := rdsCli.DescribeDBClusterParameterGroups(ctx, &rds.DescribeDBClusterParameterGroupsInput{
			DBClusterParameterGroupName: proto.String(awsdmsRoachtestDMSParameterGroup(t.BuildVersion())),
		})
		if err != nil {
			// Sometimes they don't deserialize to DBClusterParameterGroupNotFoundFault :\.
			if !errors.HasType(err, &rdstypes.DBClusterParameterGroupNotFoundFault{}) && !errors.HasType(err, &rdstypes.DBParameterGroupNotFoundFault{}) {
				return err
			}
		} else {
			for _, rdsGroup := range rdsParamGroups.DBClusterParameterGroups {
				l.Printf("attempting to delete param group %s", *rdsGroup.DBClusterParameterGroupName)

				// This can sometimes fail as the cluster still relies on the param
				// group but the cluster takes a while to wind down. Ideally, we wait
				// for the cluster to get deleted. However, there is no provided
				// NewDBClusterDeletedWaiter, so we just have to retry it by hand.
				r := retry.StartWithCtx(ctx, retry.Options{
					InitialBackoff: 30 * time.Second,
					MaxBackoff:     time.Minute,
					MaxRetries:     60,
				})
				var lastErr error
				for r.Next() {
					_, err = rdsCli.DeleteDBClusterParameterGroup(
						ctx,
						&rds.DeleteDBClusterParameterGroupInput{
							DBClusterParameterGroupName: rdsGroup.DBClusterParameterGroupName,
						},
					)
					lastErr = err
					if err == nil {
						break
					}
					l.Printf("expected error: failed to delete cluster param group, retrying: %+v", err)
				}
				if lastErr != nil {
					return errors.Wrapf(lastErr, "failed to delete param group")
				}
			}
		}

		return nil
	}
}
