// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package serverccl

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

var adminPrefix = "/_admin/v1/"

func getAdminJSONProto(
	ts serverutils.TestServerInterface, path string, response protoutil.Message,
) error {
	return getAdminJSONProtoWithAdminOption(ts, path, response, true)
}

func getAdminJSONProtoWithAdminOption(
	ts serverutils.TestServerInterface, path string, response protoutil.Message, isAdmin bool,
) error {
	return serverutils.GetJSONProtoWithAdminOption(ts, adminPrefix+path, response, isAdmin)
}

// TestAdminAPIDataDistributionPartitioning partitions a table and verifies
// that we see all zone configs (#27718).
func TestAdminAPIDataDistributionPartitioning(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())

	firstServer := testCluster.Server(0)
	sqlDB := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))

	sqlDB.Exec(t, `CREATE DATABASE roachblog`)
	sqlDB.Exec(t, `USE roachblog`)
	sqlDB.Exec(t, `CREATE TABLE posts (id INT PRIMARY KEY, title text, body text)`)
	sqlDB.Exec(t, `CREATE TABLE comments (
		id INT,
		post_id INT REFERENCES posts,
		user_region STRING,
		body text,
		PRIMARY KEY (user_region, id)
	) PARTITION BY LIST (user_region) (
		PARTITION us VALUES IN ('US'),
		PARTITION eu VALUES IN ('EU'),
		PARTITION DEFAULT VALUES IN (default)
	)`)

	// Create a zone config for each partition.
	// Would use locality constraints except this test cluster hasn't been started up with localities.
	sqlDB.Exec(t, `ALTER PARTITION us OF TABLE comments CONFIGURE ZONE USING gc.ttlseconds = 9001`)
	sqlDB.Exec(t, `ALTER PARTITION eu OF TABLE comments CONFIGURE ZONE USING gc.ttlseconds = 9002`)

	// Assert that we get all roachblog zone configs back.
	expectedZoneConfigNames := map[string]struct{}{
		"PARTITION eu OF INDEX roachblog.public.comments@primary": {},
		"PARTITION us OF INDEX roachblog.public.comments@primary": {},
	}

	var resp serverpb.DataDistributionResponse
	if err := serverutils.GetJSONProto(firstServer, adminPrefix+"data_distribution", &resp); err != nil {
		t.Fatal(err)
	}

	actualZoneConfigNames := map[string]struct{}{}
	for name := range resp.ZoneConfigs {
		if strings.Contains(name, "roachblog") {
			actualZoneConfigNames[name] = struct{}{}
		}
	}
	if !reflect.DeepEqual(actualZoneConfigNames, expectedZoneConfigNames) {
		t.Fatalf("expected zone config names %v; got %v", expectedZoneConfigNames, actualZoneConfigNames)
	}
}

// TestAdminAPIChartCatalog verifies that an error doesn't happen.
func TestAdminAPIChartCatalog(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())

	firstServer := testCluster.Server(0)

	var resp serverpb.ChartCatalogResponse
	err := serverutils.GetJSONProto(firstServer, adminPrefix+"chartcatalog", &resp)
	require.NoError(t, err)
}

// getScheduleIDs queries the scheduled jobs table for all schedule IDs that
// have the given status.
func getScheduleIDs(t testing.TB, db *sqlutils.SQLRunner) []int64 {
	rows := db.Query(t, `SELECT schedule_id FROM system.scheduled_jobs`)
	defer rows.Close()

	res := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		res = append(res, id)
	}
	return res
}

func TestAdminAPISchedules(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(conn)

	scheduleIDsBeforeBackup := getScheduleIDs(t, sqlDB)
	unpausedBackupScheduleIDs := make([]int64, 0)
	pausedBackupScheduleIDs := make([]int64, 0)
	testSchedules := []struct {
		query string
	}{
		{
			query: "CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://0/backup/1?AWS_SECRET_ACCESS_KEY=neverappears' RECURRING '@hourly'",
		},
		{
			query: "CREATE SCHEDULE 'my-backup' FOR BACKUP INTO 'nodelocal://0/backup/2' RECURRING '@hourly'",
		},
		{
			query: "CREATE SCHEDULE FOR BACKUP INTO 'nodelocal://0/backup/3' RECURRING '@hourly' FULL BACKUP ALWAYS",
		},
	}
	for _, test := range testSchedules {
		rows, err := sqlDB.DB.QueryContext(ctx, test.query)
		require.NoError(t, err)

		var unusedStr string
		var unusedTS *time.Time
		var status string
		for rows.Next() {
			var id int64
			require.NoError(t, rows.Scan(&id, &unusedStr, &status, &unusedTS, &unusedStr, &unusedStr))
			fmt.Println(status)
			if strings.Contains(status, "PAUSED") {
				pausedBackupScheduleIDs = append(pausedBackupScheduleIDs, id)
			} else {
				unpausedBackupScheduleIDs = append(unpausedBackupScheduleIDs, id)
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
	allBackupScheduleIDs := append(pausedBackupScheduleIDs, unpausedBackupScheduleIDs...)
	allScheduleIDs := append(scheduleIDsBeforeBackup, allBackupScheduleIDs...)

	const invalidScheduleType = "boom"

	testCases := []struct {
		uri                 string
		expectedScheduleIDs []int64
		err                 string
	}{
		{
			"schedules",
			allScheduleIDs,
			"",
		},
		{
			"schedules?limit=1",
			[]int64{scheduleIDsBeforeBackup[0]},
			"",
		},
		{
			"schedules?status=paused",
			pausedBackupScheduleIDs,
			"",
		},
		{
			"schedules?status=garbage",
			[]int64{},
			"invalid schedule status",
		},
		{
			fmt.Sprintf("schedules?type=%s", invalidScheduleType),
			[]int64{},
			"invalid schedule type",
		},
		{
			fmt.Sprintf("schedules?type=%s", tree.ScheduledBackupExecutor.InternalName()),
			allBackupScheduleIDs,
			"",
		},
		{
			fmt.Sprintf("schedules?status=running&type=%s", tree.ScheduledBackupExecutor.InternalName()),
			unpausedBackupScheduleIDs,
			"",
		},
	}

	for i, testCase := range testCases {
		t.Run(testCase.uri, func(t *testing.T) {
			var res serverpb.SchedulesResponse
			if err := getAdminJSONProto(s, testCase.uri, &res); err != nil {
				if testCase.err == "" {
					t.Fatal(err)
				}
				testutils.IsError(err, testCase.err)
			}
			resIDs := []int64{}
			for _, schedule := range res.Schedules {
				fmt.Println(schedule.String())
				resIDs = append(resIDs, schedule.ID)
			}

			expected := testCase.expectedScheduleIDs

			sort.Slice(expected, func(i, j int) bool {
				return expected[i] < expected[j]
			})

			sort.Slice(resIDs, func(i, j int) bool {
				return resIDs[i] < resIDs[j]
			})

			if e, a := expected, resIDs; !reflect.DeepEqual(e, a) {
				t.Errorf("%d: expected schedule IDs %v, but got %v", i, e, a)
			}
		})
	}
}
