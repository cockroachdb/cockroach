// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/idxusage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type stubUnusedIndexTime struct {
	syncutil.RWMutex
	current   time.Time
	lastRead  time.Time
	createdAt *time.Time
}

func (s *stubUnusedIndexTime) setCurrent(t time.Time) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.current = t
}

func (s *stubUnusedIndexTime) setLastRead(t time.Time) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.lastRead = t
}

func (s *stubUnusedIndexTime) setCreatedAt(t *time.Time) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.createdAt = t
}

func (s *stubUnusedIndexTime) getCurrent() time.Time {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.current
}

func (s *stubUnusedIndexTime) getLastRead() time.Time {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.lastRead
}

func (s *stubUnusedIndexTime) getCreatedAt() *time.Time {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.createdAt
}

func TestDatabaseAndTableIndexRecommendations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stubTime := stubUnusedIndexTime{}
	stubDropUnusedDuration := time.Hour

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TODOTestTenantDisabled,
		Knobs: base.TestingKnobs{
			UnusedIndexRecommendKnobs: &idxusage.UnusedIndexRecommendationTestingKnobs{
				GetCreatedAt:   stubTime.getCreatedAt,
				GetLastRead:    stubTime.getLastRead,
				GetCurrentTime: stubTime.getCurrent,
			},
		},
	})
	idxusage.DropUnusedIndexDuration.Override(context.Background(), &s.ClusterSettings().SV, stubDropUnusedDuration)
	defer s.Stopper().Stop(context.Background())

	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, "CREATE DATABASE test")
	db.Exec(t, "USE test")
	// Create a table and secondary index.
	db.Exec(t, "CREATE TABLE test.test_table (num INT PRIMARY KEY, letter char)")
	db.Exec(t, "CREATE INDEX test_idx ON test.test_table (letter)")

	// Test when last read does not exist and there is no creation time. Expect
	// an index recommendation (index never used).
	stubTime.setLastRead(time.Time{})
	stubTime.setCreatedAt(nil)

	// Test database details endpoint.
	var dbDetails serverpb.DatabaseDetailsResponse
	if err := srvtestutils.GetAdminJSONProto(
		s,
		"databases/test?include_stats=true",
		&dbDetails,
	); err != nil {
		t.Fatal(err)
	}
	// Expect 1 index recommendation (no index recommendation on primary index).
	require.Equal(t, int32(1), dbDetails.Stats.NumIndexRecommendations)

	// Test table details endpoint.
	var tableDetails serverpb.TableDetailsResponse
	if err := srvtestutils.GetAdminJSONProto(s, "databases/test/tables/test_table", &tableDetails); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, true, tableDetails.HasIndexRecommendations)

	// Test when last read does not exist and there is a creation time, and the
	// unused index duration has been exceeded. Expect an index recommendation.
	currentTime := timeutil.Now()
	createdTime := currentTime.Add(-stubDropUnusedDuration)
	stubTime.setCurrent(currentTime)
	stubTime.setLastRead(time.Time{})
	stubTime.setCreatedAt(&createdTime)

	// Test database details endpoint.
	dbDetails = serverpb.DatabaseDetailsResponse{}
	if err := srvtestutils.GetAdminJSONProto(
		s,
		"databases/test?include_stats=true",
		&dbDetails,
	); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, int32(1), dbDetails.Stats.NumIndexRecommendations)

	// Test table details endpoint.
	tableDetails = serverpb.TableDetailsResponse{}
	if err := srvtestutils.GetAdminJSONProto(s, "databases/test/tables/test_table", &tableDetails); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, true, tableDetails.HasIndexRecommendations)

	// Test when last read does not exist and there is a creation time, and the
	// unused index duration has not been exceeded. Expect no index
	// recommendation.
	currentTime = timeutil.Now()
	stubTime.setCurrent(currentTime)
	stubTime.setLastRead(time.Time{})
	stubTime.setCreatedAt(&currentTime)

	// Test database details endpoint.
	dbDetails = serverpb.DatabaseDetailsResponse{}
	if err := srvtestutils.GetAdminJSONProto(
		s,
		"databases/test?include_stats=true",
		&dbDetails,
	); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, int32(0), dbDetails.Stats.NumIndexRecommendations)

	// Test table details endpoint.
	tableDetails = serverpb.TableDetailsResponse{}
	if err := srvtestutils.GetAdminJSONProto(s, "databases/test/tables/test_table", &tableDetails); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, false, tableDetails.HasIndexRecommendations)

	// Test when last read exists and the unused index duration has been
	// exceeded. Expect an index recommendation.
	currentTime = timeutil.Now()
	lastRead := currentTime.Add(-stubDropUnusedDuration)
	stubTime.setCurrent(currentTime)
	stubTime.setLastRead(lastRead)
	stubTime.setCreatedAt(nil)

	// Test database details endpoint.
	dbDetails = serverpb.DatabaseDetailsResponse{}
	if err := srvtestutils.GetAdminJSONProto(
		s,
		"databases/test?include_stats=true",
		&dbDetails,
	); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, int32(1), dbDetails.Stats.NumIndexRecommendations)

	// Test table details endpoint.
	tableDetails = serverpb.TableDetailsResponse{}
	if err := srvtestutils.GetAdminJSONProto(s, "databases/test/tables/test_table", &tableDetails); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, true, tableDetails.HasIndexRecommendations)

	// Test when last read exists and the unused index duration has not been
	// exceeded. Expect no index recommendation.
	currentTime = timeutil.Now()
	stubTime.setCurrent(currentTime)
	stubTime.setLastRead(currentTime)
	stubTime.setCreatedAt(nil)

	// Test database details endpoint.
	dbDetails = serverpb.DatabaseDetailsResponse{}
	if err := srvtestutils.GetAdminJSONProto(
		s,
		"databases/test?include_stats=true",
		&dbDetails,
	); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, int32(0), dbDetails.Stats.NumIndexRecommendations)

	// Test table details endpoint.
	tableDetails = serverpb.TableDetailsResponse{}
	if err := srvtestutils.GetAdminJSONProto(s, "databases/test/tables/test_table", &tableDetails); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, false, tableDetails.HasIndexRecommendations)
}
