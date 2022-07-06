// Copyright 2022 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package ingest

import (
	"context"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/migrations"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	otel_pb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/common/v1"
	otlogs "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/logs/v1"
	v1 "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/resource/v1"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestPersistEvents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, s.ServingSQLAddr(),
		"TestPersistEvents", url.User(username.RootUser),
	)
	defer cleanupFunc()

	config, err := pgxpool.ParseConfig(pgURL.String())
	require.NoError(t, err)
	config.ConnConfig.Database = "defaultdb"
	pool, err := pgxpool.ConnectConfig(ctx, config)
	require.NoError(t, err)
	defer pool.Close()
	require.NoError(t, migrations.RunDBMigrations(ctx, config.ConnConfig))

	var events otlogs.ResourceLogs
	clusterID, err := uuid.NewRandom()
	require.NoError(t, err)
	const nodeID = 42
	const nodeBinaryVersion = "25.2.1"
	events.Resource = &v1.Resource{
		Attributes: []*otel_pb.KeyValue{
			{
				Key:   obspb.ClusterID,
				Value: &otel_pb.AnyValue{Value: &otel_pb.AnyValue_StringValue{StringValue: clusterID.String()}},
			},
			{
				Key:   obspb.NodeID,
				Value: &otel_pb.AnyValue{Value: &otel_pb.AnyValue_IntValue{IntValue: int64(nodeID)}},
			},
			{
				Key:   obspb.NodeBinaryVersion,
				Value: &otel_pb.AnyValue{Value: &otel_pb.AnyValue_StringValue{StringValue: nodeBinaryVersion}},
			},
		},
	}
	events.ScopeLogs = append(events.ScopeLogs, otlogs.ScopeLogs{
		Scope: &otel_pb.InstrumentationScope{
			Name: string(obspb.EventlogEvent),
		},
	})

	numEvents := 2*maxEventsPerStatement + maxEventsPerStatement/2
	now := timeutil.Now()
	for i := 0; i < numEvents; i++ {
		events.ScopeLogs[0].LogRecords = append(events.ScopeLogs[0].LogRecords, otlogs.LogRecord{
			TimeUnixNano: uint64(now.UnixNano()),
			Body: &otel_pb.AnyValue{Value: &otel_pb.AnyValue_StringValue{
				// A dummy event value. It needs to be JSON.
				StringValue: "{\"Timestamp\":1659477303978528869,\"EventType\":\"set_cluster_setting\",\"Statement\":\"SET CLUSTER SETTING version = $1\",\"Tag\":\"SET CLUSTER SETTING\",\"User\":\"root\",\"ApplicationName\":\"$ internal-set-setting\",\"PlaceholderValues\":[\"'22.1-26'\"],\"SettingName\":\"version\",\"Value\":\"22.1-26\"}"}},
		})
	}

	require.NoError(t, persistEvents(ctx, []*otlogs.ResourceLogs{&events}, pool))
	r := pool.QueryRow(ctx, "select count(1) from cluster_events")
	var count int
	require.NoError(t, r.Scan(&count))
	require.Equal(t, numEvents, count)
	// !!! check more things about the values of the events in the database
}

// Test an end-to-end integration between the ObsService and a CRDB cluster:
// verify that events get exported from CRDB and imported in the Obs Service.
func TestEventIngestionIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, s.ServingSQLAddr(),
		"TestPersistEvents", url.User(username.RootUser),
	)
	defer cleanupFunc()

	config, err := pgxpool.ParseConfig(pgURL.String())
	require.NoError(t, err)
	config.ConnConfig.Database = "defaultdb"
	pool, err := pgxpool.ConnectConfig(ctx, config)
	require.NoError(t, err)
	defer pool.Close()
	require.NoError(t, migrations.RunDBMigrations(ctx, config.ConnConfig))

	// Start the ingestion in the background.
	obsStop := stop.NewStopper()
	defer obsStop.Stop(ctx)
	StartIngestEvents(ctx, s.RPCAddr(), pool, obsStop)

	// Perform a schema change and check that we get an event.
	_, err = sqlDB.Exec("create table t()")
	require.NoError(t, err)

	r := pool.QueryRow(ctx, "select count(1) from cluster_events")
	var count int
	require.NoError(t, r.Scan(&count))
	require.GreaterOrEqual(t, 1, count)
}
