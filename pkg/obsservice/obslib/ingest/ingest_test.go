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
	gosql "database/sql"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/obs"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/migrations"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	logspb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/collector/logs/v1"
	otel_pb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/common/v1"
	otlogs "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/logs/v1"
	v1 "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/resource/v1"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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
	const instanceID = 42
	const nodeBinaryVersion = "25.2.1"
	events.Resource = &v1.Resource{
		Attributes: []*otel_pb.KeyValue{
			{
				Key:   obspb.ClusterID,
				Value: &otel_pb.AnyValue{Value: &otel_pb.AnyValue_StringValue{StringValue: clusterID.String()}},
			},
			{
				Key:   obspb.NodeID,
				Value: &otel_pb.AnyValue{Value: &otel_pb.AnyValue_IntValue{IntValue: int64(instanceID)}},
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

	const eventType = "test event type"
	numEvents := 2*maxEventsPerStatement + maxEventsPerStatement/2
	now := timeutil.Now()
	// A dummy event value. It needs to be JSON. Note that the keys are ordered,
	// so they match the string that comes out of the database when reading.
	eventData := `{"ApplicationName": "$ internal-set-setting", "EventType": "set_cluster_setting", "PlaceholderValues": ["'22.1-26'"], "SettingName": "version", "Statement": "SET CLUSTER SETTING version = $1", "Tag": "SET CLUSTER SETTING", "Timestamp": 1659477303978528869, "User": "root", "Value": "22.1-26"}`
	for i := 0; i < numEvents; i++ {
		events.ScopeLogs[0].LogRecords = append(events.ScopeLogs[0].LogRecords,
			otlogs.LogRecord{
				TimeUnixNano: uint64(now.UnixNano()),
				Attributes: []*otel_pb.KeyValue{
					{
						Key:   obspb.EventlogEventTypeAttribute,
						Value: &otel_pb.AnyValue{Value: &otel_pb.AnyValue_StringValue{StringValue: eventType}},
					},
				},
				Body: &otel_pb.AnyValue{Value: &otel_pb.AnyValue_StringValue{StringValue: eventData}},
			})
	}

	require.NoError(t, persistEvents(ctx, []*otlogs.ResourceLogs{&events}, pool))
	r := pool.QueryRow(ctx, "select count(1) from cluster_events")
	var count int
	require.NoError(t, r.Scan(&count))
	require.Equal(t, numEvents, count)
	r = pool.QueryRow(ctx, "select timestamp, cluster_id, instance_id, event_type, event from cluster_events limit 1")
	var timestamp time.Time
	var typ, ev string
	var cID []byte
	var iID int
	require.NoError(t, r.Scan(&timestamp, &cID, &iID, &typ, &ev))
	require.True(t, timestamp.Before(timeutil.Now()))
	require.Less(t, timeutil.Since(timestamp), time.Hour)
	require.Equal(t, typ, eventType)
	cUUID, err := uuid.ParseBytes(cID)
	require.NoError(t, err)
	require.Equal(t, clusterID, cUUID)
	require.Equal(t, instanceID, iID)
	require.Equal(t, eventData, ev)
}

// Test an end-to-end integration between the ObsService and a CRDB cluster:
// verify that events get exported from CRDB and imported in the Obs Service.
func TestEventIngestionIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testutils.RunTrueAndFalse(t, "embed", func(t *testing.T, embed bool) {
		var obsAddr string

		var s serverutils.TestServerInterface
		var sqlDB *gosql.DB
		if !embed {
			// Allocate a port for the ingestion service to work around a circular
			// dependency: CRDB needs to be told what the port is, but we can only create
			// the event ingester after having started CRDB (because the ingester wants a
			// reference to CRDB).
			otlpListener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer func() {
				_ = otlpListener.Close()
			}()
			obsAddr = otlpListener.Addr().String()
			s, sqlDB, _ = serverutils.StartServer(t,
				base.TestServerArgs{
					ObsServiceAddr: obsAddr,
					Knobs: base.TestingKnobs{
						EventExporter: &obs.EventExporterTestingKnobs{
							// Flush every message.
							FlushTriggerByteSize: 1,
						},
					},
				},
			)
			defer s.Stopper().Stop(ctx)

			pgURL, cleanupFunc := sqlutils.PGUrl(
				t, s.ServingSQLAddr(), "TestPersistEvents", url.User(username.RootUser),
			)
			defer cleanupFunc()

			config, err := pgxpool.ParseConfig(pgURL.String())
			require.NoError(t, err)
			config.ConnConfig.Database = "crdb_observability"
			pool, err := pgxpool.ConnectConfig(ctx, config)
			require.NoError(t, err)
			defer pool.Close()
			require.NoError(t, migrations.RunDBMigrations(ctx, config.ConnConfig))

			// Start the ingestion in the background.
			obsStop := stop.NewStopper()
			defer obsStop.Stop(ctx)
			e, err := MakeEventIngester(ctx, config)
			require.NoError(t, err)
			defer e.Close()
			grpcServer := grpc.NewServer()
			defer grpcServer.Stop()
			logspb.RegisterLogsServiceServer(grpcServer, &e)
			go func() {
				_ = grpcServer.Serve(otlpListener)
			}()
		} else {
			s, sqlDB, _ = serverutils.StartServer(t,
				base.TestServerArgs{
					ObsServiceAddr: base.ObsServiceEmbedFlagValue,
					Knobs: base.TestingKnobs{
						EventExporter: &obs.EventExporterTestingKnobs{
							// Flush every message.
							FlushTriggerByteSize: 1,
						},
					},
				},
			)
			defer s.Stopper().Stop(ctx)
		}

		// Perform a schema change and check that we get an event.
		_, err := sqlDB.Exec("create table t()")
		require.NoError(t, err)

		// Wait for an event to be ingested.
		testutils.SucceedsSoon(t, func() error {
			r := sqlDB.QueryRow("SELECT count(*) FROM crdb_observability.cluster_events WHERE event_type='create_table'")
			var count int
			require.NoError(t, r.Scan(&count))
			if count < 1 {
				return errors.Newf("no events yet")
			}
			return nil
		})
	})

}
