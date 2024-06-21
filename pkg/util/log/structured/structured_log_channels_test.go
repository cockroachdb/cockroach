// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slog_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	slog "github.com/cockroachdb/cockroach/pkg/util/log/structured"
	"github.com/cockroachdb/datadriven"
)

var testMeta = log.StructuredLogMeta{
	EventType: "test_log",
	Version:   "0.1",
}

type TestSlog struct {
	Field1 string `json:"field1"`
	Field2 int    `json:"field2"`
}

type TestSlogPayload struct {
	Metadata log.StructuredLogMeta `json:"metadata"`
	Payload  TestSlog              `json:"payload"`
}

func TestSlogChannels(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Some queries may be retried under stress.
	skip.UnderRace(t, "results inconsistent under stress")

	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)
	logSpy := logtestutils.NewStructuredLogSpy(
		t,
		[]logpb.Channel{logpb.Channel_OPS, logpb.Channel_OPS,
			logpb.Channel_STRUCTURED_EVENTS,
			logpb.Channel_DEV,
			logpb.Channel_HEALTH,
			logpb.Channel_STORAGE,
			logpb.Channel_SESSIONS,
			logpb.Channel_SQL_SCHEMA,
			logpb.Channel_USER_ADMIN,
			logpb.Channel_PRIVILEGES,
			logpb.Channel_SENSITIVE_ACCESS,
			logpb.Channel_SQL_EXEC,
			logpb.Channel_SQL_PERF,
			logpb.Channel_SQL_INTERNAL_PERF,
			logpb.Channel_TELEMETRY,
			logpb.Channel_KV_DISTRIBUTION},
		[]string{"test_log"},
		func(entry logpb.Entry) (TestSlogPayload, error) {
			var payload TestSlogPayload
			err := json.Unmarshal([]byte(entry.Message[entry.StructuredStart:entry.StructuredEnd]), &payload)
			if err != nil {
				return payload, err
			}
			return payload, nil
		},
	)

	cleanup := log.InterceptWith(context.Background(), logSpy)
	defer cleanup()
	ctx := context.Background()
	datadriven.Walk(t, datapathutils.TestDataPath(t, ""), func(t *testing.T, path string) {
		logSpy.Reset()
		sts := logtestutils.StubTracingStatus{}
		tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
		s := tc.ApplicationLayer(0)
		setupConn := s.SQLConn(t)
		defer tc.Stopper().Stop(context.Background())
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "enable-slog":
				sts.SetTracingStatus(false)
				_, err := setupConn.Exec(`SET CLUSTER SETTING sql.log.structured_logs.enabled = true;`)
				if err != nil {
					return err.Error()
				}
				return ""
			case "disable-slog":
				sts.SetTracingStatus(false)
				_, err := setupConn.Exec(`SET CLUSTER SETTING sql.log.structured_logs.enabled = false;`)
				if err != nil {
					return err.Error()
				}
				return ""
			case "slog-channel":
				slogger := getSlogger(t, d.Input)
				return logAndSpy(t, ctx, slogger, logSpy)
			default:
				return ""
			}
		})
	})
}

func logAndSpy(
	t *testing.T,
	ctx context.Context,
	slogger slog.StructuredLogger,
	spy *logtestutils.StructuredLogSpy[TestSlogPayload],
) string {
	l := TestSlog{
		Field1: "log message",
		Field2: 1000,
	}
	slogger.Info(ctx, testMeta, l)
	unreadLogs := spy.GetUnreadLogs(slogger.Channel())
	if len(unreadLogs) == 0 {
		return ""
	}

	s, err := json.Marshal(unreadLogs[0])
	if err != nil {
		t.Fatal("couldn't marshal structured log")
	}
	return string(s)
}

func getSlogger(t *testing.T, input string) slog.StructuredLogger {
	switch input {
	case "OpsLogger":
		return slog.OpsLogger
	case "EventsLogger":
		return slog.EventsLogger
	case "DevLogger":
		return slog.DevLogger
	case "HealthLogger":
		return slog.HealthLogger
	case "StorageLogger":
		return slog.StorageLogger
	case "SessionsLogger":
		return slog.SessionsLogger
	case "SqlSchemaLogger":
		return slog.SqlSchemaLogger
	case "UserAdminLogger":
		return slog.UserAdminLogger
	case "PrivilegesLogger":
		return slog.PrivilegesLogger
	case "SensitiveAccessLogger":
		return slog.SensitiveAccessLogger
	case "SqlExecLogger":
		return slog.SqlExecLogger
	case "SqlPerfLogger":
		return slog.SqlPerfLogger
	case "SqlInternalPerfLogger":
		return slog.SqlInternalPerfLogger
	case "TelemetryLogger":
		return slog.TelemetryLogger
	case "KvLogger":
		return slog.KvLogger
	default:
		t.Fatal("Unknown Logger")
		return slog.StructuredLogger{}
	}
}
