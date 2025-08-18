// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestStructuredEventMigrator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ts, _ := time.Parse(time.DateOnly, "2025-01-01")
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "log_migration"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "structured-event":
				var shouldMigrate bool
				var newChannelStr string
				var depth int
				d.ScanArgs(t, "should_migrate", &shouldMigrate)
				d.ScanArgs(t, "new_channel", &newChannelStr)
				depthIncluded := d.MaybeScanArgs(t, "depth", &depth)
				newChannel, ok := GetChannelFromString(newChannelStr)
				require.True(t, ok, "unknown channel %q", newChannelStr)

				migrationChannelSpy := logtestutils.NewStructuredLogSpy(
					t,
					[]logpb.Channel{newChannel},
					[]string{logtestutils.TestEventType},
					func(entry logpb.Entry) (logpb.Entry, error) {
						return entry, nil
					},
				)

				eventChannelSpy := logtestutils.NewStructuredLogSpy(
					t,
					[]logpb.Channel{logtestutils.TestEvent{}.LoggingChannel()},
					[]string{logtestutils.TestEventType},
					func(entry logpb.Entry) (logpb.Entry, error) {
						return entry, nil
					},
				)
				ctx := context.Background()
				cleanupMigrationChannelSpy := log.InterceptWith(ctx, migrationChannelSpy)
				cleanupEventChannelSpy := log.InterceptWith(ctx, eventChannelSpy)
				defer cleanupMigrationChannelSpy()
				defer cleanupEventChannelSpy()

				migrator := log.NewStructuredEventMigrator(func() bool {
					return shouldMigrate
				}, newChannel)

				event := logtestutils.TestEvent{Timestamp: ts.UnixNano()}
				if depthIncluded {
					migrator.StructuredEventDepth(context.Background(), severity.INFO, depth, &event)
				} else {
					migrator.StructuredEvent(context.Background(), severity.INFO, &event)
				}

				return fmt.Sprintf("%s Channel: \n%+v\n%s Channel: \n%+v\n",
					logtestutils.TestEvent{}.LoggingChannel(),
					cleanLogEntries(eventChannelSpy.GetLogs(logtestutils.TestEvent{}.LoggingChannel())),
					newChannel,
					cleanLogEntries(migrationChannelSpy.GetLogs(newChannel)),
				)

			case "log":
				var shouldMigrate bool
				var oldChannelStr string
				var newChannelStr string
				d.ScanArgs(t, "should_migrate", &shouldMigrate)
				d.ScanArgs(t, "old_channel", &oldChannelStr)
				d.ScanArgs(t, "new_channel", &newChannelStr)
				oldChannel, ok := GetChannelFromString(oldChannelStr)
				require.True(t, ok, "unknown channel %q", oldChannelStr)
				newChannel, ok := GetChannelFromString(newChannelStr)
				require.True(t, ok, "unknown channel %q", newChannelStr)

				logPrefix := "migration log"
				oldChannelSpy := logtestutils.NewLogSpy(t, func(entry logpb.Entry) bool {
					return entry.Channel == oldChannel && logtestutils.MatchesF(logPrefix)(entry)
				})
				newChannelSpy := logtestutils.NewLogSpy(t, func(entry logpb.Entry) bool {
					return entry.Channel == newChannel && logtestutils.MatchesF(logPrefix)(entry)
				})

				ctx := context.Background()
				cleanupOldChannelSpy := log.InterceptWith(ctx, oldChannelSpy)
				cleanupNewChannelSpy := log.InterceptWith(ctx, newChannelSpy)

				defer cleanupOldChannelSpy()
				defer cleanupNewChannelSpy()

				migrator := log.NewMigrator(func() bool {
					return shouldMigrate
				}, oldChannel, newChannel)

				migrator.Infof(ctx, "%s Infof", logPrefix)
				migrator.Warningf(ctx, "%s Warningf", logPrefix)
				migrator.Errorf(ctx, "%s Errorf", logPrefix)

				return fmt.Sprintf("%s Channel: \n%+v\n%s Channel: \n%+v\n",
					oldChannel,
					cleanLogEntries(oldChannelSpy.ReadAll()),
					newChannel,
					cleanLogEntries(newChannelSpy.ReadAll()),
				)
			default:
				return errors.Newf("unknown command").Error()
			}
		})
}

func BenchmarkMigrator(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	b.ReportAllocs()

	b.Run("StructuredEvent", func(b *testing.B) {
		ts, _ := time.Parse(time.DateOnly, "2025-01-01")
		event := logtestutils.TestEvent{Timestamp: ts.UnixNano()}
		b.Run("no_migrator", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				log.StructuredEvent(context.Background(), severity.INFO, &event)
			}
		})

		b.Run("migrator", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				m := log.NewStructuredEventMigrator(func() bool {
					return false
				}, logpb.Channel_CHANGEFEED)
				m.StructuredEvent(context.Background(), severity.INFO, &event)
			}
		})
	})

	b.Run("Log", func(b *testing.B) {
		const logStr = "Logging this message"
		b.Run("no_migrator", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				log.Dev.Infof(context.Background(), logStr)
			}
		})
		b.Run("migrator", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				m := log.NewMigrator(func() bool {
					return true
				}, logpb.Channel_DEV, logpb.Channel_CHANGEFEED)
				m.Infof(context.Background(), logStr)
			}
		})
	})
}

func GetChannelFromString(name string) (logpb.Channel, bool) {
	upperName := strings.ToUpper(strings.TrimSpace(name))
	if channelValue, ok := logpb.Channel_value[upperName]; ok {
		return logpb.Channel(channelValue), true
	}
	return 0, false
}

// cleanLogEntries Cleans non-deterministic fields, such as time and go routine
// id from log entries
func cleanLogEntries(logs []logpb.Entry) []logpb.Entry {
	newLogs := make([]logpb.Entry, 0, len(logs))
	for _, l := range logs {
		l.Time = 0
		l.Goroutine = 0
		newLogs = append(newLogs, l)
	}
	return newLogs
}
