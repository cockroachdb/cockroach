// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeeds

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
)

// AddChangefeedToQueryLoad augments the passed QueryLoad to contain an extra
// worker to run a changefeed over the tables of the generator.
func AddChangefeedToQueryLoad(
	ctx context.Context,
	gen workload.ConnFlagser,
	dbName string,
	resolvedTarget time.Duration,
	urls []string,
	reg *histogram.Registry,
	ql *workload.QueryLoad,
) error {
	cfg := workload.MultiConnPoolCfg{
		Method:              gen.ConnFlags().Method,
		MaxTotalConnections: 2,
	}
	cfLatency := reg.GetHandle().Get("changefeed")
	// TODO(ssd): This metric makes no sense has a histogram. The idea that every
	// metric we might want to report is a histogram is baked in pretty deeply so
	// I'll address that in a different PR (maybe)
	var cfResolved *histogram.NamedHistogram
	if resolvedTarget > 0 {
		cfResolved = reg.GetHandle().Get("changefeed-resolved")
	}

	mcp, err := workload.NewMultiConnPool(ctx, cfg, urls...)
	if err != nil {
		return err
	}
	conn, err := mcp.Get().Acquire(ctx)
	if err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, "SET CLUSTER SETTING kv.rangefeed.enabled = true"); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("USE %q", dbName)); err != nil {
		return err
	}

	setAppName := fmt.Sprintf("SET application_name='%s_changefeed'", gen.Meta().Name)
	if _, err := conn.Exec(ctx, setAppName); err != nil {
		return err
	}

	var sessionID string
	if err := conn.QueryRow(ctx, "SHOW session_id").Scan(&sessionID); err != nil {
		return errors.Wrap(err, "getting session_id")
	}

	// Create a second connection to close the first connection by issuing a
	// cancel request.
	closeConn, err := mcp.Get().Acquire(ctx)
	if err != nil {
		return err
	}
	if _, err := closeConn.Exec(ctx, setAppName); err != nil {
		return err
	}

	var cursorStr string
	if err := conn.QueryRow(ctx, "SELECT cluster_logical_timestamp()").Scan(&cursorStr); err != nil {
		return err
	}

	log.Dev.Infof(ctx, "cursor: %s", cursorStr)

	tableNames := strings.Builder{}
	for i, table := range gen.Tables() {
		if i == 0 {
			fmt.Fprintf(&tableNames, "%q", table.Name)
		} else {
			fmt.Fprintf(&tableNames, ", %q", table.Name)
		}
	}

	opts := []string{
		"updated",
		"no_initial_scan",
		"schema_change_policy=nobackfill",
		"cursor=$1",
	}
	args := []any{
		cursorStr,
	}
	if resolvedTarget > 0 {
		opts = append(opts, []string{"resolved=$2", "min_checkpoint_frequency=$2"}...)
		args = append(args, resolvedTarget.String())
	}
	stmt := fmt.Sprintf(
		"CREATE CHANGEFEED FOR %s WITH %s",
		tableNames.String(), strings.Join(opts, ","),
	)
	cfCtx, cancel := context.WithCancel(ctx)

	var doneErr error
	maybeMarkDone := func(err error) (done bool) {
		if err == nil {
			return false
		}
		cancel()
		_ = conn.Conn().Close(ctx)
		doneErr, conn = err, nil
		return true
	}
	var rows pgx.Rows
	maybeSetupRows := func() (done bool) {
		if rows != nil {
			return false
		}
		log.Dev.Infof(ctx, "creating changefeed with stmt: %s with args %v", stmt, args)
		if epoch, parseErr := strconv.ParseInt(cursorStr, 10, 64); parseErr == nil {
			t := time.Unix(epoch, 0).UTC()
			log.Dev.Infof(ctx, "starting a changefeed after %s", time.Since(t))
		}
		var err error
		rows, err = conn.Query(cfCtx, stmt, args...)
		return maybeMarkDone(err)
	}

	var lastResolved hlc.Timestamp

	ql.ChangefeedFns = append(ql.ChangefeedFns, func(ctx context.Context) error {
		if doneErr != nil {
			return doneErr
		}
		if maybeSetupRows() {
			return doneErr
		}

		if rows.Next() {
			values, err := rows.Values()
			if maybeMarkDone(err) {
				return doneErr
			}
			type updatedJSON struct {
				Updated  string `json:"updated"`
				Resolved string `json:"resolved"`
			}
			var v updatedJSON
			if maybeMarkDone(json.Unmarshal(values[2].([]byte), &v)) {
				return doneErr
			}
			if v.Updated != "" {
				updated, err := hlc.ParseHLC(v.Updated)
				if maybeMarkDone(err) {
					return doneErr
				}
				cfLatency.Record(timeutil.Since(updated.GoTime()))
			} else if v.Resolved != "" {
				resolved, err := hlc.ParseHLC(v.Resolved)
				if maybeMarkDone(err) {
					return doneErr
				}
				if resolved.Less(lastResolved) {
					return errors.Errorf("resolved timestamp %s is less than last resolved timestamp %s", resolved, lastResolved)
				}
				lastResolved = resolved
			} else {
				return errors.Errorf("failed to parse CHANGEFEED event: %s", values[2])
			}
			// Resolved timestamps arrived infrequently. Always record the time since
			// our lastResolved so that we don't get long periods of 0 in the
			// histogram.
			if cfResolved != nil {
				cfResolved.Record(timeutil.Since(lastResolved.GoTime()))
			}
			return nil
		}
		if maybeMarkDone(rows.Err()) {
			return doneErr
		}
		maybeMarkDone(errors.New("changefeed ended"))
		return doneErr
	})

	prevClose := ql.Close
	ql.Close = func(ctx context.Context) error {
		cancel()
		_, _ = closeConn.Exec(ctx, "CANCEL SESSION $1", sessionID)
		if err := closeConn.Conn().Close(ctx); err != nil {
			return err
		}
		if conn != nil {
			if err := conn.Conn().Close(ctx); err != nil {
				return err
			}
		}
		if err := prevClose(ctx); err != nil {
			return err
		}
		return nil
	}
	return nil
}
