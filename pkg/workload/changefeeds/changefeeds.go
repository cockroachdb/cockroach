// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeeds

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	urls []string,
	reg *histogram.Registry,
	ql *workload.QueryLoad,
) error {
	cfg := workload.MultiConnPoolCfg{
		Method:              gen.ConnFlags().Method,
		MaxTotalConnections: 2,
	}

	mcp, err := workload.NewMultiConnPool(ctx, cfg, urls...)
	if err != nil {
		return err
	}
	conn, err := mcp.Get().Acquire(ctx)
	if err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("USE %q", dbName)); err != nil {
		return err
	}

	setAppName := fmt.Sprintf("SET application_name='%s_changefeed'", gen.Meta().Name)
	if _, err := conn.Exec(ctx, setAppName); err != nil {
		return err
	}

	cfLatency := reg.GetHandle().Get("changefeed")

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

	tableNames := strings.Builder{}
	for i, table := range gen.Tables() {
		if i == 0 {
			fmt.Fprintf(&tableNames, "%q", table.Name)
		} else {
			fmt.Fprintf(&tableNames, ", %q", table.Name)
		}
	}

	stmt := fmt.Sprintf("EXPERIMENTAL CHANGEFEED FOR %s WITH updated, no_initial_scan, schema_change_policy=nobackfill,cursor=$1",
		tableNames.String())
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
		var err error
		rows, err = conn.Query(cfCtx, stmt, cursorStr)
		return maybeMarkDone(err)
	}

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
				Updated string `json:"updated"`
			}
			var v updatedJSON
			if maybeMarkDone(json.Unmarshal(values[2].([]byte), &v)) {
				return doneErr
			}
			updated, err := hlc.ParseHLC(v.Updated)
			if maybeMarkDone(err) {
				return doneErr
			}
			cfLatency.Record(timeutil.Since(updated.GoTime()))
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
