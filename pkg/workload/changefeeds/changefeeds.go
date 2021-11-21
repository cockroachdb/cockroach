// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package changefeeds

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4"
)

// AddChangefeedToQueryLoad augments the passed QueryLoad to contain an extra
// worker to run a changefeed over the tables of the generator.
func AddChangefeedToQueryLoad(
	ctx context.Context,
	gen workload.Generator,
	urls []string,
	reg *histogram.Registry,
	ql *workload.QueryLoad,
) error {
	cfg := workload.MultiConnPoolCfg{
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
	if _, err := conn.Exec(ctx, fmt.Sprintf("USE %q", ql.SQLDatabase)); err != nil {
		return err
	}
	cfLatency := reg.GetHandle().Get("changefeed")

	var tableNames []string
	for _, tabl := range gen.Tables() {
		tableNames = append(tableNames, fmt.Sprintf("%q", tabl.Name))
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

	stmt := fmt.Sprintf("EXPERIMENTAL CHANGEFEED FOR %s WITH updated, no_initial_scan, schema_change_policy=nobackfill",
		strings.Join(tableNames, ", "))
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
		rows, err = conn.Query(cfCtx, stmt)
		return maybeMarkDone(err)
	}
	ql.WorkerFns = append(ql.WorkerFns, func(ctx context.Context) error {
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
			updated, err := tree.ParseHLC(v.Updated)
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
	ql.Close = func(ctx context.Context) {
		cancel()
		_, _ = closeConn.Exec(ctx, "CANCEL SESSION $1", sessionID)
		_ = closeConn.Conn().Close(ctx)
		if conn != nil {
			_ = conn.Conn().Close(ctx)
		}
		prevClose(ctx)
		mcp.Close()
	}
	return nil
}
