// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlclient

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

// StmtDiagBundleInfo contains information about a statement diagnostics bundle
// that was collected.
type StmtDiagBundleInfo struct {
	ID int64
	// Statement is the SQL statement fingerprint.
	Statement   string
	CollectedAt time.Time
}

// StmtDiagListBundles retrieves information about all available statement
// diagnostics bundles.
func StmtDiagListBundles(ctx context.Context, conn Conn) ([]StmtDiagBundleInfo, error) {
	result, err := stmtDiagListBundlesInternal(ctx, conn)
	if err != nil {
		return nil, errors.Wrap(
			err, "failed to retrieve statement diagnostics bundles",
		)
	}
	return result, nil
}

func stmtDiagListBundlesInternal(ctx context.Context, conn Conn) ([]StmtDiagBundleInfo, error) {
	rows, err := conn.Query(ctx,
		`SELECT id, statement_fingerprint, collected_at
		 FROM system.statement_diagnostics
		 WHERE error IS NULL
		 ORDER BY collected_at DESC`,
	)
	if err != nil {
		return nil, err
	}
	var result []StmtDiagBundleInfo
	vals := make([]driver.Value, 3)
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		i, ok := vals[0].(int64)
		if !ok {
			// We're arriving in this function via the interactive shell,
			// with result type inference disabled.
			// The value has been read as a string.
			i, err = strconv.ParseInt(vals[0].(string), 10, 64)
			if err != nil {
				return nil, err
			}
		}
		d, ok := vals[2].(time.Time)
		if !ok {
			// We're arriving in this function via the interactive shell,
			// with result type inference disabled.
			// The value has been read as a string.
			ts := vals[2].(string)
			ts = strings.TrimSuffix(ts, "+00")
			d, err = time.Parse("2006-01-02 15:04:05.999999", ts)
			if err != nil {
				return nil, err
			}
		}
		info := StmtDiagBundleInfo{
			ID:          i,
			Statement:   vals[1].(string),
			CollectedAt: d,
		}
		result = append(result, info)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return result, nil
}

// StmtDiagActivationRequest contains information about a statement diagnostics
// activation request.
type StmtDiagActivationRequest struct {
	ID int64
	// Statement is the SQL statement fingerprint.
	Statement string
	// If empty then any plan will do.
	PlanGist string
	// If true and PlanGist is not empty, then any plan not matching the gist
	// will do.
	AntiPlanGist bool
	RequestedAt  time.Time
	// Zero value indicates that there is no sampling probability set on the
	// request.
	SamplingProbability float64
	// Zero value indicates that there is no minimum latency set on the request.
	MinExecutionLatency time.Duration
	// Zero value indicates that the request never expires.
	ExpiresAt time.Time
	// If true, then the redacted bundle is requested.
	Redacted bool
}

// StmtDiagListOutstandingRequests retrieves outstanding statement diagnostics
// activation requests.
func StmtDiagListOutstandingRequests(
	ctx context.Context, conn Conn,
) ([]StmtDiagActivationRequest, error) {
	result, err := stmtDiagListOutstandingRequestsInternal(ctx, conn)
	if err != nil {
		return nil, errors.Wrap(
			err, "failed to retrieve outstanding statement diagnostics activation requests",
		)
	}
	return result, nil
}

func isAtLeast24dot2ClusterVersion(ctx context.Context, conn Conn) (bool, error) {
	// Check whether the upgrade to add the redacted column to the
	// statement_diagnostics_requests system table has already been run.
	row, err := conn.QueryRow(ctx, `
 SELECT
   count(*)
 FROM
   [SHOW COLUMNS FROM system.statement_diagnostics_requests]
 WHERE
   column_name = 'redacted';`)
	if err != nil {
		return false, err
	}
	c, ok := row[0].(int64)
	if !ok {
		return false, nil
	}
	return c == 1, nil
}

func stmtDiagListOutstandingRequestsInternal(
	ctx context.Context, conn Conn,
) ([]StmtDiagActivationRequest, error) {
	var extraColumns string
	atLeast24dot2, err := isAtLeast24dot2ClusterVersion(ctx, conn)
	if err != nil {
		return nil, err
	}
	if atLeast24dot2 {
		extraColumns = ", redacted"
	}

	// Converting an INTERVAL to a number of milliseconds within that interval
	// is a pain - we extract the number of seconds and multiply it by 1000,
	// then we extract the number of milliseconds and add that up to the
	// previous result; however, we have now double counted the seconds field,
	// so we have to remove that times 1000.
	getMilliseconds := `EXTRACT(epoch FROM min_execution_latency)::INT8 * 1000 +
                        EXTRACT(millisecond FROM min_execution_latency)::INT8 -
                        EXTRACT(second FROM min_execution_latency)::INT8 * 1000`
	rows, err := conn.Query(ctx,
		fmt.Sprintf("SELECT id, statement_fingerprint, requested_at, "+getMilliseconds+`,
                                   expires_at, sampling_probability, plan_gist, anti_plan_gist%s
			FROM system.statement_diagnostics_requests
			WHERE NOT completed
			ORDER BY requested_at DESC`, extraColumns),
	)
	if err != nil {
		return nil, err
	}
	var result []StmtDiagActivationRequest
	vals := make([]driver.Value, 9)
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		var minExecutionLatency time.Duration
		var expiresAt time.Time
		var samplingProbability float64
		var planGist string
		var antiPlanGist, redacted bool

		if ms, ok := vals[3].(int64); ok {
			minExecutionLatency = time.Millisecond * time.Duration(ms)
		}
		if e, ok := vals[4].(time.Time); ok {
			expiresAt = e
		}
		if sp, ok := vals[5].(float64); ok {
			samplingProbability = sp
		}
		if gist, ok := vals[6].(string); ok {
			planGist = gist
		}
		if antiGist, ok := vals[7].(bool); ok {
			antiPlanGist = antiGist
		}
		if atLeast24dot2 {
			if b, ok := vals[8].(bool); ok {
				redacted = b
			}
		}
		info := StmtDiagActivationRequest{
			ID:                  vals[0].(int64),
			Statement:           vals[1].(string),
			PlanGist:            planGist,
			AntiPlanGist:        antiPlanGist,
			RequestedAt:         vals[2].(time.Time),
			SamplingProbability: samplingProbability,
			MinExecutionLatency: minExecutionLatency,
			ExpiresAt:           expiresAt,
			Redacted:            redacted,
		}
		result = append(result, info)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return result, nil
}

// StmtDiagDownloadBundle downloads the bundle with the given ID to a file.
func StmtDiagDownloadBundle(ctx context.Context, conn Conn, id int64, filename string) error {
	if err := stmtDiagDownloadBundleInternal(ctx, conn, id, filename); err != nil {
		return errors.Wrapf(
			err, "failed to download statement diagnostics bundle %d to '%s'", id, filename,
		)
	}
	return nil
}

func stmtDiagDownloadBundleInternal(
	ctx context.Context, conn Conn, id int64, filename string,
) error {
	// Retrieve the chunk IDs; these are stored in an INT ARRAY column.
	rows, err := conn.Query(ctx,
		"SELECT unnest(bundle_chunks) FROM system.statement_diagnostics WHERE id = $1",
		id,
	)
	if err != nil {
		return err
	}
	var chunkIDs []int64
	vals := make([]driver.Value, 1)
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		chunkIDs = append(chunkIDs, vals[0].(int64))
	}
	if err := rows.Close(); err != nil {
		return err
	}

	if len(chunkIDs) == 0 {
		return errors.Newf("no statement diagnostics bundle with ID %d", id)
	}

	// Create the file and write out the chunks.
	out, err := os.Create(filename)
	if err != nil {
		return err
	}

	for _, chunkID := range chunkIDs {
		data, err := conn.QueryRow(ctx,
			"SELECT data FROM system.statement_bundle_chunks WHERE id = $1",
			chunkID,
		)
		if err != nil {
			_ = out.Close()
			return err
		}
		if _, err := out.Write(data[0].([]byte)); err != nil {
			_ = out.Close()
			return err
		}
	}

	return out.Close()
}

// StmtDiagDeleteBundle deletes a statement diagnostics bundle.
func StmtDiagDeleteBundle(ctx context.Context, conn Conn, id int64) error {
	_, err := conn.QueryRow(ctx,
		"SELECT 1 FROM system.statement_diagnostics WHERE id = $1",
		id,
	)
	if err != nil {
		if err == io.EOF {
			return errors.Newf("no statement diagnostics bundle with ID %d", id)
		}
		return err
	}
	return conn.ExecTxn(ctx, func(ctx context.Context, conn TxBoundConn) error {
		// Delete the request metadata.
		if err := conn.Exec(ctx,
			"DELETE FROM system.statement_diagnostics_requests WHERE statement_diagnostics_id = $1",
			id,
		); err != nil {
			return err
		}
		// Delete the bundle chunks.
		if err := conn.Exec(ctx,
			`DELETE FROM system.statement_bundle_chunks
			  WHERE id IN (
				  SELECT unnest(bundle_chunks) FROM system.statement_diagnostics WHERE id = $1
				)`,
			id,
		); err != nil {
			return err
		}
		// Finally, delete the diagnostics entry.
		return conn.Exec(ctx,
			"DELETE FROM system.statement_diagnostics WHERE id = $1",
			id,
		)
	})
}

// StmtDiagDeleteAllBundles deletes all statement diagnostics bundles.
func StmtDiagDeleteAllBundles(ctx context.Context, conn Conn) error {
	return conn.ExecTxn(ctx, func(ctx context.Context, conn TxBoundConn) error {
		// Delete the request metadata.
		if err := conn.Exec(ctx,
			"DELETE FROM system.statement_diagnostics_requests WHERE completed",
		); err != nil {
			return err
		}
		// Delete all bundle chunks.
		if err := conn.Exec(ctx,
			`DELETE FROM system.statement_bundle_chunks WHERE true`,
		); err != nil {
			return err
		}
		// Finally, delete the diagnostics entry.
		return conn.Exec(ctx,
			"DELETE FROM system.statement_diagnostics WHERE true",
		)
	})
}

// StmtDiagCancelOutstandingRequest deletes an outstanding statement diagnostics
// activation request.
func StmtDiagCancelOutstandingRequest(ctx context.Context, conn Conn, id int64) error {
	_, err := conn.QueryRow(ctx,
		"DELETE FROM system.statement_diagnostics_requests WHERE id = $1 RETURNING id",
		id,
	)
	if err != nil {
		if err == io.EOF {
			return errors.Newf("no outstanding activation request with ID %d", id)
		}
		return err
	}
	return nil
}

// StmtDiagCancelAllOutstandingRequests deletes all outstanding statement
// diagnostics activation requests.
func StmtDiagCancelAllOutstandingRequests(ctx context.Context, conn Conn) error {
	return conn.Exec(ctx,
		"DELETE FROM system.statement_diagnostics_requests WHERE NOT completed",
	)
}
