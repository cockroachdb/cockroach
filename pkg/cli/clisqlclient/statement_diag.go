// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlclient

import (
	"database/sql/driver"
	"io"
	"os"
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
func StmtDiagListBundles(conn Conn) ([]StmtDiagBundleInfo, error) {
	result, err := stmtDiagListBundlesInternal(conn)
	if err != nil {
		return nil, errors.Wrap(
			err, "failed to retrieve statement diagnostics bundles",
		)
	}
	return result, nil
}

func stmtDiagListBundlesInternal(conn Conn) ([]StmtDiagBundleInfo, error) {
	rows, err := conn.Query(
		`SELECT id, statement_fingerprint, collected_at
		 FROM system.statement_diagnostics
		 WHERE error IS NULL
		 ORDER BY collected_at DESC`,
		nil, /* args */
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
		info := StmtDiagBundleInfo{
			ID:          vals[0].(int64),
			Statement:   vals[1].(string),
			CollectedAt: vals[2].(time.Time),
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
	Statement   string
	RequestedAt time.Time
}

// StmtDiagListOutstandingRequests retrieves outstanding statement diagnostics
// activation requests.
func StmtDiagListOutstandingRequests(conn Conn) ([]StmtDiagActivationRequest, error) {
	result, err := stmtDiagListOutstandingRequestsInternal(conn)
	if err != nil {
		return nil, errors.Wrap(
			err, "failed to retrieve outstanding statement diagnostics activation requests",
		)
	}
	return result, nil
}

func stmtDiagListOutstandingRequestsInternal(conn Conn) ([]StmtDiagActivationRequest, error) {
	rows, err := conn.Query(
		`SELECT id, statement_fingerprint, requested_at
		 FROM system.statement_diagnostics_requests
		 WHERE NOT completed
		 ORDER BY requested_at DESC`,
		nil, /* args */
	)
	if err != nil {
		return nil, err
	}
	var result []StmtDiagActivationRequest
	vals := make([]driver.Value, 3)
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		info := StmtDiagActivationRequest{
			ID:          vals[0].(int64),
			Statement:   vals[1].(string),
			RequestedAt: vals[2].(time.Time),
		}
		result = append(result, info)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return result, nil
}

// StmtDiagDownloadBundle downloads the bundle with the given ID to a file.
func StmtDiagDownloadBundle(conn Conn, id int64, filename string) error {
	if err := stmtDiagDownloadBundleInternal(conn, id, filename); err != nil {
		return errors.Wrapf(
			err, "failed to download statement diagnostics bundle %d to '%s'", id, filename,
		)
	}
	return nil
}

func stmtDiagDownloadBundleInternal(conn Conn, id int64, filename string) error {
	// Retrieve the chunk IDs; these are stored in an INT ARRAY column.
	rows, err := conn.Query(
		"SELECT unnest(bundle_chunks) FROM system.statement_diagnostics WHERE id = $1",
		[]driver.Value{id},
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
		data, err := conn.QueryRow(
			"SELECT data FROM system.statement_bundle_chunks WHERE id = $1",
			[]driver.Value{chunkID},
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
func StmtDiagDeleteBundle(conn Conn, id int64) error {
	_, err := conn.QueryRow(
		"SELECT 1 FROM system.statement_diagnostics WHERE id = $1",
		[]driver.Value{id},
	)
	if err != nil {
		if err == io.EOF {
			return errors.Newf("no statement diagnostics bundle with ID %d", id)
		}
		return err
	}
	return conn.ExecTxn(func(conn TxBoundConn) error {
		// Delete the request metadata.
		if err := conn.Exec(
			"DELETE FROM system.statement_diagnostics_requests WHERE statement_diagnostics_id = $1",
			[]driver.Value{id},
		); err != nil {
			return err
		}
		// Delete the bundle chunks.
		if err := conn.Exec(
			`DELETE FROM system.statement_bundle_chunks
			  WHERE id IN (
				  SELECT unnest(bundle_chunks) FROM system.statement_diagnostics WHERE id = $1
				)`,
			[]driver.Value{id},
		); err != nil {
			return err
		}
		// Finally, delete the diagnostics entry.
		return conn.Exec(
			"DELETE FROM system.statement_diagnostics WHERE id = $1",
			[]driver.Value{id},
		)
	})
}

// StmtDiagDeleteAllBundles deletes all statement diagnostics bundles.
func StmtDiagDeleteAllBundles(conn Conn) error {
	return conn.ExecTxn(func(conn TxBoundConn) error {
		// Delete the request metadata.
		if err := conn.Exec(
			"DELETE FROM system.statement_diagnostics_requests WHERE completed",
			nil,
		); err != nil {
			return err
		}
		// Delete all bundle chunks.
		if err := conn.Exec(
			`DELETE FROM system.statement_bundle_chunks WHERE true`,
			nil,
		); err != nil {
			return err
		}
		// Finally, delete the diagnostics entry.
		return conn.Exec(
			"DELETE FROM system.statement_diagnostics WHERE true",
			nil,
		)
	})
}

// StmtDiagCancelOutstandingRequest deletes an outstanding statement diagnostics
// activation request.
func StmtDiagCancelOutstandingRequest(conn Conn, id int64) error {
	_, err := conn.QueryRow(
		"DELETE FROM system.statement_diagnostics_requests WHERE id = $1 RETURNING id",
		[]driver.Value{id},
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
func StmtDiagCancelAllOutstandingRequests(conn Conn) error {
	return conn.Exec(
		"DELETE FROM system.statement_diagnostics_requests WHERE NOT completed",
		nil,
	)
}
