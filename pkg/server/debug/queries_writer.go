// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package debug

import (
	"io"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
)

var csvHeader = []string{
	// ID of the active query.
	"ID",
	// ID of the session that the active query belongs to.
	"Session ID",
	// Transaction ID of the active query.
	"TxnID",
	// Start time of the active query.
	"Start Time",
	// Session Alloc. Bytes measures the # of bytes allocated to the session that
	// the query belongs to, as tracked by the session's ByteMonitor
	"Session Alloc. Bytes",
	// SQL Statement Fingerprint is the SQL statement fingerprint.
	"SQL Statement Fingerprint",
	// IsDistributed indicates whether or not the query is distributed.
	"IsDistributed",
	// Phase of the query ("ActiveQuery_PREPARING" or "ActiveQuery_EXECUTING").
	"Phase",
	// Progress is an estimate of the fraction of this query that has been
	// processed, as a percentage.
	"Progress",
}

// ActiveQueriesWriter writes debug information about the active
// queries for each session in the SessionRegistry to CSV
// format, using the provided io.Writer.
//
// For more about which information is written, see csvHeader
// above.
type ActiveQueriesWriter struct {
	sessions []serverpb.Session
	writer   io.Writer
}

// NewActiveQueriesWriter instantiates and returns a new ActiveQueriesWriter,
// using the provided sessions slice and io.Writer.
func NewActiveQueriesWriter(s []serverpb.Session, w io.Writer) *ActiveQueriesWriter {
	return &ActiveQueriesWriter{
		sessions: s,
		writer:   w,
	}
}

// Write writes all active queries for the sessions provided to this
// ActiveQueriesWriter.
func (qw *ActiveQueriesWriter) Write() error {
	csvWriter := csv.NewWriter(qw.writer)
	defer csvWriter.Flush()
	err := csvWriter.Write(csvHeader)
	if err != nil {
		return err
	}
	for _, session := range qw.sessions {
		for _, q := range session.ActiveQueries {
			row := []string{
				q.ID,
				uint128.FromBytes(session.ID).String(),
				q.TxnID.String(),
				q.Start.String(),
				strconv.FormatInt(session.AllocBytes, 10),
				q.SqlNoConstants,
				strconv.FormatBool(q.IsDistributed),
				q.Phase.String(),
				strconv.FormatFloat(float64(q.Progress), 'f', 3, 64),
			}
			err := csvWriter.Write(row)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
