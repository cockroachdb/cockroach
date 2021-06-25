package debug

import (
	"io"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
)

var csvHeader = []string{
	"ID",
	"Session ID",
	"TxnID",
	"Start Time",
	"Session Alloc. Bytes",
	"SQL Anon.",
	"IsDistributed",
	"Phase",
	"Progress",
}

// QueriesWriter writes debug information about the active
// queries for each session in the SessionRegistry to CSV
// format, using the provided io.Writer.
//
// For more about which information is written, see csvHeader
// above.
type QueriesWriter struct {
	sessions []serverpb.Session
	writer   io.Writer
}

// NewQueriesWriter instantiates and returns a new QueriesWriter, using
// the provided sessions slice and io.Writer.
func NewQueriesWriter(s []serverpb.Session, w io.Writer) *QueriesWriter {
	return &QueriesWriter{
		sessions: s,
		writer:   w,
	}
}

// Write writes all active queries for the sessions provided to
// this QueriesWriter.
func (qw *QueriesWriter) Write() error {
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
				q.SqlAnon,
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
