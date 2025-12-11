// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"errors"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// inspectCheckRowCount defines an inspectCheck that counts rows in addition to
// its primary validation.
type inspectCheckRowCount interface {
	inspectCheck

	// Rows returns the number of rows processed by the check. This is only
	// guaranteed to be accurate when the check is done with no errors.
	Rows() map[descpb.IndexID]uint64
}

// rowCountCheck verifies a table's row count matches the expected count.
type rowCountCheck struct {
	rowCountCheckApplicability

	expected uint64

	asOf hlc.Timestamp

	state checkState

	checks []inspectCheck

	rowCounts map[descpb.IndexID]uint64
}

// rowCountCheckApplicability is a lightweight version that only implements applicability logic.
type rowCountCheckApplicability struct {
	tableID descpb.ID
}

var _ inspectCheckApplicability = (*indexConsistencyCheckApplicability)(nil)

// AppliesTo implements the inspectCheckApplicability interface.
func (c *rowCountCheckApplicability) AppliesTo(
	codec keys.SQLCodec, span roachpb.Span,
) (bool, error) {
	return spanContainsTable(c.tableID, codec, span)
}

var _ inspectCheck = (*rowCountCheck)(nil)
var _ inspectCheckApplicability = (*rowCountCheckApplicability)(nil)

var _ inspectMetaCheck = (*rowCountCheck)(nil)
var _ inspectPostCheck = (*rowCountCheck)(nil)

// Started implements the inspectCheck interface.
func (c *rowCountCheck) Started() bool {
	return c.state != checkNotStarted
}

// Start implements the inspectCheck interface.
func (c *rowCountCheck) Start(
	ctx context.Context, cfg *execinfra.ServerConfig, span roachpb.Span, workerIndex int,
) error {
	if err := assertCheckApplies(c, cfg.Codec, span); err != nil {
		return err
	}

	c.rowCounts = make(map[descpb.IndexID]uint64)

	c.state = checkRunning

	return nil
}

// Next implements the inspectCheck interface.
//
// It collates the row counts from other checks. Issues are returned on
// different checks reporting inconsistent row counts on the same index.
func (c *rowCountCheck) Next(
	ctx context.Context, cfg *execinfra.ServerConfig,
) (*inspectIssue, error) {
	for _, check := range c.checks {
		if rowCountCheck, ok := check.(inspectCheckRowCount); ok {
			for indexID, count := range rowCountCheck.Rows() {
				if existingCount, ok := c.rowCounts[indexID]; ok {
					if existingCount != count {
						return &inspectIssue{
							ErrorType: InternalError,
							AOST:      c.asOf.GoTime(),
							// Details:    details,
						}, nil
					}
				} else {
					c.rowCounts[indexID] = count
				}
			}
		}
	}

	c.state = checkDone
	return nil, nil
}

// Done implements the inspectCheck interface.
func (c *rowCountCheck) Done(context.Context) bool {
	return c.state == checkDone
}

// Close implements the inspectCheck interface.
func (c *rowCountCheck) Close(context.Context) error {
	return nil
}

// RegisterChecks implements the inspectMetaCheck interface.
func (c *rowCountCheck) RegisterChecks(checks inspectChecks) error {
	for _, check := range checks {
		if _, ok := check.(inspectCheckRowCount); ok {
			c.checks = checks
			return nil
		}
	}

	return errors.New("rowCountCheck requires an inspectCheckRowCount to be registered")
}

// MetaCheck implements the inspectMetaCheck interface.
func (c *rowCountCheck) MetaCheck(
	ctx context.Context, msg *jobspb.InspectProcessorProgress, logger *inspectLoggerBundle,
) error {
	for indexID, count := range c.rowCounts {
		msg.SpanRowCounts = append(msg.SpanRowCounts, &jobspb.InspectRowCount{
			IndexID:  indexID,
			AsOf:     c.asOf,
			RowCount: count,
		})
	}
	return nil
}

// Issues implements the inspectPostCheck interface.
func (c *rowCountCheck) Issues(
	ctx context.Context, progress *jobspb.InspectProgress,
) ([]*inspectIssue, error) {
	var issues []*inspectIssue

	if len(progress.IndexRowCounts) == 0 {
		issue := &inspectIssue{
			ErrorType: RowCountMismatch,
			// AOST:       rowCount.AsOf.GoTime(), // FIXME
			DatabaseID: 0,   // FIXME
			Details:    nil, // FIXME
		}
		issues = append(issues, issue)
	}
	for _, rowCount := range progress.IndexRowCounts {
		if rowCount.RowCount != c.expected {
			issue := &inspectIssue{
				ErrorType:  RowCountMismatch,
				AOST:       rowCount.AsOf.GoTime(),
				DatabaseID: 0,   // FIXME
				Details:    nil, // FIXME
			}
			issues = append(issues, issue)
		}
	}

	return issues, nil
}
