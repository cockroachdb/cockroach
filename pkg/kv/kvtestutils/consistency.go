// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package kvtestutils

import (
	"context"
	gosql "database/sql"
	"fmt"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

type querier interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*gosql.Rows, error)
}

// CheckConsistency runs a consistency check on all ranges in the given span,
// primarily to verify that MVCC stats are accurate. Any failures are returned
// as a list of errors. RANGE_CONSISTENT_STATS_ESTIMATED is considered a
// success, since stats estimates are fine (if unfortunate).
func CheckConsistency(ctx context.Context, db querier, span roachpb.Span) []error {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
		SELECT range_id, start_key_pretty, status, detail
		FROM crdb_internal.check_consistency(false, b'\x%x', b'\x%x')
		ORDER BY range_id ASC`,
		span.Key, span.EndKey,
	))
	if err != nil {
		return []error{err}
	}
	defer rows.Close()

	var failures []error
	for rows.Next() {
		var rangeID int
		var key, status, detail string
		if err := rows.Scan(&rangeID, &key, &status, &detail); err != nil {
			return []error{err}
		}
		// NB: There's a known issue that can result in a 10-byte discrepancy in
		// SysBytes. See:
		// https://github.com/cockroachdb/cockroach/issues/93896
		//
		// This isn't critical, so we ignore such discrepancies.
		if status == kvpb.CheckConsistencyResponse_RANGE_CONSISTENT_STATS_INCORRECT.String() {
			m := regexp.MustCompile(`.*\ndelta \(stats-computed\): \{(.*)\}`).FindStringSubmatch(detail)
			if len(m) > 1 {
				delta := m[1]
				// Strip out LastUpdateNanos and all zero-valued fields.
				delta = regexp.MustCompile(`LastUpdateNanos:\d+`).ReplaceAllString(delta, "")
				delta = regexp.MustCompile(`\S+:0\b`).ReplaceAllString(delta, "")
				if regexp.MustCompile(`^\s*SysBytes:-?10\s*$`).MatchString(delta) {
					continue
				}
			}
		}
		switch status {
		case kvpb.CheckConsistencyResponse_RANGE_INDETERMINATE.String():
			// Can't do anything, so let it slide.
		case kvpb.CheckConsistencyResponse_RANGE_CONSISTENT.String():
			// Good.
		case kvpb.CheckConsistencyResponse_RANGE_CONSISTENT_STATS_ESTIMATED.String():
			// Ok.
		default:
			failures = append(failures, errors.Errorf("range %d (%s) %s:\n%s", rangeID, key, status, detail))
		}
	}
	return failures
}
