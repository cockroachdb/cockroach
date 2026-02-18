// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlwriter

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

var internalPgErrors = func() *regexp.Regexp {
	codePrefixes := []string{
		// Section: Class 57 - Operator Intervention
		"57",
		// Section: Class 58 - System Error
		"58",
		// Section: Class 25 - Invalid Transaction State
		"25",
		// Section: Class 2D - Invalid Transaction Termination
		"2D",
		// Section: Class 40 - Transaction Rollback
		"40",
		// Section: Class XX - Internal Error
		"XX",
		// Section: Class 58C - System errors related to CockroachDB node problems.
		"58C",
	}
	return regexp.MustCompile(fmt.Sprintf("^(%s)", strings.Join(codePrefixes, "|")))
}()

// IsLwwLoser returns true if the error is a ConditionFailedError with an
// OriginTimestampOlderThan set, indicating the row lost a last-write-wins
// comparison against a newer local value or tombstone.
func IsLwwLoser(err error) bool {
	if condErr := (*kvpb.ConditionFailedError)(nil); errors.As(err, &condErr) {
		return condErr.OriginTimestampOlderThan.IsSet()
	}
	return false
}

// CanDLQError returns nil if the error should send a row to the DLQ. It
// returns a non-nil error if the error should not be DLQ'd, for example
// because it indicates an internal or retryable problem. The idea is it is
// better to fail the processor so the job backs off until the system is
// healthy.
func CanDLQError(err error) error {
	// If the error is not from the SQL layer, then we don't want to DLQ it.
	if !pgerror.HasCandidateCode(err) {
		return errors.Wrap(err, "can only DLQ errors with pg codes")
	}
	code := pgerror.GetPGCode(err)
	if internalPgErrors.MatchString(code.String()) {
		return errors.Wrap(err, "unable to DLQ pgcode that indicates an internal or retryable error")
	}
	return nil
}
