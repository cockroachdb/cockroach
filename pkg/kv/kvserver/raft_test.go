// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
)

// TestWrapNumbersAsSafe tests the wrapNumbersAsSafe through ReportablesToSafeError.
func TestWrapNumbersAsSafe(t *testing.T) {
	defer leaktest.AfterTest(t)()

	reportables := []interface{}{
		uint(math.MaxUint32),
		uint8(math.MaxUint8),
		uint16(math.MaxUint16),
		uint32(math.MaxUint32),
		uint64(math.MaxUint64),

		int(math.MaxInt32),
		int8(math.MaxInt8),
		int16(math.MaxInt16),
		int32(math.MaxInt32),
		int64(math.MaxInt64),

		float32(math.MaxFloat32),
		float64(math.MaxFloat64),

		"unsafe-string",
		"123",
	}

	wrapNumbersAsSafe(reportables...)
	const format = "some reportables"
	err := errors.WithSafeDetails(leafErr{}, format, reportables...)

	const expected = `<kvserver.leafErr>
wrapper: <*safedetails.withSafeDetails>
(more details:)
some reportables
-- arg 1: 4294967295
-- arg 2: 255
-- arg 3: 65535
-- arg 4: 4294967295
-- arg 5: 18446744073709551615
-- arg 6: 2147483647
-- arg 7: 127
-- arg 8: 32767
-- arg 9: 2147483647
-- arg 10: 9223372036854775807
-- arg 11: 3.4028235e+38
-- arg 12: 1.7976931348623157e+308
-- arg 13: <string>
-- arg 14: <string>`

	if redacted := errors.Redact(err); expected != redacted {
		t.Fatalf("expected error to be:\n%s\n\nbut was:\n%s", expected, redacted)
	}

}

type leafErr struct{}

func (leafErr) Error() string { return "error" }
