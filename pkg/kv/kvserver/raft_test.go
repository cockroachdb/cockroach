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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// TestWrapNumbersAsSafe tests the wrapNumbersAsSafe through ReportablesToSafeError.
func TestWrapNumbersAsSafe(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	const format = "some reportables" +
		" %v %v %v %v %v" +
		" %v %v %v %v %v" +
		" %v %v" +
		" %v %v"
	err := errors.Newf(format, reportables...)

	// rm is the redaction mark, what remains after redaction when
	// the redaction markers are removed.
	rm := string(redact.RedactableBytes(redact.RedactedMarker()).StripMarkers())

	expected := `some reportables` +
		` 4294967295 255 65535 4294967295 18446744073709551615` +
		` 2147483647 127 32767 2147483647 9223372036854775807` +
		` 3.4028235e+38 1.7976931348623157e+308 ` +
		rm + ` ` + rm

	if redacted := redact.Sprint(err).Redact().StripMarkers(); expected != redacted {
		t.Errorf("expected short error string to be:\n%s\n\nbut was:\n%s", expected, redacted)
	}
}
