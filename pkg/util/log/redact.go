// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"context"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

// EditSensitiveData describes how the messages in log entries should
// be edited through the API.
type EditSensitiveData int

const (
	invalidEditMode EditSensitiveData = iota
	// WithMarkedSensitiveData is the "raw" log with sensitive data markers included.
	WithMarkedSensitiveData
	// WithFlattenedSensitiveData is the log with markers stripped.
	WithFlattenedSensitiveData
	// WithoutSensitiveData is the log with the sensitive data redacted.
	WithoutSensitiveData
)

// SelectEditMode returns an EditSensitiveData value that's suitable
// for use with NewDecoder depending on client-side desired
// "redact" and "keep redactable" flags.
// (See the documentation for the Logs and LogFile RPCs
// and that of the 'merge-logs' CLI command.)
func SelectEditMode(redact, keepRedactable bool) EditSensitiveData {
	editMode := WithMarkedSensitiveData
	if redact {
		editMode = WithoutSensitiveData
	}
	if !keepRedactable && !redact {
		editMode = WithFlattenedSensitiveData
	}
	return editMode
}

type redactEditor func(redactablePackage) redactablePackage

func getEditor(editMode EditSensitiveData) redactEditor {
	switch editMode {
	case WithMarkedSensitiveData:
		return func(r redactablePackage) redactablePackage {
			if !r.redactable {
				r.msg = []byte(redact.EscapeBytes(r.msg))
				r.redactable = true
			}
			return r
		}
	case WithFlattenedSensitiveData:
		return func(r redactablePackage) redactablePackage {
			if r.redactable {
				r.msg = redact.RedactableBytes(r.msg).StripMarkers()
				r.redactable = false
			}
			return r
		}
	case WithoutSensitiveData:
		return func(r redactablePackage) redactablePackage {
			if r.redactable {
				r.msg = []byte(redact.RedactableBytes(r.msg).Redact())
			} else {
				r.msg = redact.RedactedMarker()
				r.redactable = true
			}
			return r
		}
	case invalidEditMode:
		fallthrough
	default:
		panic(errors.AssertionFailedf("unrecognized mode: %v", editMode))
	}
}

// Safe constructs a SafeFormatter / SafeMessager.
// This is obsolete. Use redact.Safe directly.
// TODO(knz): Remove this.
var Safe = redact.Safe

func init() {
	// We consider booleans and numeric values to be always safe for
	// reporting. A log call can opt out by using redact.Unsafe() around
	// a value that would be otherwise considered safe.
	redact.RegisterSafeType(reflect.TypeOf(true)) // bool
	redact.RegisterSafeType(reflect.TypeOf(123))  // int
	redact.RegisterSafeType(reflect.TypeOf(int8(0)))
	redact.RegisterSafeType(reflect.TypeOf(int16(0)))
	redact.RegisterSafeType(reflect.TypeOf(int32(0)))
	redact.RegisterSafeType(reflect.TypeOf(int64(0)))
	redact.RegisterSafeType(reflect.TypeOf(uint8(0)))
	redact.RegisterSafeType(reflect.TypeOf(uint16(0)))
	redact.RegisterSafeType(reflect.TypeOf(uint32(0)))
	redact.RegisterSafeType(reflect.TypeOf(uint64(0)))
	redact.RegisterSafeType(reflect.TypeOf(float32(0)))
	redact.RegisterSafeType(reflect.TypeOf(float64(0)))
	redact.RegisterSafeType(reflect.TypeOf(complex64(0)))
	redact.RegisterSafeType(reflect.TypeOf(complex128(0)))
	// Signal names are also safe for reporting.
	redact.RegisterSafeType(reflect.TypeOf(os.Interrupt))
	// Times and durations too.
	redact.RegisterSafeType(reflect.TypeOf(time.Time{}))
	redact.RegisterSafeType(reflect.TypeOf(time.Duration(0)))
}

type redactablePackage struct {
	msg        []byte
	redactable bool
}

const redactableIndicator = "⋮"

var redactableIndicatorBytes = []byte(redactableIndicator)

func redactTags(ctx context.Context, buf *strings.Builder) {
	tags := logtags.FromContext(ctx)
	if tags == nil {
		return
	}
	comma := ""
	for _, t := range tags.Get() {
		buf.WriteString(comma)
		buf.WriteString(t.Key())
		if v := t.Value(); v != nil && v != "" {
			if len(t.Key()) > 1 {
				buf.WriteByte('=')
			}
			redact.Fprint(buf, v)
		}
		comma = ","
	}
}

// TestingSetRedactable sets the redactable flag for usage in a test.
// The caller is responsible for calling the cleanup function.  This
// is exported for use in tests only -- it causes the logging
// configuration to be at risk of leaking unsafe information due to
// asynchronous direct writes to fd 2 / os.Stderr.
//
// See the discussion on SetupRedactionAndStderrRedirects() for
// details.
func TestingSetRedactable(redactableLogs bool) (cleanup func()) {
	prev := mainLog.redactableLogs.Swap(redactableLogs)
	return func() {
		mainLog.redactableLogs.Set(prev)
	}
}
