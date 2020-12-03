// Copyright 2015 The Cockroach Authors.
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
	"strings"
)

// FormatWithContextTags formats the string and prepends the context
// tags.
//
// Redaction markers are *not* inserted. The resulting
// string is generally unsafe for reporting.
func FormatWithContextTags(ctx context.Context, format string, args ...interface{}) string {
	var buf strings.Builder
	formatTags(ctx, true /* brackets */, &buf)
	formatArgs(&buf, format, args...)
	return buf.String()
}
