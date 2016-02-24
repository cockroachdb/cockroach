// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package log

import (
	"bytes"
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/util/caller"
)

// makeMessage creates a structured log entry.
func makeMessage(ctx context.Context, format string, args []interface{}) string {
	var buf bytes.Buffer
	if ctx != nil {
		sep := "["
		for _, field := range allFields {
			if v := ctx.Value(field); v != nil {
				_, _ = buf.WriteString(sep)
				field.format(&buf, v)
				sep = ","
			}
		}
		if sep != "[" {
			_, _ = buf.WriteString("] ")
		}
	}

	if len(format) == 0 {
		fmt.Fprint(&buf, args...)
	} else {
		fmt.Fprintf(&buf, format, args...)
	}
	return buf.String()
}

// addStructured creates a structured log entry to be written to the
// specified facility of the logger.
func addStructured(ctx context.Context, s Severity, depth int, format string, args []interface{}) {
	file, line, _ := caller.Lookup(depth + 1)
	logging.outputLogEntry(s, file, line, makeMessage(ctx, format, args))
}
