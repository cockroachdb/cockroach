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
	tags := contextLogTags(ctx)
	if len(tags) > 0 {
		buf.WriteString("[")
		for i, t := range tags {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(t.name)
			if value := t.value(); value != nil {
				buf.WriteString("=")
				fmt.Fprint(&buf, value)
			}
		}
		buf.WriteString("] ")
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
	if ctx == nil {
		panic("nil context")
	}
	file, line, _ := caller.Lookup(depth + 1)
	msg := makeMessage(ctx, format, args)
	Trace(ctx, msg)
	logging.outputLogEntry(s, file, line, msg)
}
