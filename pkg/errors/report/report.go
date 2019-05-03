// Copyright 2019 The Cockroach Authors.
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

package report

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/errors/domains"
	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/errors/withstack"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	raven "github.com/getsentry/raven-go"
)

// ReportError reports the given error to Sentry.
// The caller is responsible for checking whether
// telemetry is enabled.
func ReportError(ctx context.Context, err error) {
	if err == nil {
		// No error: do nothing.
		return
	}

	var stacks []*withstack.ReportableStackTrace
	var details []errbase.SafeDetailPayload
	// Peel the error.
	for c := err; c != nil; c = errbase.UnwrapOnce(c) {
		st := withstack.GetReportableStackTrace(c)
		stacks = append(stacks, st)

		sd := errbase.GetSafeDetails(c)
		details = append(details, sd)
	}

	// A report can contain at most one "message", at most one
	// "exception", but then it can contain arbitrarily many "extra"
	// fields.
	//
	// So we populate the packet as follow:
	// - the "exception" will contain the first detail with
	//   a populated encoded exception field.
	// - the "message" will contain the concatenation of all
	//   error types, and the first safe detail string,
	//   with links to the extra fields.
	// - the "extra" will contain all the encoded stack traces
	//   or safe detail arrays.

	var firstError *string
	var exc *withstack.ReportableStackTrace
	extras := make(map[string]interface{})
	var msgBuf bytes.Buffer
	var typesBuf bytes.Buffer

	extraNum := 1
	sep := ""
	for i := len(details) - 1; i >= 0; i-- {
		msgBuf.WriteString(sep)
		sep = "\n"

		// Collect the type name.
		tn := details[i].ErrorTypeName
		fmt.Fprintln(&typesBuf, tn)

		// Compose the message for this layer. The message consists of:
		// - optionally, a file/line reference, if a stack trace was available.
		// - the error/wrapper type name, with file prefix removed.
		// - optionally, the first line of the first detail string, if one is available.
		// - optionally, references to stack trace / details.
		if stacks[i] != nil && len(stacks[i].Frames) > 0 {
			f := stacks[i].Frames[len(stacks[i].Frames)-1]
			fn := f.Filename
			if j := strings.LastIndexByte(fn, '/'); j >= 0 {
				fn = fn[j+1:]
			}
			fmt.Fprintf(&msgBuf, "%s:%d: ", fn, f.Lineno)
		}

		msgBuf.WriteString(simpleErrType(tn))

		var genExtra bool

		// Is there a stack trace?
		if st := stacks[i]; st != nil {
			// Yes: generate the extra and list it on the line.
			stKey := fmt.Sprintf("%d: stacktrace", extraNum)
			extras[stKey] = log.PrintStackTrace(st)
			fmt.Fprintf(&msgBuf, " (%d)", extraNum)
			extraNum++

			if exc == nil {
				// Keep the stack trace to generate an exception object below.
				exc = st
			}
		} else {
			// No: are there details? If so, print them.
			// Note: we only print the details if no stack trace
			// was found that that level. This is because
			// stack trace annotations also produce the stack
			// trace as safe detail string.
			genExtra = len(details[i].SafeDetails) > 1
			if len(details[i].SafeDetails) > 0 {
				d := details[i].SafeDetails[0]
				if d != "" {
					genExtra = true
				}
				if j := strings.IndexByte(d, '\n'); j >= 0 {
					d = d[:j]
				}
				if d != "" {
					msgBuf.WriteString(": ")
					msgBuf.WriteString(d)
					if firstError == nil {
						// Keep the string for later.
						firstError = &d
					}
				}
			}
		}

		// Are we generating another extra for the safe detail strings?
		if genExtra {
			stKey := fmt.Sprintf("%d: details", extraNum)
			var extraStr bytes.Buffer
			for _, d := range details[i].SafeDetails {
				fmt.Fprintln(&extraStr, d)
			}
			extras[stKey] = extraStr.String()
			fmt.Fprintf(&msgBuf, " (%d)", extraNum)
			extraNum++
		}
	}

	// Determine a head message for the report.
	headMsg := "<unknown error>"
	if firstError != nil {
		headMsg = *firstError
	}
	// Prepend the "main" source line information if available/found.
	if file, line, fn, ok := withstack.GetOneLineSource(err); ok {
		headMsg = fmt.Sprintf("%s:%d: %s: %s", file, line, fn, headMsg)
	}

	extras["error types"] = typesBuf.String()

	// Make the message part more informational.
	msgBuf.WriteString("\n(check the extra data payloads)")

	var reportDetails log.ReportableObject
	if exc != nil {
		module := domains.GetDomain(err)
		reportDetails = &raven.Exception{
			Value:      headMsg,
			Type:       "<reported error>",
			Module:     string(module),
			Stacktrace: exc,
		}
	}

	// Finally, send the report.
	reportMsg := log.NewReportMessage(msgBuf.String())
	if reportDetails != nil {
		log.SendReport(ctx, headMsg, log.ReportTypeError, extras, reportMsg, reportDetails)
	} else {
		log.SendReport(ctx, headMsg, log.ReportTypeError, extras, reportMsg)
	}
}

func simpleErrType(typ errbase.TypeName) string {
	tn := typ.FamilyName()
	// Strip the colon-suffix.
	if i := strings.IndexByte(tn, ':'); i >= 0 {
		tn = tn[:i]
	}
	// Strip the path prefix.
	if i := strings.LastIndexByte(tn, '/'); i >= 0 {
		tn = tn[i+1:]
	}
	return tn
}
