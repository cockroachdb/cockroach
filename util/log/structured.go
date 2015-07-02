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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package log

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/caller"

	"golang.org/x/net/context"
)

// AddStructured creates a structured log entry to be written to the
// specified facility of the logger.
func AddStructured(ctx context.Context, s Severity, depth int, format string, args []interface{}) {
	file, line, _ := caller.Lookup(depth + 1)
	entry := &LogEntry{}
	setLogEntry(ctx, format, args, entry)
	logging.outputLogEntry(s, file, line, false, entry)
}

// getJSON returns a JSON representation of the specified argument.
// Returns nil if the type is simple and does not require a separate
// JSON representation.
func getJSON(arg interface{}) []byte {
	// Not much point in storying strings and byte slices twice, as
	// they're nearly always exactly specified in the format string.
	switch arg.(type) {
	case string, []byte, proto.Key, proto.EncodedKey:
		return nil
	}

	jsonBytes, err := json.Marshal(arg)
	if err != nil {
		return []byte(fmt.Sprintf("{\"error\": %q}", err.Error()))
	}
	return jsonBytes
}
func setLogEntry(ctx context.Context, format string, args []interface{}, entry *LogEntry) {
	entry.Format, entry.Args = parseFormatWithArgs(format, args)

	if ctx != nil {
		for i := Field(0); i < maxField; i++ {
			if v := ctx.Value(i); v != nil {
				switch vTyp := v.(type) {
				case proto.NodeID:
					entry.NodeID = &vTyp
				case proto.StoreID:
					entry.StoreID = &vTyp
				case proto.RaftID:
					entry.RaftID = &vTyp
				case proto.Method:
					entry.Method = &vTyp
				case proto.Key:
					entry.Key = vTyp
				}
			}
		}
	}
}

// parseFormatWithArgs parses the format string, matching each
// format specifier with an argument from the args array.
func parseFormatWithArgs(format string, args []interface{}) (string, []LogEntry_Arg) {
	// Process format string.
	var logArgs []LogEntry_Arg
	var buf []byte
	var idx int
	end := len(format)
	for i := 0; i < end; {
		lasti := i
		for i < end && format[i] != '%' {
			i++
		}
		if i > lasti {
			buf = append(buf, format[lasti:i]...)
		}

		if i >= end {
			break
		}

		start := i

		// Process one verb.
		i++

	F:
		for ; i < end; i++ {
			switch format[i] {
			case '#', '0', '+', '-', ' ':
			default:
				break F
			}
		}

		// TODO(spencer): should arg numbers dynamic precision be
		// supported? They're so rare, better to just panic here for now.
		if i < end && format[i] == '[' || format[i] == '*' {
			panic(fmt.Sprintf("arg numbers in format not supported by logger: %s", format))
		}

		// Read optional width.
		for ; i < end && format[i] >= '0' && format[i] <= '9'; i++ {
		}
		// Read optional precision.
		if i < end && format[i] == '.' {
			for i = i + 1; i < end && format[i] >= '0' && format[i] <= '9'; i++ {
			}
		}
		if i >= end {
			break
		}
		c, w := utf8.DecodeRuneInString(format[i:])
		i += w
		// Escape and add percent directly to format buf.
		if c == '%' {
			buf = append(buf, '%', '%')
			continue
		}
		buf = append(buf, "%s"...)
		// New format string always gets %s, though we use the actual
		// format to generate the string here for the log argument.
		if idx >= len(args) {
			fmt.Fprintf(os.Stderr, "ERROR: insufficient parameters specified for format string %s", format)
			return string(append(buf, format[i:]...)), logArgs
		}
		logArgs = append(logArgs, makeLogArg(format[start:i], args[idx]))
		idx++ // advance to next arg index
	}

	// Add arguments which were not processed via format specifiers.
	for ; idx < len(args); idx++ {
		logArgs = append(logArgs, makeLogArg("%v", args[idx]))
	}

	return string(buf), logArgs
}

func makeLogArg(format string, arg interface{}) LogEntry_Arg {
	var tstr string
	if t := reflect.TypeOf(arg); t != nil {
		tstr = t.String()
	}
	return LogEntry_Arg{
		Type: tstr,
		Str:  fmt.Sprintf(format, arg),
		Json: getJSON(arg),
	}
}
