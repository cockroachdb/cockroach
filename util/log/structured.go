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
	otlog "github.com/opentracing/opentracing-go/log"
)

// msgBuf extends bytes.Buffer and implements otlog.Encoder.
type msgBuf struct {
	bytes.Buffer
}

var _ otlog.Encoder = &msgBuf{}

func (b *msgBuf) writeKey(key string, hasValue bool) {
	b.WriteString(key)
	if hasValue && len(key) > 1 {
		b.WriteByte('=')
	}
}

func (b *msgBuf) EmitString(key, value string) {
	b.writeKey(key, value != "")
	b.WriteString(value)
}

func (b *msgBuf) EmitBool(key string, value bool) {
	b.writeKey(key, true /* hasValue */)
	fmt.Fprint(b, value)
}

func (b *msgBuf) EmitInt(key string, value int) {
	b.writeKey(key, true /* hasValue */)
	fmt.Fprint(b, value)
}

func (b *msgBuf) EmitInt32(key string, value int32) {
	b.writeKey(key, true /* hasValue */)
	fmt.Fprint(b, value)
}

func (b *msgBuf) EmitInt64(key string, value int64) {
	b.writeKey(key, true /* hasValue */)
	fmt.Fprint(b, value)
}

func (b *msgBuf) EmitUint32(key string, value uint32) {
	b.writeKey(key, true /* hasValue */)
	fmt.Fprint(b, value)
}

func (b *msgBuf) EmitUint64(key string, value uint64) {
	b.writeKey(key, true /* hasValue */)
	fmt.Fprint(b, value)
}

func (b *msgBuf) EmitFloat32(key string, value float32) {
	b.writeKey(key, true /* hasValue */)
	fmt.Fprint(b, value)
}

func (b *msgBuf) EmitFloat64(key string, value float64) {
	b.writeKey(key, true /* hasValue */)
	fmt.Fprint(b, value)
}

func (b *msgBuf) EmitObject(key string, value interface{}) {
	hasValue := (value != nil)
	b.writeKey(key, hasValue)
	if hasValue {
		fmt.Fprint(b, value)
	}
}

func (b *msgBuf) EmitLazyLogger(value otlog.LazyLogger) {
	panic("not implemented")
}

// formatTags appends the tags to a bytes.Buffer. If there are no tags,
// returns false.
func formatTags(buf *msgBuf, ctx context.Context) bool {
	tags := contextLogTags(ctx)
	if len(tags) > 0 {
		buf.WriteString("[")
		for i, t := range tags {
			if i > 0 {
				buf.WriteString(",")
			}
			t.Field.Marshal(buf)
		}
		buf.WriteString("] ")
		return true
	}
	return false
}

// makeMessage creates a structured log entry.
func makeMessage(ctx context.Context, format string, args []interface{}) string {
	var buf msgBuf
	formatTags(&buf, ctx)
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
	// makeMessage already added the tags when forming msg, we don't want
	// eventInternal to prepend them again.
	eventInternal(ctx, (s >= Severity_ERROR), false /*withTags*/, "%s:%d %s", file, line, msg)
	logging.outputLogEntry(s, file, line, msg)
}
