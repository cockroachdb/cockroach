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
	"strconv"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/caller"
	otlog "github.com/opentracing/opentracing-go/log"
)

// msgBuf extends bytes.Buffer and implements otlog.Encoder.
type msgBuf struct {
	bytes.Buffer
	tagBuf [8]*logTag
}

var _ otlog.Encoder = &msgBuf{}

func (b *msgBuf) writeKey(key string, hasValue bool) {
	b.WriteString(key)
	// For tags that have a value and are longer than a character, we output
	// "tag=value". For one character tags we don't use a separator (e.g. "n1").
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
	b.WriteString(strconv.FormatBool(value))
}

func (b *msgBuf) EmitInt(key string, value int) {
	b.writeKey(key, true /* hasValue */)
	b.WriteString(strconv.Itoa(value))
}

func (b *msgBuf) EmitInt32(key string, value int32) {
	b.writeKey(key, true /* hasValue */)
	b.WriteString(strconv.Itoa(int(value)))
}

func (b *msgBuf) EmitInt64(key string, value int64) {
	b.writeKey(key, true /* hasValue */)
	b.WriteString(strconv.FormatInt(value, 10))
}

func (b *msgBuf) EmitUint32(key string, value uint32) {
	b.writeKey(key, true /* hasValue */)
	b.WriteString(strconv.FormatUint(uint64(value), 10))
}

func (b *msgBuf) EmitUint64(key string, value uint64) {
	b.writeKey(key, true /* hasValue */)
	b.WriteString(strconv.FormatUint(value, 10))
}

func (b *msgBuf) EmitFloat32(key string, value float32) {
	b.writeKey(key, true /* hasValue */)
	b.WriteString(strconv.FormatFloat(float64(value), 'g', -1, 32))
}

func (b *msgBuf) EmitFloat64(key string, value float64) {
	b.writeKey(key, true /* hasValue */)
	b.WriteString(strconv.FormatFloat(value, 'g', -1, 64))
}

func (b *msgBuf) EmitObject(key string, value interface{}) {
	hasValue := (value != nil)
	b.writeKey(key, hasValue)
	if hasValue {
		if s, ok := value.(fmt.Stringer); ok {
			b.WriteString(s.String())
		} else {
			fmt.Fprint(b, value)
		}
	}
}

func (b *msgBuf) EmitLazyLogger(value otlog.LazyLogger) {
	panic("not implemented")
}

// formatTags appends the tags to a bytes.Buffer. If there are no tags,
// returns false.
func formatTags(ctx context.Context, buf *msgBuf) bool {
	tags := contextLogTags(ctx, buf.tagBuf[:0])
	if len(tags) > 0 {
		buf.WriteByte('[')
		for i, t := range tags {
			if i > 0 {
				buf.WriteByte(',')
			}
			t.Field.Marshal(buf)
		}
		buf.WriteString("] ")
		return true
	}
	return false
}

// MakeMessage creates a structured log entry.
func MakeMessage(ctx context.Context, format string, args []interface{}) string {
	var buf msgBuf
	formatTags(ctx, &buf)
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
	msg := MakeMessage(ctx, format, args)
	// MakeMessage already added the tags when forming msg, we don't want
	// eventInternal to prepend them again.
	eventInternal(ctx, (s >= Severity_ERROR), false /*withTags*/, "%s:%d %s", file, line, msg)
	logging.outputLogEntry(s, file, line, msg)
}
