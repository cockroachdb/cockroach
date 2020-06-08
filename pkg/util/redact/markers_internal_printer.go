// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package redact

import (
	"bytes"
	"fmt"
)

// printer implements SafePrinter.
// This is the machinery for the Print() functions offered
// by this package.
type printer struct {
	escapeState
	buf bytes.Buffer
}

var _ fmt.State = (*printer)(nil)
var _ SafeWriter = (*printer)(nil)

// Print is part of the SafeWriter interface.
func (b *printer) Print(args ...interface{}) {
	_, _ = Fprint(&b.buf, args...)
}

// Printf is part of the SafeWriter interface.
func (b *printer) Printf(format string, args ...interface{}) {
	_, _ = Fprintf(&b.buf, format, args...)
}

// SafeString is part of the SafeWriter interface.
func (b *printer) SafeString(s SafeString) {
	w := escapeWriter{w: &b.buf, enclose: false}
	_, _ = w.Write([]byte(s))
}

// SafeRune is part of the SafeWriter interface.
func (b *printer) SafeRune(s SafeRune) {
	if s == startRedactable || s == endRedactable {
		s = escapeMark
	}
	_, _ = b.buf.WriteRune(rune(s))
}

// UnsafeString is part of the SafeWriter interface.
func (b *printer) UnsafeString(s string) {
	w := escapeWriter{w: &b.buf, enclose: true, strip: true}
	_, _ = w.Write([]byte(s))
}

// UnsafeRune is part of the SafeWriter interface.
func (b *printer) UnsafeRune(s rune) {
	_, _ = b.buf.WriteRune(startRedactable)
	b.SafeRune(SafeRune(s))
	_, _ = b.buf.WriteRune(endRedactable)
}

// UnsafeByte is part of the SafeWriter interface.
func (b *printer) UnsafeByte(s byte) {
	_, _ = b.buf.WriteRune(startRedactable)
	_ = b.buf.WriteByte(s)
	_, _ = b.buf.WriteRune(endRedactable)
}

// UnsafeBytes is part of the SafeWriter interface.
func (b *printer) UnsafeBytes(s []byte) {
	w := escapeWriter{w: &b.buf, enclose: true, strip: true}
	_, _ = w.Write(s)
}
