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

import "fmt"

// Unsafe turns any value that would otherwise be considered safe,
// into an unsafe value.
func Unsafe(a interface{}) interface{} {
	return unsafeWrap{a}
}

type unsafeWrap struct {
	a interface{}
}

func (w unsafeWrap) Format(s fmt.State, verb rune) {
	reproducePrintf(s, s, verb, w.a)
}

// Safe turns any value into an object that is considered as safe by
// the formatter.
//
// This is provided as an “escape hatch” for cases where the other
// interfaces and conventions fail. Increased usage of this mechanism
// should be taken as a signal that a new abstraction is missing.
// The implementation is also slow.
func Safe(a interface{}) SafeValue {
	return safeWrapper{a}
}

type safeWrapper struct {
	a interface{}
}

var _ SafeValue = safeWrapper{}
var _ fmt.Formatter = safeWrapper{}
var _ SafeMessager = safeWrapper{}

// SafeValue implements the SafeValue interface.
func (w safeWrapper) SafeValue() {}

// Format implements the fmt.Formatter interface.
func (w safeWrapper) Format(s fmt.State, verb rune) {
	reproducePrintf(s, s, verb, w.a)
}

// SafeMessage implements SafeMessager.
func (w safeWrapper) SafeMessage() string {
	return fmt.Sprintf("%v", w.a)
}
