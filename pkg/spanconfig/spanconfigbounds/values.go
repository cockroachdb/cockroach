// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigbounds

import (
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

// Value is a value from a SpanConfig that knows how to format itself.
type Value interface {
	fmt.Stringer
	redact.SafeFormatter
}

type int32Value int32

func (i int32Value) String() string {
	return fmt.Sprintf("%d", int32(i))
}
func (i int32Value) SafeFormat(s interfaces.SafePrinter, verb rune) {
	s.Printf("%d", int32(i))
}

type int64Value int64

func (i int64Value) String() string {
	return fmt.Sprintf("%d", int64(i))
}
func (i int64Value) SafeFormat(s interfaces.SafePrinter, verb rune) {
	s.Printf("%d", int64(i))
}

type constraintsConjunctionValue []roachpb.ConstraintsConjunction

func (c constraintsConjunctionValue) String() string {
	return fmt.Sprint([]roachpb.ConstraintsConjunction(c))
}
func (c constraintsConjunctionValue) SafeFormat(s interfaces.SafePrinter, verb rune) {
	s.Printf("%v", []roachpb.ConstraintsConjunction(c))
}

type leasePreferencesValue []roachpb.LeasePreference

func (l leasePreferencesValue) String() string {
	return fmt.Sprint([]roachpb.LeasePreference(l))
}
func (l leasePreferencesValue) SafeFormat(s interfaces.SafePrinter, verb rune) {
	s.Printf("%v", []roachpb.LeasePreference(l))
}

type boolValue bool

func (b boolValue) String() string {
	return strconv.FormatBool(bool(b))
}
func (b boolValue) SafeFormat(s interfaces.SafePrinter, verb rune) {
	s.Print(bool(b))
}
