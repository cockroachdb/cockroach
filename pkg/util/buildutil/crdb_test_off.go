// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !crdb_test || crdb_test_off
// +build !crdb_test crdb_test_off

package buildutil

import "reflect"

// CrdbTestBuild is a flag that is set to true if the binary was compiled
// with the 'crdb_test' build tag (which is the case for all test targets). This
// flag can be used to enable expensive checks, test randomizations, or other
// metamorphic-style perturbations that will not affect test results but will
// exercise different parts of the code.
const CrdbTestBuild = false

// TestingInt64 is an empty struct that can be used as a `gogoproto.casttype` in
// proto messages. It uses no space. When the crdb_test build tag is set, this
// type is instead represented by a TestingInt64.
type TestingInt64 struct{}

func (m *TestingInt64) Reset() {}

// String implements (a part of) protoutil.Message.
func (m *TestingInt64) String() string { return "0" }

// ProtoMessage implements (a part of) protoutil.Message.
func (m *TestingInt64) ProtoMessage() {}

// MarshalTo implements (a part of) protoutil.Message.
func (m *TestingInt64) MarshalTo(buf []byte) (int, error) { return 0, nil }

// Unmarshal implements (a part of) protoutil.Message.
func (m *TestingInt64) Unmarshal(buf []byte) error { return nil }

// MarshalToSizedBuffer implements (a part of) protoutil.Message.
func (m *TestingInt64) MarshalToSizedBuffer(buf []byte) (int, error) { return 0, nil }

// Size implements (a part of) protoutil.Message.
func (m *TestingInt64) Size() int { return 0 }

// Equal implements (gogoproto.equal).
func (m *TestingInt64) Equal(n interface{}) bool { return reflect.DeepEqual(m, n) }

// Set is a no-op.
func (m *TestingInt64) Set(int64) {}

// Get returns zero.
func (m TestingInt64) Get() int64 { return 0 }
