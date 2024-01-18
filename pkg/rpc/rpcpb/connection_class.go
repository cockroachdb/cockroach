// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpcpb

import (
	"strings"

	"github.com/gogo/protobuf/proto"
)

// connectionClassName contains all the ConnectionClass enum values in lower
// case. This is done for backward compatibility.
var connectionClassName = func() map[int32]string {
	m := make(map[int32]string, len(ConnectionClass_name))
	for c, name := range ConnectionClass_name {
		m[c] = strings.ToLower(name)
	}
	return m
}()

// String implements the fmt.Stringer interface.
func (c ConnectionClass) String() string {
	return proto.EnumName(connectionClassName, int32(c))
}

// SafeValue implements the redact.SafeValue interface.
func (ConnectionClass) SafeValue() {}
