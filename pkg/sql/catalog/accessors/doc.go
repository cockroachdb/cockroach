// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package accessors provides implementations of SchemaAccessors as well as the
// backing (poorly named) TableCollection for interacting with mutations to
// structured descriptors.
//
// TODO(ajwerner): Provide better abstraction around this package.
package accessors
