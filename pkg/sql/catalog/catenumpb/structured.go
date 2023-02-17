// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catenumpb

// IndexDescriptorEncodingType is a custom type to represent different encoding types
// for secondary indexes.
type IndexDescriptorEncodingType uint32

const (
	// SecondaryIndexEncoding corresponds to the standard way of encoding secondary indexes
	// as described in docs/tech-notes/encoding.md. We allow the 0 value of this type
	// to have a value so that existing descriptors are encoding using this encoding.
	SecondaryIndexEncoding IndexDescriptorEncodingType = iota
	// PrimaryIndexEncoding corresponds to when a secondary index is encoded using the
	// primary index encoding as described in docs/tech-notes/encoding.md.
	PrimaryIndexEncoding
)

// Remove unused warning.
var _ = SecondaryIndexEncoding
