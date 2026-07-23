// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package valueside contains low-level primitives used to encode/decode SQL
// values into/from KV Values (see roachpb.Value).
//
// Low-level here means that these primitives do not operate with table or index
// descriptors.
//
// There are two separate schemes for encoding values:
//
//   - version 1 (legacy): the original encoding, which supported at most one SQL
//     value (column) per roachpb.Value. It is still used for old table
//     descriptors that went through many upgrades, and for some system tables.
//     Primitives related to this version contain the name `Legacy`.
//
//   - version 2 (column families): the current encoding which supports multiple
//     SQL values (columns) per roachpb.Value.
//
// See also: docs/tech-notes/encoding.md.
package valueside
