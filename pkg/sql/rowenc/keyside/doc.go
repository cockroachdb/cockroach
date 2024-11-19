// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package keyside contains low-level primitives used to encode/decode SQL
// values into/from KV Keys (see roachpb.Key).
//
// Low-level here means that these primitives do not operate with table or index
// descriptors.
//
// See also: docs/tech-notes/encoding.md.
package keyside
