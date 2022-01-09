// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package keyside contains low-level primitives used to encode/decode SQL
// values into/from KV Keys (see roachpb.Key).
//
// Low-level here means that these primitives do not operate with table or index
// descriptors.
//
// See also: docs/tech-notes/encoding.md.
package keyside
