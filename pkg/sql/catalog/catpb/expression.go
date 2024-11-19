// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catpb

// Expression is a SQL expression encoded as a string.
// This type exists for use as a cast type in protobufs to annotate which
// fields hold SQL expressions.
//
// TODO(ajwerner): adopt this in descpb.
type Expression string
