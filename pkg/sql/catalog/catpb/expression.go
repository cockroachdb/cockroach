// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catpb

// Expression, Statement, and RoutineBody are all strings that encode SQL,
// however the distinction exists to denote how to parse each string.

// Expression is a SQL expression encoded as a string.
// This type exists for use as a cast type in protobufs to annotate which
// fields hold SQL expressions.
type Expression string

// Statement is a SQL statement encoded as a string.
// This type exists for use as a cast type in protobufs to annotate which
// fields hold full SQL statements.
type Statement string

// RoutineBody is a PL/pgSQL routine body encoded as a string.
// This type exists for use as a cast type in protobufs to annotate which
// fields hold PL/pgSQL function bodies.
type RoutineBody string
