// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package logstore contains interfaces and implementation for a Raft log
// storage. It is built directly above the Raft layer, enhanced with
// CRDB-specific handling and optimizations.
//
// TODO(pavelkalinnikov): keep commenting.
// TODO(pavelkalinnikov): eventually move up to kv/repl/logstore.
package logstore
