// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package clisqlshell contains the code that powers CockroachDB's
// interactive SQL shell.
//
// Note that the common code that is shared with other CLI commands
// that are not interactive SQL shells but establish a SQL connection
// to a server should be placed in package clisqlclient instead.
package clisqlshell
