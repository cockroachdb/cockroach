// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package clisqlshell contains the code that powers CockroachDB's
// interactive SQL shell.
//
// Note that the common code that is shared with other CLI commands
// that are not interactive SQL shells but establish a SQL connection
// to a server should be placed in package clisqlclient instead.
package clisqlshell
