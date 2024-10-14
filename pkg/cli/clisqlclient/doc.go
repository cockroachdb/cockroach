// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package clisqlclient implements the connection code between a SQL
// client and server. This package contains code common to all CLI
// commands that establish SQL connections, including but not
// exclusively the SQL interactive shell. It also supports commands
// like 'cockroach node ls'.
package clisqlclient
