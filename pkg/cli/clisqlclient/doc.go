// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package clisqlclient implements the connection code between a SQL
// client and server. This package contains code common to all CLI
// commands that establish SQL connections, including but not
// exclusively the SQL interactive shell. It also supports commands
// like 'cockroach node ls'.
package clisqlclient
