// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package clisqlcfg defines configuration settings and mechanisms for
// instances of the SQL shell.
//
// This package is intended to be used as follows:
//
// 1. instantiate a configuration with `NewDefaultConfig()`.
//
// 2. load customizations from e.g. command-line flags, env vars, etc.
//
// 3. validate the configuration and open the input/output streams via
//    `(*Context).Open()`. Defer a call to the returned cleanup function.
//
// 4. open a client connection via `(*Context).MakeConn()`.
//    Note: this must occur after the call to `Open()`, as the configuration
//    may not be ready before that point.
//
// 5. call `(*Context).Run()`.
//
package clisqlcfg
