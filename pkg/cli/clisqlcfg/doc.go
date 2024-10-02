// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package clisqlcfg defines configuration settings and mechanisms for
// instances of the SQL shell.
//
// This package is intended to be used as follows:
//
// 1. instantiate a configuration with `NewDefaultConfig()`.
//
// 2. load customizations from e.g. command-line flags, env vars, etc.
//
//  3. validate the configuration and open the input/output streams via
//     `(*Context).Open()`. Defer a call to the returned cleanup function.
//
//  4. open a client connection via `(*Context).MakeConn()`.
//     Note: this must occur after the call to `Open()`, as the configuration
//     may not be ready before that point.
//
// 5. call `(*Context).Run()`.
package clisqlcfg
