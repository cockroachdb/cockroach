// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !linux,!freebsd

package cli

import "github.com/cockroachdb/cockroach/pkg/util/stop"

func initBacktrace(logDir string, options ...stop.Option) *stop.Stopper {
	return stop.NewStopper(options...)
}
