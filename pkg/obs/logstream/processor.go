// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logstream

import "context"

// Processor defines how a component can process streams of structured log events as they're
// passed to log.Structured. Processor enables engineers to use streams of log events as the
// primary input to a larger system, usually focused on observability.
//
// See RegisterProcessor for more details on how to create & register a processor with this package.
type Processor interface {
	// Process processes a single log stream event.
	Process(context.Context, any) error
}
