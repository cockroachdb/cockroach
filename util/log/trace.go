// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package log

import (
	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"
)

// Trace looks for an opentracing.Trace in the context and logs the given
// message to it on success.
func Trace(ctx context.Context, msg string) {
	sp := opentracing.SpanFromContext(ctx)
	if sp != nil {
		sp.LogEvent(msg)
	}
}
