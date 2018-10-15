// Copyright 2018 The Cockroach Authors.
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

package pgdate

import "time"

// Context provides configuration options for date/time parsing.
type Context struct {
	exposeErr bool
	mode      ParseMode
	now       func() time.Time
}

// ISOContext is a default parsing context for ISO-style dates,
// using the local UTC time.
func ISOContext() Context {
	return isoContext
}

var isoContext = Context{
	mode: ParseModeISO,
	now:  func() time.Time { return time.Now().UTC() },
}

func WithExposedErrors(ctx Context) Context {
	ctx.exposeErr = true
	return ctx
}

func WithFixedNow(ctx Context, now time.Time) Context {
	ctx.now = func() time.Time { return now }
	return ctx
}

func WithNow(ctx Context, now func() time.Time) Context {
	ctx.now = now
	return ctx
}

func WithParseMode(ctx Context, mode ParseMode) Context {
	ctx.mode = mode
	return ctx
}
