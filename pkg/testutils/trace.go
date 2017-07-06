// Copyright 2017 The Cockroach Authors.
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

package testutils

import (
	"regexp"
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// MakeRecordCtx returns a context with an embedded trace span which is finished
// and its contents returned when the returned closure (which is idempotent but
// not thread safe) is called. This closure also cancels the context.
func MakeRecordCtx() (context.Context, func() string) {
	tr := tracing.NewTracer()
	sp := tr.StartSpan("test span", tracing.Recordable)
	tracing.StartRecording(sp, tracing.SingleNodeRecording)
	ctx, cancel := context.WithCancel(context.Background())
	ctx = opentracing.ContextWithSpan(ctx, sp)

	var once sync.Once
	var dump string

	return ctx, func() string {
		once.Do(func() {
			dump = tracing.FormatRecordedSpans(tracing.GetRecording(sp))
			tracing.StopRecording(sp)
			sp.Finish()
			tr.Close()
			cancel()
		})
		return dump
	}
}

// MatchInOrder matches interprets the given slice of strings as a slice of
// regular expressions and checks that they match, in order and without overlap,
// against the given string. For example, if s=abcdefg and res=ab,cd,fg no error
// is returned, whereas res=abc,cde would return a descriptive error about
// failing to match cde.
func MatchInOrder(s string, res ...string) error {
	sPos := 0
	for i := range res {
		reStr := "(?ms)" + res[i]
		re, err := regexp.Compile(reStr)
		if err != nil {
			return errors.Errorf("regexp %d (%q) does not compile: %s", i, reStr, err)
		}
		loc := re.FindStringIndex(s[sPos:])
		if loc == nil {
			// Not found.
			return errors.Errorf(
				"unable to find regexp %d (%q) in remaining string:\n\n%s\n\nafter having matched:\n\n%s",
				i, reStr, s[sPos:], s[:sPos],
			)
		}
		sPos += loc[1]
	}
	return nil
}
