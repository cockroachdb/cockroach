// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

func TestInterceptEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()

	require.False(t, haveActiveInterceptors())
	sink := &interceptSink{}
	require.False(t, sink.active())
}

type devnull struct{}

func (d *devnull) Intercept(message []byte) {

}

var _ LogMessageInterceptor = (*devnull)(nil)

func TestManyInterceptors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	require.False(t, haveActiveInterceptors())
	require.NoError(t,
		ctxgroup.GroupWorkers(context.Background(), 100, func(ctx context.Context, worker int) error {
			defer RemoveInterceptor(AddInterceptor(&devnull{}))
			for i := 0; i < 10; i++ {
				Infof(ctx, "hello worker %d: %d", worker, i)
			}
			return nil
		}))
	require.False(t, haveActiveInterceptors())
}

type captureInterceptor struct {
	syncutil.Mutex
	re       *regexp.Regexp
	messages [][]byte
	expected []*regexp.Regexp
}

func (c *captureInterceptor) Intercept(message []byte) {
	c.Lock()
	defer c.Unlock()
	if c.re.Match(message) {
		cp := append([]byte(nil), message...)
		c.messages = append(c.messages, cp)
	}
}

func (c *captureInterceptor) verifyCaptures(t *testing.T) {
	c.Lock()
	defer c.Unlock()

	seen := 0
	for _, m := range c.messages {
		for _, re := range c.expected {
			if re.Match(m) {
				seen++
			}
		}
	}
	require.Equal(t, len(c.expected), seen,
		"expected to match %d, but matched %d", len(c.expected), seen)
}

func infoWithCapture(msg string, interceptors ...*captureInterceptor) {
	InfofDepth(context.Background(), 1, msg)
	file, line, _ := caller.Lookup(1)
	expectedRe := regexp.MustCompile(fmt.Sprintf("%s:%d.*%s", file, line, msg))

	addExpectation := func(interceptor *captureInterceptor) {
		interceptor.Lock()
		defer interceptor.Unlock()
		interceptor.expected = append(interceptor.expected, expectedRe)
	}
	for _, c := range interceptors {
		addExpectation(c)
	}
}

func TestIntercept(t *testing.T) {
	defer leaktest.AfterTest(t)()

	first := &captureInterceptor{re: regexp.MustCompile("hello world")}
	second := &captureInterceptor{re: regexp.MustCompile("bonjour le monde|привет мир")}
	empty := &captureInterceptor{re: regexp.MustCompile("no such luck")}

	defer RemoveInterceptor(AddInterceptor(first))
	defer RemoveInterceptor(AddInterceptor(second))
	defer RemoveInterceptor(AddInterceptor(empty))

	infoWithCapture("hello world and bonjour le monde", first, second)
	infoWithCapture("привет мир", second)

	first.verifyCaptures(t)
	second.verifyCaptures(t)
	empty.verifyCaptures(t)
}
