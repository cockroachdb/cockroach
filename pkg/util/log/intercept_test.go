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

type devnull struct{}

var _ Interceptor = (*devnull)(nil)

func (d *devnull) Intercept(message []byte) {}

func addInterceptor(t *testing.T, interceptor Interceptor) func() {
	t.Helper()
	return InterceptWith(context.Background(), interceptor)
}

func TestManyInterceptors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer Scope(t).Close(t)

	require.NoError(t,
		ctxgroup.GroupWorkers(context.Background(), 100, func(ctx context.Context, worker int) error {
			defer addInterceptor(t, &devnull{})()
			for i := 0; i < 10; i++ {
				Infof(ctx, "hello worker %d: %d", worker, i)
			}
			return nil
		}))
}

type captureInterceptor struct {
	t testing.TB
	syncutil.Mutex
	re       *regexp.Regexp
	messages [][]byte
	expected []*regexp.Regexp
}

var _ Interceptor = (*captureInterceptor)(nil)

func (c *captureInterceptor) Intercept(message []byte) {
	c.Lock()
	defer c.Unlock()
	c.t.Logf("captured message: %s", message)
	if c.re.Match(message) {
		c.t.Logf("message matches re: %s", c.re)
		cp := append([]byte(nil), message...)
		c.messages = append(c.messages, cp)
	}
}

func (c *captureInterceptor) verifyCaptures(t *testing.T) {
	c.Lock()
	defer c.Unlock()

	seen := 0
	for _, m := range c.messages {
		c.t.Logf("inspecting message: %s", m)
		for _, re := range c.expected {
			c.t.Logf("with re: %s", re)
			if re.Match(m) {
				seen++
			}
		}
	}
	require.Equal(t, len(c.expected), seen,
		"expected to match %d, but matched %d", len(c.expected), seen)
}

func infoWithCapture(msg string, interceptors ...*captureInterceptor) {
	InfofDepth(context.Background(), 1, "%s", msg)
	file, line, _ := caller.Lookup(1)
	expectedRe := regexp.MustCompile(fmt.Sprintf(`.*"file":"%s","line":%d.*%s`, file, line, msg))

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
	defer Scope(t).Close(t)

	first := &captureInterceptor{t: t, re: regexp.MustCompile("hello world")}
	second := &captureInterceptor{t: t, re: regexp.MustCompile("bonjour le monde|привет мир")}
	empty := &captureInterceptor{t: t, re: regexp.MustCompile("no such luck")}

	defer addInterceptor(t, first)()
	defer addInterceptor(t, second)()
	defer addInterceptor(t, empty)()

	infoWithCapture("hello world and bonjour le monde", first, second)
	infoWithCapture("привет мир", second)

	first.verifyCaptures(t)
	second.verifyCaptures(t)
	empty.verifyCaptures(t)
}
