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

func haveInterceptors() bool {
	_, interceptors := loadInterceptors()
	return interceptors != nil
}

func TestInterceptEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()

	require.False(t, haveInterceptors())
	sink := &interceptSink{}
	require.False(t, sink.active())
}

func TestManyInterceptors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	require.False(t, haveInterceptors())
	require.NoError(t,
		ctxgroup.GroupWorkers(context.Background(), 100, func(ctx context.Context, worker int) error {
			devNull := func(_ []byte) {}
			defer RemoveInterceptor(AddInterceptor(devNull))
			for i := 0; i < 10; i++ {
				Infof(ctx, "hello worker %d: %d", worker, i)
			}
			return nil
		}))
	require.False(t, haveInterceptors())
}

type captureInterceptor struct {
	syncutil.Mutex
	re       *regexp.Regexp
	messages [][]byte
	expected []*regexp.Regexp
}

func (c *captureInterceptor) capture(message []byte) {
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
	for _, c := range interceptors {
		c.Lock()
		defer c.Unlock()
		c.expected = append(c.expected, regexp.MustCompile(fmt.Sprintf("%s:%d.*%s", file, line, msg)))
	}
}

func TestIntercept(t *testing.T) {
	defer leaktest.AfterTest(t)()

	first := &captureInterceptor{re: regexp.MustCompile("hello world")}
	second := &captureInterceptor{re: regexp.MustCompile("bonjour le monde|привет мир")}
	empty := &captureInterceptor{re: regexp.MustCompile("no such luck")}

	defer RemoveInterceptor(AddInterceptor(first.capture))
	defer RemoveInterceptor(AddInterceptor(second.capture))
	defer RemoveInterceptor(AddInterceptor(empty.capture))

	infoWithCapture("hello world and bonjour le monde", first, second)
	infoWithCapture("привет мир", second)

	first.verifyCaptures(t)
	second.verifyCaptures(t)
	empty.verifyCaptures(t)
}
