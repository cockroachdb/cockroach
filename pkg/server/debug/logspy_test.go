// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package debug

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

func TestDebugLogSpyOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		vals    url.Values
		expOpts logSpyOptions
		expErr  string
	}{
		{
			// Example where everything is specified (and parsed).
			vals: map[string][]string{
				"NonexistentOptionIsIgnored": {"banana"},
				"Count":                      {"123"},
				"Duration":                   {"9s"},
				"Grep":                       {`^foo$`},
			},
			expOpts: logSpyOptions{
				Count:    123,
				Duration: durationAsString(9 * time.Second),
				Grep:     regexpAsString{re: regexp.MustCompile(`^foo$`)},
			},
		},
		{
			// Example where everything is specified (and parsed) and where grep is an integer.
			vals: map[string][]string{
				"NonexistentOptionIsIgnored": {"banana"},
				"Count":                      {"123"},
				"Duration":                   {"9s"},
				"Grep":                       {`123`},
			},
			expOpts: logSpyOptions{
				Count:    123,
				Duration: durationAsString(9 * time.Second),
				Grep:     regexpAsString{re: regexp.MustCompile(`123`), i: 123},
			},
		},
		{
			// When nothing is given, default to "infinite" count and a 5s duration.
			expOpts: logSpyOptions{
				Count:    logSpyDefaultCount,
				Duration: logSpyDefaultDuration,
			},
		},
		{
			// Can't stream out too much at once.
			vals: map[string][]string{
				"Count": {strconv.Itoa(2 * logSpyMaxCount)},
			},
			expErr: (`count .* is too large .limit is .*.`),
		},
		// Various parse errors.
		{
			vals: map[string][]string{
				"Count": {"bellpepper is not a number"},
			},
			expErr: `strconv.Atoi: parsing "bellpepper is not a number": invalid syntax`,
		},
		{
			vals: map[string][]string{
				"Duration": {"very long"},
			},
			expErr: `time: invalid duration very long`,
		},
		{
			vals: map[string][]string{
				"Grep": {"(unresolved parentheses = tension"},
			},
			expErr: regexp.QuoteMeta("error parsing regexp: missing closing ): `(unresolved parentheses = tension`"),
		},
	}

	for i, test := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			opts, err := logSpyOptionsFromValues(test.vals)
			if !testutils.IsError(err, test.expErr) {
				t.Fatalf("unexpected error: %s [expected %s]", err, test.expErr)
			}
			if isStr, shouldStr := fmt.Sprintf("%v", opts), fmt.Sprintf("%v", test.expOpts); isStr != shouldStr {
				t.Fatalf("wanted: %s\ngot: %s", shouldStr, isStr)
			}
		})
	}
}

func TestDebugLogSpyHandle(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Verify that a parse error doesn't execute anything.
	{
		spy := logSpy{
			setIntercept: func(ctx context.Context, f log.InterceptorFn) {
				t.Fatal("tried to intercept")
			},
		}

		r := httptest.NewRequest("GET", "/?duration=notaduration", nil)
		rec := httptest.NewRecorder()
		spy.handleDebugLogSpy(rec, r)
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("unexpected status: %d", rec.Code)
		}
		exp := "while parsing options: time: invalid duration notaduration\n"
		if body := rec.Body.String(); body != exp {
			t.Fatalf("expected: %q\ngot: %q", exp, body)
		}
	}

	// Verify that overlapping intercepts are prevented.
	{
		waiting := make(chan struct{})
		waitingAlias := waiting
		spy := logSpy{
			setIntercept: func(ctx context.Context, f log.InterceptorFn) {
				if f != nil && waitingAlias != nil {
					close(waitingAlias)
					waitingAlias = nil
					<-ctx.Done()
				}
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		{
			r := httptest.NewRequest("GET", "/?duration=1m", nil)
			r = r.WithContext(ctx)
			rec := httptest.NewRecorder()
			go spy.handleDebugLogSpy(rec, r)
		}

		<-waiting // goroutine is now armed, new requests should bounce

		request := func() (int, string) {
			rec := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "/", nil)
			ctx, c := context.WithCancel(context.Background())
			c() // intentionally to avoid doing any real work
			r = r.WithContext(ctx)
			spy.handleDebugLogSpy(rec, r)
			resp := rec.Result()
			defer resp.Body.Close()
			bodyBytes, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				panic(err)
			}
			return resp.StatusCode, string(bodyBytes)
		}

		for {
			status, body := request()

			if cancel != nil {
				cancel() // cancel open request, should finish soon
				cancel = nil
			}

			if cancel != nil || status == http.StatusInternalServerError {
				exp := "a log interception is already in progress\n"
				if status != http.StatusInternalServerError || body != exp {
					t.Fatalf("expected: %d %q\ngot: %d %q",
						http.StatusInternalServerError, exp,
						status, body)
				}
				continue
			} else {
				if status != http.StatusOK {
					t.Fatalf("%d %s", status, body)
				}
				if re := regexp.MustCompile(`intercepting logs with options`); !re.MatchString(body) {
					t.Fatal(body)
				}
				break
			}
		}
	}
}

func TestDebugLogSpyRun(t *testing.T) {
	defer leaktest.AfterTest(t)()

	send := make(chan log.InterceptorFn, 1)
	spy := logSpy{
		setIntercept: func(ctx context.Context, f log.InterceptorFn) {
			send <- f
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var buf bytes.Buffer
	go func() {
		if err := spy.run(ctx, &buf, logSpyOptions{
			Duration: durationAsString(time.Hour),
			Count:    3, // we expect 2 results but log spy sends a header event
			Grep:     regexpAsString{re: regexp.MustCompile(`first\.go|#2`)},
		}); err != nil {
			panic(err)
		}
		close(send)
	}()

	f := <-send

	f(log.Entry{
		File:    "first.go",
		Line:    1,
		Message: "#1",
	})
	f(log.Entry{
		File:    "nonmatching.go",
		Line:    12345,
		Message: "ignored because neither message nor file match",
	})
	f(log.Entry{
		File:    "second.go",
		Line:    2,
		Message: "#2",
	})
	if undoF := <-send; undoF != nil {
		t.Fatal("interceptor closed with non-nil function")
	}

	for i := 0; i < 10000; i++ {
		// f could be invoked arbitrarily after the operation finishes (though
		// in reality the duration would be limited to the blink of an eye). It
		// must not fill up a channel and block, or panic.
		f(log.Entry{})
	}

	body := buf.String()
	re := regexp.MustCompile(
		`(?m:.*intercepting.*\n.*first\.go:1\s+#1\n.*second\.go:2\s+#2)`,
	)
	if !re.MatchString(body) {
		t.Fatal(body)
	}
}

type brokenConnWriter struct {
	syncutil.Mutex // for stalling progress
	fail           *regexp.Regexp
	buf            bytes.Buffer
}

func (bcw *brokenConnWriter) Write(p []byte) (int, error) {
	bcw.Lock()
	defer bcw.Unlock()
	if bcw.fail.Match(p) {
		return 0, errors.Errorf("boom on: %q", p)
	}
	return bcw.buf.Write(p)
}

func TestDebugLogSpyBrokenConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, test := range []struct {
		name, failStr string
	}{
		{name: "OnEntry", failStr: "foobar #1"},
		{name: "OnDropped", failStr: "messages were dropped"},
	} {
		send := make(chan log.InterceptorFn, 1)
		spy := logSpy{
			setIntercept: func(_ context.Context, f log.InterceptorFn) {
				send <- f
			},
		}

		w := &brokenConnWriter{
			fail: regexp.MustCompile(test.failStr),
		}

		go func() {
			f := <-send
			// Block the writer while we create entries. That way, the spy will
			// have to drop entries and that's what we want (in one of the
			// tests. In the other, we error out earlier instead, so causing
			// dropped entries doesn't hurt either).
			w.Lock()
			defer w.Unlock()
			for i := 0; i < 2*logSpyChanCap; i++ {
				f(log.Entry{
					File:    "fake.go",
					Line:    int64(i),
					Message: fmt.Sprintf("foobar #%d", i),
				})
			}
		}()

		ctx := context.Background()

		func() {
			if err := spy.run(ctx, w, logSpyOptions{
				Duration: durationAsString(time.Hour),
				Count:    logSpyChanCap - 1, // will definitely see that many entries
			}); !testutils.IsError(err, test.failStr) {
				t.Fatal(err)
			}
		}()
	}
}
