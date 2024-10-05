// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package debug

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
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
				Count:          123,
				vmoduleOptions: vmoduleOptions{Duration: durationAsString(9 * time.Second)},
				Grep:           regexpAsString{re: regexp.MustCompile(`^foo$`)},
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
				Count:          123,
				vmoduleOptions: vmoduleOptions{Duration: durationAsString(9 * time.Second)},
				Grep:           regexpAsString{re: regexp.MustCompile(`123`)},
			},
		},
		{
			// When nothing is given, default to "infinite" count and a 5s duration.
			expOpts: logSpyOptions{
				Count:          logSpyDefaultCount,
				vmoduleOptions: vmoduleOptions{Duration: logSpyDefaultDuration},
			},
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
			expErr: `time: invalid duration "very long"`,
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
			setIntercept: func(ctx context.Context, f log.Interceptor) func() {
				t.Fatal("tried to intercept")
				return nil
			},
		}

		r := httptest.NewRequest("GET", "/?duration=notaduration", nil)
		rec := httptest.NewRecorder()
		spy.handleDebugLogSpy(rec, r)
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("unexpected status: %d", rec.Code)
		}
		exp := "while parsing options: time: invalid duration \"notaduration\"\n"
		if body := rec.Body.String(); body != exp {
			t.Fatalf("expected: %q\ngot: %q", exp, body)
		}
	}
}

func TestDebugLogSpyRun(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	send := make(chan log.Interceptor, 1)
	spy := logSpy{
		setIntercept: func(ctx context.Context, f log.Interceptor) func() {
			send <- f
			return func() {}
		},
		tenantID: roachpb.SystemTenantID,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var buf bytes.Buffer
	go func() {
		defer func() {
			close(send)
		}()
		if err := spy.run(ctx, &buf, logSpyOptions{
			vmoduleOptions: vmoduleOptions{Duration: durationAsString(5 * time.Second)},
			Count:          2,
			Grep:           regexpAsString{re: regexp.MustCompile(`first\.go|#2`)},
		}); err != nil {
			panic(err)
		}
	}()

	t.Logf("waiting for interceptor")
	f := <-send
	t.Logf("got interceptor, sending some events")

	f.Intercept(toJSON(t, logpb.Entry{
		File:     "first.go",
		Line:     1,
		Message:  "#1",
		TenantID: "1",
	}))
	f.Intercept(toJSON(t, logpb.Entry{
		File:     "nonmatching.go",
		Line:     12345,
		Message:  "ignored because neither message nor file match",
		TenantID: "1",
	}))
	f.Intercept(toJSON(t, logpb.Entry{
		File:     "second.go",
		Line:     2,
		Message:  "#2",
		TenantID: "2",
	}))
	if undoF := <-send; undoF != nil {
		t.Fatal("interceptor closed with non-nil function")
	}

	t.Logf("fill in the channel")

	for i := 0; i < 10000; i++ {
		// f could be invoked arbitrarily after the operation finishes (though
		// in reality the duration would be limited to the blink of an eye). It
		// must not fill up a channel and block, or panic.
		f.Intercept(toJSON(t, logpb.Entry{}))
	}

	t.Logf("check results")

	body := buf.String()
	const expected = `{"file":"first.go","line":1,"message":"#1","tenant_id":"1"}
{"file":"second.go","line":2,"message":"#2","tenant_id":"2"}
`
	if expected != body {
		t.Fatalf("expected:\n%q\ngot:\n%q", expected, body)
	}
}

func TestDebugLogSpyTenantFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tID, err := roachpb.MakeTenantID(uint64(2))
	require.NoError(t, err)
	otherTenantID, err := roachpb.MakeTenantID(uint64(3))
	require.NoError(t, err)
	// Logs for now use the integer representation for
	// all tenant IDs, so use `1` instead of `system`.
	sysTenantIDStr := "1"

	send := make(chan log.Interceptor, 1)
	spy := logSpy{
		setIntercept: func(ctx context.Context, f log.Interceptor) func() {
			send <- f
			return func() {}
		},
		tenantID: tID,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var buf bytes.Buffer
	go func() {
		defer func() {
			close(send)
		}()
		if err := spy.run(ctx, &buf, logSpyOptions{
			vmoduleOptions: vmoduleOptions{Duration: durationAsString(5 * time.Second)},
			Count:          2,
			tenantIDFilter: tID.String(),
		}); err != nil {
			panic(err)
		}
	}()

	t.Logf("waiting for interceptor")
	f := <-send
	t.Logf("got interceptor, sending some events")

	f.Intercept(toJSON(t, logpb.Entry{
		File:     "first.go",
		Line:     1,
		Message:  "#1",
		TenantID: tID.String(),
	}))
	f.Intercept(toJSON(t, logpb.Entry{
		File:     "second.go",
		Line:     12345,
		Message:  "ignored because tenant ID does not match filter",
		TenantID: otherTenantID.String(),
	}))
	f.Intercept(toJSON(t, logpb.Entry{
		File:     "third.go",
		Line:     12345,
		Message:  "also ignored because tenant ID does not match filter",
		TenantID: sysTenantIDStr,
	}))
	f.Intercept(toJSON(t, logpb.Entry{
		File:     "fourth.go",
		Line:     2,
		Message:  "#2",
		TenantID: tID.String(),
	}))
	if undoF := <-send; undoF != nil {
		t.Fatal("interceptor closed with non-nil function")
	}

	t.Logf("fill in the channel")

	for i := 0; i < 10000; i++ {
		// f could be invoked arbitrarily after the operation finishes (though
		// in reality the duration would be limited to the blink of an eye). It
		// must not fill up a channel and block, or panic.
		f.Intercept(toJSON(t, logpb.Entry{}))
	}

	t.Logf("check results")

	body := buf.String()
	const expected = `{"file":"first.go","line":1,"message":"#1","tenant_id":"2"}
{"file":"fourth.go","line":2,"message":"#2","tenant_id":"2"}
`
	if expected != body {
		t.Fatalf("expected:\n%q\ngot:\n%q", expected, body)
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

func toJSON(t *testing.T, entry logpb.Entry) []byte {
	j, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestDebugLogSpyBrokenConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, test := range []struct {
		name, failStr string
	}{
		{name: "OnEntry", failStr: "foobar #1"},
		{name: "OnDropped", failStr: "messages were dropped"},
	} {
		send := make(chan log.Interceptor, 1)
		spy := logSpy{
			setIntercept: func(_ context.Context, f log.Interceptor) func() {
				send <- f
				return func() {}
			},
		}

		w := &brokenConnWriter{
			fail: regexp.MustCompile(test.failStr),
		}

		go func() {
			t.Logf("waiting for intercept function")
			f := <-send
			t.Logf("got interceptor")
			// Block the writer while we create entries. That way, the spy will
			// have to drop entries and that's what we want (in one of the
			// tests. In the other, we error out earlier instead, so causing
			// dropped entries doesn't hurt either).
			w.Lock()
			defer w.Unlock()
			t.Logf("writing entries...")
			for i := 0; i < 2*logSpyChanCap; i++ {
				f.Intercept(toJSON(t, logpb.Entry{
					File:    "fake.go",
					Line:    int64(i),
					Message: fmt.Sprintf("foobar #%d", i),
				}))
			}
			t.Logf("all entries written")
		}()

		ctx := context.Background()

		t.Logf("running logspy...")
		if err := spy.run(ctx, w, logSpyOptions{
			vmoduleOptions: vmoduleOptions{Duration: durationAsString(5 * time.Second)},
			Count:          logSpyChanCap - 1, // will definitely see that many entries
		}); !testutils.IsError(err, test.failStr) {
			t.Fatal(err)
		}
	}
}
