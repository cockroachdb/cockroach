// Copyright 2019 The Cockroach Authors.
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

package report_test

import (
	"context"
	goErr "errors"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/errors/domains"
	"github.com/cockroachdb/cockroach/pkg/errors/report"
	"github.com/cockroachdb/cockroach/pkg/errors/safedetails"
	"github.com/cockroachdb/cockroach/pkg/errors/withstack"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	raven "github.com/getsentry/raven-go"
	"github.com/kr/pretty"
)

func TestReport(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var packets []*raven.Packet
	defer log.TestingSetCrashReportingURL("https://ignored:ignored@ignored/ignored")()

	// Install a Transport that locally records packets rather than sending them
	// to Sentry over HTTP.
	defer func(transport raven.Transport) {
		raven.DefaultClient.Transport = transport
	}(raven.DefaultClient.Transport)
	raven.DefaultClient.Transport = log.InterceptingTransport{
		SendFunc: func(_, _ string, packet *raven.Packet) {
			packets = append(packets, packet)
		},
	}

	log.SetupCrashReporter(ctx, "test")

	thisDomain := domains.NamedDomain("thisdomain")

	err := goErr.New("hello")
	err = safedetails.WithSafeDetails(err, "universe %d", log.Safe(123))
	err = withstack.WithStack(err)
	err = domains.WithDomain(err, thisDomain)
	report.ReportError(ctx, err)

	t.Logf("received packets: %# v", pretty.Formatter(packets))

	tt := testutils.T{T: t}

	tt.Assert(len(packets) == 1)
	p := packets[0]

	tt.Run("valid short message", func(tt testutils.T) {
		tt.CheckRegexpEqual(p.Message, `report_test.go:\d+: TestReport: universe %d`)
	})

	tt.Run("valid extra details", func(tt testutils.T) {
		expectedTypes := `errors/*errors.errorString::
github.com/cockroachdb/cockroach/pkg/errors/safedetails/*safedetails.withSafeDetails::
github.com/cockroachdb/cockroach/pkg/errors/withstack/*withstack.withStack::
github.com/cockroachdb/cockroach/pkg/errors/domains/*domains.withDomain::error domain: "thisdomain"
`
		types := fmt.Sprintf("%s", p.Extra["error types"])
		tt.CheckEqual(types, expectedTypes)

		expectedDetail := "universe %d\n-- arg 0: 123"
		detail := fmt.Sprintf("%s", p.Extra["1: details"])
		tt.CheckEqual(strings.TrimSpace(detail), expectedDetail)

		expectedDetail = string(thisDomain)
		detail = fmt.Sprintf("%s", p.Extra["3: details"])
		tt.CheckEqual(strings.TrimSpace(detail), expectedDetail)
	})

	hasMessage := false
	hasStack := false
	for _, im := range p.Interfaces {
		switch m := im.(type) {
		case *raven.Message:
			tt.Check(!hasMessage) // more than one message payload is invalid

			tt.Run("message payload", func(tt testutils.T) {
				expectedMessage := `^\*errors.errorString
\*safedetails.withSafeDetails: universe %d \(1\)
report_test.go:\d+: \*withstack.withStack \(2\)
\*domains.withDomain: error domain: "thisdomain" \(3\)
\(check the extra data payloads\)$`
				tt.CheckRegexpEqual(m.Message, expectedMessage)
			})

			hasMessage = true

		case *raven.Exception:
			tt.Check(!hasStack)

			tt.Run("stack trace payload", func(tt testutils.T) {
				tt.CheckRegexpEqual(m.Value, `report_test.go:\d+: TestReport: universe %d`)

				tt.CheckEqual(m.Module, string(thisDomain))

				st := m.Stacktrace
				if st == nil || len(st.Frames) < 1 {
					t.Error("stack trace too short")
				} else {
					f := st.Frames[len(st.Frames)-1]
					tt.Check(strings.HasSuffix(f.Filename, "report_test.go"))
					tt.Check(strings.HasSuffix(f.AbsolutePath, "report_test.go"))
					tt.Check(strings.HasSuffix(f.Module, "/report_test"))
					tt.CheckEqual(f.Function, "TestReport")
					tt.Check(f.Lineno != 0)
				}
			})
			hasStack = true
		}
	}

	tt.Check(hasStack && hasMessage)
}
