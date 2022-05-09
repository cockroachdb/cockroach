// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package skip

import (
	"bufio"
	"io"
	"strings"

	"github.com/pkg/errors"
)

var DefaultRegistry Registry

func init() {
	if err := DefaultRegistry.Import(strings.NewReader(SkippedTests)); err != nil {
		panic(err)
	}
}

type fqName struct {
	pkg, test string
}

type Registry struct {
	m map[fqName]string // test name -> skipped
}

// TODO(tbg): JSON, yaml, etc might all be better options.
// Need to make sure to mark them as a build dependency for
// this pkg for bzl.
func (r *Registry) Import(rdr io.Reader) error {
	sc := bufio.NewScanner(rdr)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		pkgAndName, reason, ok := strings.Cut(line, " ")
		if !ok {
			return errors.Errorf("invalid line: %q", line)
		}
		lastDot := strings.LastIndex(pkgAndName, ".")
		if lastDot < 0 {
			return errors.Errorf("invalid line: %q", line)
		}
		if r.m == nil {
			r.m = map[fqName]string{}
		}
		r.m[fqName{
			pkg:  pkgAndName[:lastDot],
			test: pkgAndName[lastDot+1:],
		}] = reason
	}
	return sc.Err()
}

func (r *Registry) IsSkipped(pkg, testName string) (reason string, skipped bool) {
	reason, skipped = r.m[fqName{pkg: pkg, test: testName}]
	return reason, skipped
}

const SkippedTests = `
pkg/ccl/telemetryccl.TestTelemetry/index/server don't know but I skipped it
`
