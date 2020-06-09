// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cockroachdb/cockroach/pkg/release"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type mockS3 struct {
	gets []string
	puts []string
}

var _ s3I = (*mockS3)(nil)

func (s *mockS3) GetObject(i *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	url := fmt.Sprintf(`s3://%s/%s`, *i.Bucket, *i.Key)
	s.gets = append(s.gets, url)
	o := &s3.GetObjectOutput{
		Body: ioutil.NopCloser(bytes.NewBufferString(url)),
	}
	return o, nil
}

func (s *mockS3) PutObject(i *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	url := fmt.Sprintf(`s3://%s/%s`, *i.Bucket, *i.Key)
	if i.CacheControl != nil {
		url += `/` + *i.CacheControl
	}
	if i.Body != nil {
		bytes, err := ioutil.ReadAll(i.Body)
		if err != nil {
			return nil, err
		}
		if utf8.Valid(bytes) {
			s.puts = append(s.puts, fmt.Sprintf("%s CONTENTS %s", url, bytes))
		} else {
			s.puts = append(s.puts, fmt.Sprintf("%s CONTENTS <binary stuff>", url))
		}
	} else if i.WebsiteRedirectLocation != nil {
		s.puts = append(s.puts, fmt.Sprintf("%s REDIRECT %s", url, *i.WebsiteRedirectLocation))
	}
	return &s3.PutObjectOutput{}, nil
}

type mockExecRunner struct {
	cmds []string
}

func (r *mockExecRunner) run(c *exec.Cmd) ([]byte, error) {
	if c.Dir == `` {
		return nil, errors.Errorf(`Dir must be specified`)
	}
	cmd := fmt.Sprintf("env=%s args=%s", c.Env, c.Args)

	var path string
	if c.Args[0] == `mkrelease` {
		path = filepath.Join(c.Dir, `cockroach`)
		for _, arg := range c.Args {
			if strings.HasPrefix(arg, `SUFFIX=`) {
				path += strings.TrimPrefix(arg, `SUFFIX=`)
			}
		}
	} else if c.Args[0] == `make` && c.Args[1] == `archive` {
		for _, arg := range c.Args {
			if strings.HasPrefix(arg, `ARCHIVE=`) {
				path = filepath.Join(c.Dir, strings.TrimPrefix(arg, `ARCHIVE=`))
				break
			}
		}
	}

	if path != `` {
		if err := ioutil.WriteFile(path, []byte(cmd), 0666); err != nil {
			return nil, err
		}
		r.cmds = append(r.cmds, cmd)
	}

	var output []byte
	return output, nil
}

func TestProvisional(t *testing.T) {
	tests := []struct {
		name         string
		flags        runFlags
		expectedCmds []string
		expectedGets []string
		expectedPuts []string
	}{
		{
			name: `release`,
			flags: runFlags{
				doProvisional: true,
				isRelease:     true,
				branch:        `provisional_201901010101_v0.0.1-alpha`,
			},
			expectedCmds: []string{
				"env=[] args=[mkrelease darwin SUFFIX=.darwin-10.9-amd64 GOFLAGS= TAGS= BUILDCHANNEL=official-binary BUILDINFO_TAG=v0.0.1-alpha BUILD_TAGGED_RELEASE=true]",
				"env=[] args=[mkrelease linux-gnu SUFFIX=.linux-2.6.32-gnu-amd64 GOFLAGS= TAGS= BUILDCHANNEL=official-binary BUILDINFO_TAG=v0.0.1-alpha BUILD_TAGGED_RELEASE=true]",
				"env=[] args=[mkrelease windows SUFFIX=.windows-6.2-amd64.exe GOFLAGS= TAGS= BUILDCHANNEL=official-binary BUILDINFO_TAG=v0.0.1-alpha BUILD_TAGGED_RELEASE=true]",
				"env=[] args=[make archive ARCHIVE_BASE=cockroach-v0.0.1-alpha ARCHIVE=cockroach-v0.0.1-alpha.src.tgz BUILDINFO_TAG=v0.0.1-alpha]",
			},
			expectedGets: nil,
			expectedPuts: []string{
				"s3://binaries.cockroachdb.com/cockroach-v0.0.1-alpha.darwin-10.9-amd64.tgz " +
					"CONTENTS <binary stuff>",
				"s3://binaries.cockroachdb.com/cockroach-v0.0.1-alpha.linux-amd64.tgz " +
					"CONTENTS <binary stuff>",
				"s3://binaries.cockroachdb.com/cockroach-v0.0.1-alpha.windows-6.2-amd64.zip " +
					"CONTENTS <binary stuff>",
				"s3://binaries.cockroachdb.com/cockroach-v0.0.1-alpha.src.tgz " +
					"CONTENTS env=[] args=[make archive ARCHIVE_BASE=cockroach-v0.0.1-alpha ARCHIVE=cockroach-v0.0.1-alpha.src.tgz BUILDINFO_TAG=v0.0.1-alpha]",
			},
		},
		{
			name: `edge`,
			flags: runFlags{
				doProvisional: true,
				isRelease:     false,
				branch:        `master`,
				sha:           `00SHA00`,
			},
			expectedCmds: []string{
				"env=[] args=[mkrelease darwin SUFFIX=.darwin-10.9-amd64 GOFLAGS= TAGS= BUILDCHANNEL=official-binary]",
				"env=[] args=[mkrelease linux-gnu SUFFIX=.linux-2.6.32-gnu-amd64 GOFLAGS= TAGS= BUILDCHANNEL=official-binary]",
				"env=[] args=[mkrelease windows SUFFIX=.windows-6.2-amd64.exe GOFLAGS= TAGS= BUILDCHANNEL=official-binary]",
			},
			expectedGets: nil,
			expectedPuts: []string{
				"s3://cockroach//cockroach/cockroach.darwin-amd64.00SHA00 " +
					"CONTENTS env=[] args=[mkrelease darwin SUFFIX=.darwin-10.9-amd64 GOFLAGS= TAGS= BUILDCHANNEL=official-binary]",
				"s3://cockroach/cockroach/cockroach.darwin-amd64.LATEST/no-cache " +
					"REDIRECT /cockroach/cockroach.darwin-amd64.00SHA00",
				"s3://cockroach//cockroach/cockroach.linux-gnu-amd64.00SHA00 " +
					"CONTENTS env=[] args=[mkrelease linux-gnu SUFFIX=.linux-2.6.32-gnu-amd64 GOFLAGS= TAGS= BUILDCHANNEL=official-binary]",
				"s3://cockroach/cockroach/cockroach.linux-gnu-amd64.LATEST/no-cache " +
					"REDIRECT /cockroach/cockroach.linux-gnu-amd64.00SHA00",
				"s3://cockroach//cockroach/cockroach.windows-amd64.00SHA00.exe " +
					"CONTENTS env=[] args=[mkrelease windows SUFFIX=.windows-6.2-amd64.exe GOFLAGS= TAGS= BUILDCHANNEL=official-binary]",
				"s3://cockroach/cockroach/cockroach.windows-amd64.LATEST/no-cache " +
					"REDIRECT /cockroach/cockroach.windows-amd64.00SHA00.exe",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir, cleanup := testutils.TempDir(t)
			defer cleanup()

			var s3 mockS3
			var exec mockExecRunner
			flags := test.flags
			flags.pkgDir = dir
			run(&s3, exec.run, flags)
			require.Equal(t, test.expectedCmds, exec.cmds)
			require.Equal(t, test.expectedGets, s3.gets)
			require.Equal(t, test.expectedPuts, s3.puts)
		})
	}
}

func TestBless(t *testing.T) {
	tests := []struct {
		name         string
		flags        runFlags
		expectedGets []string
		expectedPuts []string
	}{
		{
			name: "testing",
			flags: runFlags{
				doBless:   true,
				isRelease: true,
				branch:    `provisional_201901010101_v0.0.1-alpha`,
			},
			expectedGets: nil,
			expectedPuts: nil,
		},
		{
			name: "stable",
			flags: runFlags{
				doBless:   true,
				isRelease: true,
				branch:    `provisional_201901010101_v0.0.1`,
			},
			expectedGets: []string{
				"s3://binaries.cockroachdb.com/cockroach-v0.0.1.darwin-10.9-amd64.tgz",
				"s3://binaries.cockroachdb.com/cockroach-v0.0.1.linux-amd64.tgz",
				"s3://binaries.cockroachdb.com/cockroach-v0.0.1.windows-6.2-amd64.zip",
				"s3://binaries.cockroachdb.com/cockroach-v0.0.1.src.tgz",
			},
			expectedPuts: []string{
				"s3://binaries.cockroachdb.com/cockroach-latest.darwin-10.9-amd64.tgz/no-cache " +
					"CONTENTS s3://binaries.cockroachdb.com/cockroach-v0.0.1.darwin-10.9-amd64.tgz",
				"s3://binaries.cockroachdb.com/cockroach-latest.linux-amd64.tgz/no-cache " +
					"CONTENTS s3://binaries.cockroachdb.com/cockroach-v0.0.1.linux-amd64.tgz",
				"s3://binaries.cockroachdb.com/cockroach-latest.windows-6.2-amd64.zip/no-cache " +
					"CONTENTS s3://binaries.cockroachdb.com/cockroach-v0.0.1.windows-6.2-amd64.zip",
				"s3://binaries.cockroachdb.com/cockroach-latest.src.tgz/no-cache " +
					"CONTENTS s3://binaries.cockroachdb.com/cockroach-v0.0.1.src.tgz",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var s3 mockS3
			var execFn release.ExecFn // bless shouldn't exec anything
			run(&s3, execFn, test.flags)
			require.Equal(t, test.expectedGets, s3.gets)
			require.Equal(t, test.expectedPuts, s3.puts)
		})
	}
}
