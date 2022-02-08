// Copyright 2017 The Cockroach Authors.
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
	"os"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/stretchr/testify/require"
)

// Whether to run slow tests.
var slow bool

func init() {
	if err := os.Setenv("AWS_ACCESS_KEY_ID", "testing"); err != nil {
		panic(err)
	}
	if err := os.Setenv("AWS_SECRET_ACCESS_KEY", "hunter2"); err != nil {
		panic(err)
	}
}

type recorder struct {
	reqs []s3.PutObjectInput
}

func (r *recorder) PutObject(req *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	r.reqs = append(r.reqs, *req)
	return &s3.PutObjectOutput{}, nil
}

func mockPutter(p s3putter) func() {
	origPutter := testableS3
	f := func() {
		testableS3 = origPutter
	}
	testableS3 = func() (s3putter, error) {
		return p, nil
	}
	return f
}

// NB: the function name TestMain is special as it is expected to be a function
// which takes a *testing.M as a parameter. Somehow `go test` allows TestMain to
// also work if passed a *testing.T, in which case it acts like a normal test
// function. Bazel prohibits this, so give this test function a name which
// doesn't collide with TestMain.
func TestMainF(t *testing.T) {
	if !slow {
		skip.IgnoreLint(t, "only to be run manually via `./build/builder.sh go test -tags slow -timeout 1h -v ./pkg/cmd/publish-artifacts`")
	}
	r := &recorder{}
	undo := mockPutter(r)
	defer undo()

	shaPat := regexp.MustCompile(`[a-f0-9]{40}`)
	const shaStub = "<sha>"

	type testCase struct {
		Bucket, ContentDisposition, Key, WebsiteRedirectLocation, CacheControl string
	}
	exp := []testCase{
		{
			Bucket:             "cockroach",
			ContentDisposition: "attachment; filename=cockroach.darwin-amd64." + shaStub,
			Key:                "/cockroach/cockroach.darwin-amd64." + shaStub,
		},
		{
			Bucket:                  "cockroach",
			CacheControl:            "no-cache",
			Key:                     "cockroach/cockroach.darwin-amd64.LATEST",
			WebsiteRedirectLocation: "/cockroach/cockroach.darwin-amd64." + shaStub,
		},
		{
			Bucket:             "cockroach",
			ContentDisposition: "attachment; filename=libgeos.darwin-amd64." + shaStub + ".dylib",
			Key:                "/cockroach/lib/libgeos.darwin-amd64." + shaStub + ".dylib",
		},
		{
			Bucket:                  "cockroach",
			CacheControl:            "no-cache",
			Key:                     "cockroach/lib/libgeos.darwin-amd64.dylib.LATEST",
			WebsiteRedirectLocation: "/cockroach/lib/libgeos.darwin-amd64." + shaStub + ".dylib",
		},
		{
			Bucket:             "cockroach",
			ContentDisposition: "attachment; filename=libgeos_c.darwin-amd64." + shaStub + ".dylib",
			Key:                "/cockroach/lib/libgeos_c.darwin-amd64." + shaStub + ".dylib",
		},
		{
			Bucket:                  "cockroach",
			CacheControl:            "no-cache",
			Key:                     "cockroach/lib/libgeos_c.darwin-amd64.dylib.LATEST",
			WebsiteRedirectLocation: "/cockroach/lib/libgeos_c.darwin-amd64." + shaStub + ".dylib",
		},

		{
			Bucket:             "cockroach",
			ContentDisposition: "attachment; filename=cockroach.linux-gnu-amd64." + shaStub,
			Key:                "/cockroach/cockroach.linux-gnu-amd64." + shaStub,
		},
		{
			Bucket:                  "cockroach",
			CacheControl:            "no-cache",
			Key:                     "cockroach/cockroach.linux-gnu-amd64.LATEST",
			WebsiteRedirectLocation: "/cockroach/cockroach.linux-gnu-amd64." + shaStub,
		},
		{
			Bucket:             "cockroach",
			ContentDisposition: "attachment; filename=libgeos.linux-gnu-amd64." + shaStub + ".so",
			Key:                "/cockroach/lib/libgeos.linux-gnu-amd64." + shaStub + ".so",
		},
		{
			Bucket:                  "cockroach",
			CacheControl:            "no-cache",
			Key:                     "cockroach/lib/libgeos.linux-gnu-amd64.so.LATEST",
			WebsiteRedirectLocation: "/cockroach/lib/libgeos.linux-gnu-amd64." + shaStub + ".so",
		},
		{
			Bucket:             "cockroach",
			ContentDisposition: "attachment; filename=libgeos_c.linux-gnu-amd64." + shaStub + ".so",
			Key:                "/cockroach/lib/libgeos_c.linux-gnu-amd64." + shaStub + ".so",
		},
		{
			Bucket:                  "cockroach",
			CacheControl:            "no-cache",
			Key:                     "cockroach/lib/libgeos_c.linux-gnu-amd64.so.LATEST",
			WebsiteRedirectLocation: "/cockroach/lib/libgeos_c.linux-gnu-amd64." + shaStub + ".so",
		},

		{
			Bucket:             "cockroach",
			ContentDisposition: "attachment; filename=cockroach.race.linux-gnu-amd64." + shaStub,
			Key:                "/cockroach/cockroach.race.linux-gnu-amd64." + shaStub,
		},
		{
			Bucket:                  "cockroach",
			CacheControl:            "no-cache",
			Key:                     "cockroach/cockroach.race.linux-gnu-amd64.LATEST",
			WebsiteRedirectLocation: "/cockroach/cockroach.race.linux-gnu-amd64." + shaStub,
		},
		{
			Bucket:             "cockroach",
			ContentDisposition: "attachment; filename=libgeos.race.linux-gnu-amd64." + shaStub + ".so",
			Key:                "/cockroach/lib/libgeos.race.linux-gnu-amd64." + shaStub + ".so",
		},
		{
			Bucket:                  "cockroach",
			CacheControl:            "no-cache",
			Key:                     "cockroach/lib/libgeos.race.linux-gnu-amd64.so.LATEST",
			WebsiteRedirectLocation: "/cockroach/lib/libgeos.race.linux-gnu-amd64." + shaStub + ".so",
		},
		{
			Bucket:             "cockroach",
			ContentDisposition: "attachment; filename=libgeos_c.race.linux-gnu-amd64." + shaStub + ".so",
			Key:                "/cockroach/lib/libgeos_c.race.linux-gnu-amd64." + shaStub + ".so",
		},
		{
			Bucket:                  "cockroach",
			CacheControl:            "no-cache",
			Key:                     "cockroach/lib/libgeos_c.race.linux-gnu-amd64.so.LATEST",
			WebsiteRedirectLocation: "/cockroach/lib/libgeos_c.race.linux-gnu-amd64." + shaStub + ".so",
		},

		{
			Bucket:             "cockroach",
			ContentDisposition: "attachment; filename=cockroach.windows-amd64." + shaStub + ".exe",
			Key:                "/cockroach/cockroach.windows-amd64." + shaStub + ".exe",
		},
		{
			Bucket:                  "cockroach",
			CacheControl:            "no-cache",
			Key:                     "cockroach/cockroach.windows-amd64.LATEST",
			WebsiteRedirectLocation: "/cockroach/cockroach.windows-amd64." + shaStub + ".exe",
		},
		{
			Bucket:             "cockroach",
			ContentDisposition: "attachment; filename=libgeos.windows-amd64." + shaStub + ".dll",
			Key:                "/cockroach/lib/libgeos.windows-amd64." + shaStub + ".dll",
		},
		{
			Bucket:                  "cockroach",
			CacheControl:            "no-cache",
			Key:                     "cockroach/lib/libgeos.windows-amd64.dll.LATEST",
			WebsiteRedirectLocation: "/cockroach/lib/libgeos.windows-amd64." + shaStub + ".dll",
		},
		{
			Bucket:             "cockroach",
			ContentDisposition: "attachment; filename=libgeos_c.windows-amd64." + shaStub + ".dll",
			Key:                "/cockroach/lib/libgeos_c.windows-amd64." + shaStub + ".dll",
		},
		{
			Bucket:                  "cockroach",
			CacheControl:            "no-cache",
			Key:                     "cockroach/lib/libgeos_c.windows-amd64.dll.LATEST",
			WebsiteRedirectLocation: "/cockroach/lib/libgeos_c.windows-amd64." + shaStub + ".dll",
		},

		{
			Bucket:             "cockroach",
			ContentDisposition: "attachment; filename=workload." + shaStub,
			Key:                "/cockroach/workload." + shaStub,
		},
		{
			Bucket:                  "cockroach",
			CacheControl:            "no-cache",
			Key:                     "cockroach/workload.LATEST",
			WebsiteRedirectLocation: "/cockroach/workload." + shaStub,
		},
		{
			Bucket: "binaries.cockroachdb.com",
			Key:    "cockroach-v42.42.42.src.tgz",
		},
		{
			Bucket:       "binaries.cockroachdb.com",
			CacheControl: "no-cache",
			Key:          "cockroach-latest.src.tgz",
		},
		{
			Bucket: "binaries.cockroachdb.com",
			Key:    "cockroach-v42.42.42.darwin-10.9-amd64.tgz",
		},
		{
			Bucket:       "binaries.cockroachdb.com",
			CacheControl: "no-cache",
			Key:          "cockroach-latest.darwin-10.9-amd64.tgz",
		},
		{
			Bucket: "binaries.cockroachdb.com",
			Key:    "cockroach-v42.42.42.linux-amd64.tgz",
		},
		{
			Bucket:       "binaries.cockroachdb.com",
			CacheControl: "no-cache",
			Key:          "cockroach-latest.linux-amd64.tgz",
		},
		{
			Bucket: "binaries.cockroachdb.com",
			Key:    "cockroach-v42.42.42.windows-6.2-amd64.zip",
		},
		{
			Bucket:       "binaries.cockroachdb.com",
			CacheControl: "no-cache",
			Key:          "cockroach-latest.windows-6.2-amd64.zip",
		},
	}

	if err := os.Setenv("TC_BUILD_BRANCH", "master"); err != nil {
		t.Fatal(err)
	}
	main()

	if err := os.Setenv("TC_BUILD_BRANCH", "v42.42.42"); err != nil {
		t.Fatal(err)
	}
	*isRelease = true
	main()

	var acts []testCase
	for _, req := range r.reqs {
		var act testCase
		if req.Bucket != nil {
			act.Bucket = *req.Bucket
		}
		if req.ContentDisposition != nil {
			act.ContentDisposition = shaPat.ReplaceAllLiteralString(*req.ContentDisposition, shaStub)
		}
		if req.Key != nil {
			act.Key = shaPat.ReplaceAllLiteralString(*req.Key, shaStub)
		}
		if req.WebsiteRedirectLocation != nil {
			act.WebsiteRedirectLocation = shaPat.ReplaceAllLiteralString(*req.WebsiteRedirectLocation, shaStub)
		}
		if req.CacheControl != nil {
			act.CacheControl = *req.CacheControl
		}
		acts = append(acts, act)
	}

	require.Equal(t, exp, acts)
}
