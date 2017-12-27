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

package main

import (
	"os"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/kr/pretty"
)

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

func TestMain(t *testing.T) {
	t.Skip("only to be run manually via `./build/builder.sh go test -timeout 1h -v ./pkg/cmd/publish-artifacts`")
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
			Bucket:       "cockroach",
			CacheControl: "no-cache",
			Key:          "cockroach/cockroach.darwin-amd64.LATEST",
			WebsiteRedirectLocation: "/cockroach/cockroach.darwin-amd64." + shaStub,
		},
		{
			Bucket:             "cockroach",
			ContentDisposition: "attachment; filename=cockroach.linux-gnu-amd64." + shaStub,
			Key:                "/cockroach/cockroach.linux-gnu-amd64." + shaStub,
		},
		{
			Bucket:       "cockroach",
			CacheControl: "no-cache",
			Key:          "cockroach/cockroach.linux-gnu-amd64.LATEST",
			WebsiteRedirectLocation: "/cockroach/cockroach.linux-gnu-amd64." + shaStub,
		},
		{
			Bucket:             "cockroach",
			ContentDisposition: "attachment; filename=cockroach.race.linux-gnu-amd64." + shaStub,
			Key:                "/cockroach/cockroach.race.linux-gnu-amd64." + shaStub,
		},
		{
			Bucket:       "cockroach",
			CacheControl: "no-cache",
			Key:          "cockroach/cockroach.race.linux-gnu-amd64.LATEST",
			WebsiteRedirectLocation: "/cockroach/cockroach.race.linux-gnu-amd64." + shaStub,
		},
		{
			Bucket:             "cockroach",
			ContentDisposition: "attachment; filename=cockroach.linux-musl-amd64." + shaStub,
			Key:                "/cockroach/cockroach.linux-musl-amd64." + shaStub,
		},
		{
			Bucket:       "cockroach",
			CacheControl: "no-cache",
			Key:          "cockroach/cockroach.linux-musl-amd64.LATEST",
			WebsiteRedirectLocation: "/cockroach/cockroach.linux-musl-amd64." + shaStub,
		},
		{
			Bucket:             "cockroach",
			ContentDisposition: "attachment; filename=cockroach.windows-amd64." + shaStub + ".exe",
			Key:                "/cockroach/cockroach.windows-amd64." + shaStub + ".exe",
		},
		{
			Bucket:       "cockroach",
			CacheControl: "no-cache",
			Key:          "cockroach/cockroach.windows-amd64.LATEST",
			WebsiteRedirectLocation: "/cockroach/cockroach.windows-amd64." + shaStub + ".exe",
		},
	}

	if err := os.Setenv("TC_BUILD_BRANCH", "master"); err != nil {
		t.Fatal(err)
	}

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

	for i := len(exp); i < len(acts); i++ {
		exp = append(exp, testCase{})
	}
	for i := len(acts); i < len(exp); i++ {
		acts = append(acts, testCase{})
	}

	if len(pretty.Diff(acts, exp)) > 0 {
		t.Error("diff(act, exp) is nontrivial")
		pretty.Ldiff(t, acts, exp)
	}
}
