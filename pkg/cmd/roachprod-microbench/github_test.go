// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package main

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost/issues"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
)

func TestCreatePostRequest(t *testing.T) {
	datadriven.Walk(t, datapathutils.TestDataPath(t, "github"), func(t *testing.T, path string) {
		var response cluster.RemoteResponse
		var bk benchmarkKey
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "benchmark":
				d.ScanArgs(t, "name", &bk.name)
				d.ScanArgs(t, "pkg", &bk.pkg)
				d.ScanArgs(t, "args", &response.Args)
				return ""
			case "stdout":
				response.Stdout = d.Input
				return ""
			case "stderr":
				response.Stderr = d.Input
				return ""
			case "post":
				response.Err = errors.New("benchmark failed")
				response.ExitStatus = 1
				response.Duration = time.Second * 10
				response.Metadata = bk
				formatter, req := createBenchmarkPostRequest("", response, false)
				str, err := formatPostRequest(formatter, req)
				if err != nil {
					t.Fatal(err)
				}
				return str
			}
			return ""
		})
	})
}

// formatPostRequest emulates the behavior of the githubpost package.
func formatPostRequest(formatter issues.IssueFormatter, req issues.PostRequest) (string, error) {
	// These fields can vary based on the test env so we set them to arbitrary
	// values here.
	req.MentionOnCreate = []string{"@test-eng"}

	data := issues.TemplateData{
		PostRequest:      req,
		Parameters:       req.ExtraParams,
		CondensedMessage: issues.CondensedMessage(req.Message),
		PackageNameShort: req.PackageName,
	}

	r := &issues.Renderer{}
	if err := formatter.Body(r, data); err != nil {
		return "", err
	}

	var post strings.Builder
	post.WriteString(r.String())

	u, err := url.Parse("https://github.com/cockroachdb/cockroach/issues/new")
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Add("title", formatter.Title(data))
	q.Add("body", post.String())
	// Adding a template parameter is required to be able to view the rendered
	// template on GitHub, otherwise it just takes you to the template selection
	// page.
	q.Add("template", "none")
	u.RawQuery = q.Encode()
	post.WriteString(fmt.Sprintf("Rendered:\n%s", u.String()))

	return post.String(), nil
}
