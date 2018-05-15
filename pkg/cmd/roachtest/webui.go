// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"

	"github.com/tebeka/selenium"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/webui"
)

func (c *cluster) getWebdriver(ctx context.Context, node int) (selenium.WebDriver, error) {
	caps := selenium.Capabilities{"browserName": "chrome"}
	wd, err := selenium.NewRemote(caps, "")
	if err != nil {
		return nil, err
	}

	urls := c.WebURL(ctx, c.Node(node))
	err = wd.Get(urls[0])

	return wd, err
}

type webuiTest struct {
	Name  string
	Nodes int
	Run   func(context.Context, *test, *cluster, selenium.WebDriver)
}

func registerWebUI(r *registry) {
	tests := []webuiTest{
		webuiTest{
			Name:  "webui/smoke",
			Nodes: 3,
			Run: func(ctx context.Context, t *test, c *cluster, wd selenium.WebDriver) {
				var page webui.Page = webui.MakeOverviewPage(wd)

				if page.Heading() != "CLUSTER OVERVIEW" {
					t.Fatalf(`Expected heading to be "CLUSTER OVERVIEW", saw "%s"!`, page.Heading())
				}

				if page.Title() != "Cluster Overview | Cockroach Console" {
					t.Fatalf(`Expected title to be "Cluster Overview | Cockroach Console", saw "%s"`, page.Title())
				}

				if !page.NavBar().OverviewLink().IsActive() {
					t.Fatal("Overview link should be active!")
				}

				if page.NavBar().MetricsLink().IsActive() {
					t.Fatal("Metrics link should not be active!")
				}

				if page.NavBar().DatabasesLink().IsActive() {
					t.Fatal("Databases link should not be active!")
				}

				if page.NavBar().JobsLink().IsActive() {
					t.Fatal("Jobs link should not be active!")
				}

				page = page.NavBar().DatabasesLink().Click()

				if page.Heading() != "DATABASES" {
					t.Fatalf(`Expected title to be "DATABASES", saw "%s"!`, page.Heading())
				}

				if page.Title() != "Tables | Databases | Cockroach Console" {
					t.Fatalf(`Expected title to be "Tables | Databases | Cockroach Console", saw "%s"`, page.Title())
				}

				if page.NavBar().OverviewLink().IsActive() {
					t.Fatal("Overview link should not be active!")
				}

				if page.NavBar().MetricsLink().IsActive() {
					t.Fatal("Metrics link should not be active!")
				}

				if !page.NavBar().DatabasesLink().IsActive() {
					t.Fatal("Databases link should be active!")
				}

				if page.NavBar().JobsLink().IsActive() {
					t.Fatal("Jobs link should not be active!")
				}
			},
		},
	}

	for _, testCase := range tests {
		r.Add(testSpec{
			Name:  testCase.Name,
			Nodes: nodes(testCase.Nodes),
			Run: func(ctx context.Context, t *test, c *cluster) {
				nodes := c.nodes - 1
				c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
				c.Start(ctx, c.Range(1, nodes))

				m := newMonitor(ctx, c, c.Range(1, nodes))
				m.Go(func(ctx context.Context) error {

					wd, err := c.getWebdriver(ctx, 1)
					if err != nil {
						t.Fatal("Error connecting to page: ", err)
					}

					defer wd.Quit()

					testCase.Run(ctx, t, c, wd)

					return nil
				})
				m.Wait()
			},
		})
	}
}
