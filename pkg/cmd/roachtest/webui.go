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
	"fmt"
	//"io/ioutil"
	//"os/exec"
	//"time"

	"github.com/tebeka/selenium"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/webui"
)

func (c *cluster) webdriver(ctx context.Context, node int) (selenium.WebDriver, error) {
	caps := selenium.Capabilities{"browserName": "chrome"}
	wd, err := selenium.NewRemote(caps, "")
	if err != nil {
		panic(err)
	}

	urls := c.WebURL(ctx, c.Node(node))

	err = wd.Get(urls[0])

	return wd, err
}

func Test(wd selenium.WebDriver) error {
	page := webui.MakeOverviewPage(wd)

	link := page.NavBar().OverviewLink()

	if !link.IsActive() {
		return fmt.Errorf("Overview link should be active!")
	}

	next := page.NavBar().DatabasesLink().Click()

	output := next.Heading()
	fmt.Printf("Got: %s\n", output)

	if output != "DATABASES2" {
		return fmt.Errorf("Expected title to be `DATABASES`, saw `%s`!", output)
	}

	return nil
}

func registerWebUI(r *registry) {
	r.Add(testSpec{
		Name:  "webui",
		Nodes: nodes(3),
		Run: func(ctx context.Context, t *test, c *cluster) {
			nodes := c.nodes - 1
			c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
			c.Start(ctx, c.Range(1, nodes))

			m := newMonitor(ctx, c, c.Range(1, nodes))
			m.Go(func(ctx context.Context) error {

				wd, err := c.webdriver(ctx, 1)
				if err != nil {
					t.Fatal("Error loading page: ", err)
				}

				defer wd.Quit()

				err = Test(wd)
				if err != nil {
					t.Fatal("Error running test: ", err)
				}

				return nil
			})
			m.Wait()
		},
	})
}
