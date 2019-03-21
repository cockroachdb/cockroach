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
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// This test runs TypeORM's full test suite against a single cockroach node.
func registerTypeORM(r *registry) {
	runTypeORM := func(
		ctx context.Context,
		t *test,
		c *cluster,
	) {
		if c.isLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up cockroach")
		c.Put(ctx, cockroach, "./cockroach", c.All())
		c.Start(ctx, t, c.All())

		latestTag, err := repeatGetLatestTag(ctx, c, "typeorm", "typeorm")
		if err != nil {
			t.Fatal(err)
		}
		if len(latestTag) == 0 {
			t.Fatal(fmt.Sprintf("did not get a latest tag"))
		}
		c.l.Printf("Latest TypeORM release is %s.", latestTag)

		t.Status("cloning TypeORM and installing prerequisites")
		if err := repeatRunE(
			ctx, c, node, "update apt-get", `sudo apt-get -q update`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			c,
			node,
			"add nodesource repository",
			`curl -sL https://deb.nodesource.com/setup_11.x | sudo -E bash -`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "install nodejs and npm", `sudo apt-get -qy install nodejs`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "update npm", `sudo npm i -g npm`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "remove old TypeORM", `sudo rm -rf /mnt/data1/typeorm`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			c,
			"https://github.com/typeorm/typeorm.git",
			"/mnt/data1/typeorm",
			latestTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		// TypeORM is super picky about this file format and if it cannot be parsed
		// it will return a file not found error.
		if err := repeatRunE(
			ctx,
			c,
			node,
			"configuring tests for cockroach only",
			fmt.Sprintf("echo '%s' > /mnt/data1/typeorm/ormconfig.json", typeORMConfigJSON),
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "building TypeORM", `cd /mnt/data1/typeorm/ && sudo npm install`,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("running TypeORM test suite - approx 12 mins")
		rawResults, err := c.RunWithBuffer(ctx, t.l, node,
			`cd /mnt/data1/typeorm/ && sudo npm test`,
		)
		c.l.Printf("Test Results: %s", rawResults)
		if err != nil {
			t.Fatal(err)
		}
	}

	r.Add(testSpec{
		Name:       "typeorm",
		Cluster:    makeClusterSpec(1),
		MinVersion: "v19.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTypeORM(ctx, t, c)
		},
	})
}

// TODO(bram): Move these functions to either be part of cluster or in a
// canary helper file. And use them as part of the other canary tests.
var canaryRetryOptions = retry.Options{
	InitialBackoff: 10 * time.Second,
	Multiplier:     2,
	MaxBackoff:     5 * time.Minute,
}

func repeatRunE(
	ctx context.Context, c *cluster, node nodeListOption, operation string, args ...string,
) error {
	for attempt, r := 0, retry.StartWithCtx(ctx, canaryRetryOptions); r.Next(); {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if c.t.Failed() {
			return fmt.Errorf("test has failed")
		}
		attempt++
		c.l.Printf("attempt %d - %s", attempt, operation)
		if err := c.RunE(ctx, node, args...); err != nil {
			c.l.Printf("error - retrying: %s", err)
			continue
		}
		break
	}
	return nil
}

func repeatGitCloneE(
	ctx context.Context, c *cluster, src, dest, branch string, node nodeListOption,
) error {
	for attempt, r := 0, retry.StartWithCtx(ctx, canaryRetryOptions); r.Next(); {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if c.t.Failed() {
			return fmt.Errorf("test has failed")
		}
		attempt++
		c.l.Printf("attempt %d - clone %s", attempt, src)
		if err := c.GitCloneE(ctx, src, dest, branch, node); err != nil {
			c.l.Printf("error - retrying: %s", err)
			continue
		}
		break
	}
	return nil
}

func repeatGetLatestTag(ctx context.Context, c *cluster, user string, repo string) (string, error) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/tags", user, repo)
	httpClient := &http.Client{Timeout: 10 * time.Second}
	type Tag struct {
		Name string
	}
	type Tags []Tag
	for attempt, r := 0, retry.StartWithCtx(ctx, canaryRetryOptions); r.Next(); {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if c.t.Failed() {
			return "", fmt.Errorf("test has failed")
		}
		attempt++

		c.l.Printf("attempt %d - fetching %s", attempt, url)
		r, err := httpClient.Get(url)
		if err != nil {
			c.l.Printf("error fetching - retrying: %s", err)
			continue
		}
		defer r.Body.Close()

		var tags Tags
		if err := json.NewDecoder(r.Body).Decode(&tags); err != nil {
			c.l.Printf("error decoding - retrying: %s", err)
			continue
		}
		if len(tags) == 0 {
			return "", fmt.Errorf("no tags found at %s", url)
		}

		actualTags := make([]string, len(tags))
		for i, t := range tags {
			actualTags[i] = t.Name
		}
		sort.Strings(actualTags)
		return actualTags[len(actualTags)-1], nil
	}
	// This should be unreachable.
	return "", fmt.Errorf("could not get tags from %s", url)
}

const typeORMConfigJSON = `
[
  {
    "skip": true,
    "name": "mysql",
    "type": "mysql",
    "host": "localhost",
    "port": 3306,
    "username": "root",
    "password": "admin",
    "database": "test",
    "logging": false
  },
  {
    "skip": true,
    "name": "mariadb",
    "type": "mariadb",
    "host": "localhost",
    "port": 3307,
    "username": "root",
    "password": "admin",
    "database": "test",
    "logging": false
  },
  {
    "skip": true,
    "name": "sqlite",
    "type": "sqlite",
    "database": "temp/sqlitedb.db",
    "logging": false
  },
  {
    "skip": true,
    "name": "postgres",
    "type": "postgres",
    "host": "localhost",
    "port": 5432,
    "username": "test",
    "password": "test",
    "database": "test",
    "logging": false
  },
  {
    "skip": true,
    "name": "mssql",
    "type": "mssql",
    "host": "localhost",
    "username": "sa",
    "password": "Admin12345",
    "database": "tempdb",
    "logging": false
  },
  {
    "skip": true,
    "name": "oracle",
    "type": "oracle",
    "host": "localhost",
    "username": "system",
    "password": "oracle",
    "port": 1521,
    "sid": "xe.oracle.docker",
    "logging": false
  },
  {
    "skip": false,
    "name": "cockroachdb",
    "type": "cockroachdb",
    "host": "localhost",
    "port": 26257,
    "username": "root",
    "password": "",
    "database": "defaultdb"
  },
  {
    "skip": true,
    "disabledIfNotEnabledImplicitly": true,
    "name": "mongodb",
    "type": "mongodb",
    "database": "test",
    "logging": false,
    "useNewUrlParser": true
  }
]
`
