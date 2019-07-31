// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl_test

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
)

// cliTest is a stripped-down version of package cli's cliTest. It's currently
// easier to reimplement the pieces we need than it is to export cli.cliTest, as
// cli's tests frequently mutate internal package variables. We may need to
// revisit this decision if this package's tests become more complex.
type cliTest struct {
	*server.TestServer
	connArgs []string
}

func newCLITest(args base.TestServerArgs) cliTest {
	s, err := serverutils.StartServerRaw(args)
	if err != nil {
		panic(err)
	}
	host, port, err := net.SplitHostPort(s.ServingAddr())
	if err != nil {
		panic(err)
	}
	return cliTest{
		TestServer: s.(*server.TestServer),
		connArgs:   []string{"--insecure", "--host=" + host + ":" + port},
	}
}

func (c *cliTest) close() {
	c.Stopper().Stop(context.Background())
}

func (c *cliTest) run(line string) {
	c.runWithArgs(strings.Fields(line))
}

func (c *cliTest) runWithArgs(args []string) {
	fmt.Println(strings.Join(args, " "))
	cli.TestingReset()
	if err := cli.Run(append(args, c.connArgs...)); err != nil {
		fmt.Println(err)
	}
}

func Example_cclzone() {
	// Note(solon): This test is redundant with the Example_zone test and could be
	// removed. However, I'm keeping it here as an example in case we need to add
	// more CCL CLI tests in the future.
	storeSpec := base.DefaultTestStoreSpec
	storeSpec.Attributes = roachpb.Attributes{Attrs: []string{"ssd"}}
	c := newCLITest(base.TestServerArgs{
		Insecure:   true,
		StoreSpecs: []base.StoreSpec{storeSpec},
		Locality: roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "region", Value: "us-east-1"},
				{Key: "zone", Value: "us-east-1a"},
			},
		},
	})
	defer c.close()

	c.runWithArgs([]string{"sql", "-e", "CREATE DATABASE db"})
	c.runWithArgs([]string{"sql", "-e", `CREATE TABLE db.t (
  c1 STRING PRIMARY KEY,
  c2 STRING
) PARTITION BY LIST (c1) (
  PARTITION p0 VALUES IN ('a'),
  PARTITION p1 VALUES IN (DEFAULT)
)`})
	c.runWithArgs([]string{"sql", "-e", "CREATE INDEX ON db.t (c2)"})
	c.run("zone set db.t@primary --file=./../../cli/testdata/zone_attrs.yaml")

	// Output:
	// sql -e CREATE DATABASE db
	// CREATE DATABASE
	// sql -e CREATE TABLE db.t (
	//   c1 STRING PRIMARY KEY,
	//   c2 STRING
	// ) PARTITION BY LIST (c1) (
	//   PARTITION p0 VALUES IN ('a'),
	//   PARTITION p1 VALUES IN (DEFAULT)
	// )
	// CREATE TABLE
	// sql -e CREATE INDEX ON db.t (c2)
	// CREATE INDEX
	// zone set db.t@primary --file=./../../cli/testdata/zone_attrs.yaml
	// command "set" has been removed
}
