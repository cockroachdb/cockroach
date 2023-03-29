// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !race
// +build !race

package cli

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cli/democluster"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func Example_demo_locality() {
	_ = disabledExample_demo_locality
	fmt.Println("skipped due to flake: https://github.com/cockroachdb/cockroach/issues/99903")
}

func disabledExample_demo_locality() {
	c := NewCLITest(TestCLIParams{NoServer: true})
	defer c.Cleanup()

	defer democluster.TestingForceRandomizeDemoPorts()()

	// Disable multi-tenant for this test due to the unsupported gossip commands.
	testData := [][]string{
		{`demo`, `--nodes`, `3`, `--multitenant=false`, `-e`, `select node_id, locality from crdb_internal.gossip_nodes order by node_id`},
		{`demo`, `--nodes`, `9`, `--multitenant=false`, `-e`, `select node_id, locality from crdb_internal.gossip_nodes order by node_id`},
		{`demo`, `--nodes`, `3`, `--multitenant=false`, `--demo-locality=region=us-east1:region=us-east2:region=us-east3`,
			`-e`, `select node_id, locality from crdb_internal.gossip_nodes order by node_id`},
	}
	setCLIDefaultsForTests()
	// We must reset the security asset loader here, otherwise the dummy
	// asset loader that is set by default in tests will not be able to
	// find the certs that demo sets up.
	securityassets.ResetLoader()
	for _, cmd := range testData {
		// `demo` sets up a server and log file redirection, which asserts
		// that the logging subsystem has not been initialized yet.  Fake
		// this to be true.
		log.TestingResetActive()
		c.RunWithArgs(cmd)
	}

	// Output:
	// demo --nodes 3 --multitenant=false -e select node_id, locality from crdb_internal.gossip_nodes order by node_id
	// node_id	locality
	// 1	region=us-east1,az=b
	// 2	region=us-east1,az=c
	// 3	region=us-east1,az=d
	// demo --nodes 9 --multitenant=false -e select node_id, locality from crdb_internal.gossip_nodes order by node_id
	// node_id	locality
	// 1	region=us-east1,az=b
	// 2	region=us-east1,az=c
	// 3	region=us-east1,az=d
	// 4	region=us-west1,az=a
	// 5	region=us-west1,az=b
	// 6	region=us-west1,az=c
	// 7	region=europe-west1,az=b
	// 8	region=europe-west1,az=c
	// 9	region=europe-west1,az=d
	// demo --nodes 3 --multitenant=false --demo-locality=region=us-east1:region=us-east2:region=us-east3 -e select node_id, locality from crdb_internal.gossip_nodes order by node_id
	// node_id	locality
	// 1	region=us-east1
	// 2	region=us-east2
	// 3	region=us-east3
}
