// Copyright 2015 The Cockroach Authors.
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

package acceptance

import (
	"errors"
	"flag"
	"fmt"
	"go/build"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/acceptance/terrafarm"
)

type keepClusterVar string

func (kcv *keepClusterVar) String() string {
	return string(*kcv)
}

func (kcv *keepClusterVar) Set(val string) error {
	if val != terrafarm.KeepClusterAlways &&
		val != terrafarm.KeepClusterFailed &&
		val != terrafarm.KeepClusterNever {
		return errors.New("invalid value")
	}
	*kcv = keepClusterVar(val)
	return nil
}

func init() {
	flag.Var(&flagTFKeepCluster, "tf.keep-cluster",
		"keep the cluster after the test, either 'always', 'never', or 'failed'")

	flag.Parse()
}

var flagDuration = flag.Duration("d", 5*time.Second, "for duration-limited tests, how long to run them for")
var flagNodes = flag.Int("nodes", 4, "number of nodes")
var flagStores = flag.Int("stores", 1, "number of stores to use for each node")
var flagRemote = flag.Bool("remote", false, "run the test using terrafarm instead of docker")
var flagCwd = flag.String("cwd", func() string {
	aceptancePkg, err := build.Import("github.com/cockroachdb/cockroach/pkg/acceptance", "", build.FindOnly)
	if err != nil {
		panic(err)
	}
	return filepath.Join(aceptancePkg.Dir, "terraform", "azure")
}(), "directory to run terraform from")
var flagKeyName = flag.String("key-name", "", "name of key for remote cluster")
var flagLogDir = flag.String("l", "", "the directory to store log files, relative to the test source")

// Terrafarm flags.
var flagTFReuseCluster = flag.String("reuse", "",
	`attempt to use the cluster with the given name.
	  Tests which don't support this may behave unexpectedly.
	  This flag can also be set to have a test create a cluster
	  with predetermined name.`,
)
var flagTFStorageLocation = flag.String("tf.storage-location", "eastus",
	"the azure location from which to download fixtures and store ephemeral data",
)
var flagTFKeepCluster = keepClusterVar(terrafarm.KeepClusterNever) // see init()
var flagTFCockroachFlags = flag.String("tf.cockroach-flags", "",
	"command-line flags to pass to cockroach for allocator tests")
var flagTFCockroachEnv = flag.String("tf.cockroach-env", "",
	"supervisor-style environment variables to pass to cockroach")

// Allocator test flags.
var flagATMaxStdDev = flag.Float64("at.std-dev", 10,
	"maximum standard deviation of replica counts")
var flagCLTMinQPS = flag.Float64("clt.min-qps", 5.0,
	"fail load tests when queries per second drops below this during a health check interval")

// readConfigFromFlags will convert the flags to a TestConfig for the purposes
// of starting up a cluster.
func readConfigFromFlags() cluster.TestConfig {
	cfg := cluster.TestConfig{
		Name:     fmt.Sprintf("AdHoc %dx%d", *flagNodes, *flagStores),
		Duration: *flagDuration,
	}
	for i := 0; i < *flagNodes; i++ {
		cfg.Nodes = append(cfg.Nodes, cluster.NodeConfig{
			Stores: make([]cluster.StoreConfig, *flagStores),
		})
	}
	return cfg
}
