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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package acceptance

import (
	gosql "database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/pkg/namesgenerator"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/acceptance/terrafarm"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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

	if *flagCLTWriters == -1 {
		*flagCLTWriters = *flagNodes
	}
}

var flagDuration = flag.Duration("d", cluster.DefaultDuration, "duration to run the test")
var flagNodes = flag.Int("nodes", 3, "number of nodes")
var flagStores = flag.Int("stores", 1, "number of stores to use for each node")
var flagRemote = flag.Bool("remote", false, "run the test using terrafarm instead of docker")
var flagCwd = flag.String("cwd", "../cloud/aws", "directory to run terraform from")
var flagKeyName = flag.String("key-name", "", "name of key for remote cluster")
var flagLogDir = flag.String("l", "", "the directory to store log files, relative to the test source")
var flagTestConfigs = flag.Bool("test-configs", false, "instead of using the passed in configuration, use the default "+
	"cluster configurations for each test. This overrides the nodes, stores and duration flags and will run the test "+
	"against a collection of pre-specified cluster configurations.")
var flagConfig = flag.String("config", "", "a json TestConfig proto, see testconfig.proto")

var flagPrivileged = flag.Bool("privileged", os.Getenv("CIRCLECI") != "true",
	"run containers in privileged mode (required for nemesis tests")

// Terrafarm flags.
var flagTFReuseCluster = flag.String("reuse", "",
	`attempt to use the cluster with the given name.
	  Tests which don't support this may behave unexpectedly.
	  This flag can also be set to have a test create a cluster
	  with predetermined name.`,
)

var flagTFKeepCluster = keepClusterVar(terrafarm.KeepClusterNever) // see init()
var flagTFCockroachBinary = flag.String("tf.cockroach-binary", "",
	"path to custom CockroachDB binary to use for allocator tests")
var flagTFCockroachFlags = flag.String("tf.cockroach-flags", "",
	"command-line flags to pass to cockroach for allocator tests")
var flagTFCockroachEnv = flag.String("tf.cockroach-env", "",
	"supervisor-style environment variables to pass to cockroach")
var flagTFDiskType = flag.String("tf.disk-type", "pd-standard",
	"type of disk (either 'pd-standard' for spinny disk, or 'pd-ssd' for SSD)")

// Allocator test flags.
var flagATMaxStdDev = flag.Float64("at.std-dev", 10,
	"maximum standard deviation of replica counts")

// continuousLoadTest (CLT) flags.
var flagCLTWriters = flag.Int("clt.writers", -1,
	"# of load generators to spawn (defaults to # of nodes)")
var flagCLTMinQPS = flag.Float64("clt.min-qps", 5.0,
	"fail load tests when queries per second drops below this during a health check interval")

var testFuncRE = regexp.MustCompile("^(Test|Benchmark)")

var stopper = make(chan struct{})

func runTests(m *testing.M) {
	randutil.SeedForTests()
	go func() {
		// Shut down tests when interrupted (for example CTRL+C).
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig
		select {
		case <-stopper:
		default:
			// There is a very tiny race here: the cluster might be closing
			// the stopper simultaneously.
			close(stopper)
		}
	}()
	os.Exit(m.Run())
}

// prefixRE is based on a Terraform error message regarding invalid resource
// names. We perform this check to make sure that when we prepend the name
// to the Terraform-generated resource name, we have a name that meets
// Terraform's naming rules.
//
// Here's an example of the error message:
//
// * google_compute_instance.cockroach: Error creating instance: googleapi:
//   Error 400: Invalid value for field 'resource.name':
//   'Uprep-1to3-small-cockroach-1'. Must be a match of regex
//   '(?:[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?)', invalid
var prefixRE = regexp.MustCompile("^(?:[a-z](?:[-a-z0-9]{0,45}[a-z0-9])?)$")

// getRandomName generates a random, human-readable name to ease identification
// of different test resources.
func getRandomName() string {
	// Remove characters that aren't allowed in hostnames for machines allocated
	// by Terraform.
	return strings.Replace(namesgenerator.GetRandomName(0), "_", "", -1)
}

func farmer(t *testing.T, prefix string) *terrafarm.Farmer {
	SkipUnlessRemote(t)

	if *flagKeyName == "" {
		t.Fatal("-key-name is required") // saves a lot of trouble
	}

	if e := "GOOGLE_PROJECT"; os.Getenv(e) == "" {
		t.Fatalf("%s environment variable must be set for Terraform", e)
	}

	logDir := *flagLogDir
	if logDir == "" {
		var err error
		logDir, err = ioutil.TempDir("", "clustertest_")
		if err != nil {
			t.Fatal(err)
		}
	}
	if !filepath.IsAbs(logDir) {
		logDir = filepath.Join(filepath.Clean(os.ExpandEnv("${PWD}")), logDir)
	}
	stores := "--store=/mnt/data0"
	for j := 1; j < *flagStores; j++ {
		stores += " --store=/mnt/data" + strconv.Itoa(j)
	}

	// Pass variables to be passed to the Terraform config.
	terraformVars := make(map[string]string)
	if *flagTFCockroachBinary != "" {
		terraformVars["cockroach_binary"] = *flagTFCockroachBinary
	}
	if *flagTFCockroachFlags != "" {
		terraformVars["cockroach_flags"] = *flagTFCockroachFlags
	}
	if *flagTFCockroachEnv != "" {
		terraformVars["cockroach_env"] = *flagTFCockroachEnv
	}
	terraformVars["cockroach_disk_type"] = *flagTFDiskType

	var name string
	if *flagTFReuseCluster == "" {
		// We concatenate a random name to the prefix (for Terraform resource
		// names) to allow multiple instances of the same test to run
		// concurrently. The prefix is also used as the name of the Terraform
		// state file.

		name = prefix
		if name != "" {
			name += "-"
		}

		name += getRandomName()

		// Rudimentary collision control.
		for i := 0; ; i++ {
			newName := name
			if i > 0 {
				newName += strconv.Itoa(i)
			}
			_, err := os.Stat(filepath.Join(*flagCwd, newName+".tfstate"))
			if os.IsNotExist(err) {
				name = newName
				break
			}
		}
	} else {
		name = *flagTFReuseCluster
	}

	if !prefixRE.MatchString(name) {
		t.Fatalf("generated cluster name '%s' must match regex %s", name, prefixRE)
	}
	f := &terrafarm.Farmer{
		Output:      os.Stderr,
		Cwd:         *flagCwd,
		LogDir:      logDir,
		KeyName:     *flagKeyName,
		Stores:      strconv.Quote(stores),
		Prefix:      name,
		StateFile:   name + ".tfstate",
		AddVars:     terraformVars,
		KeepCluster: flagTFKeepCluster.String(),
	}
	log.Infof(context.Background(), "logging to %s", logDir)
	return f
}

// readConfigFromFlags will convert the flags to a TestConfig for the purposes
// of starting up a cluster.
func readConfigFromFlags() cluster.TestConfig {
	return cluster.TestConfig{
		Name:     fmt.Sprintf("AdHoc %dx%d", *flagNodes, *flagStores),
		Duration: *flagDuration,
		Nodes: []cluster.NodeConfig{
			{
				Count:  int32(*flagNodes),
				Stores: []cluster.StoreConfig{{Count: int32(*flagStores)}},
			},
		},
	}
}

// getConfigs returns a list of test configs based on the passed in flags.
func getConfigs(t *testing.T) []cluster.TestConfig {
	// If a config not supplied, just read the flags.
	if (flagConfig == nil || len(*flagConfig) == 0) &&
		(flagTestConfigs == nil || !*flagTestConfigs) {
		return []cluster.TestConfig{readConfigFromFlags()}
	}

	var configs []cluster.TestConfig
	if flagTestConfigs != nil && *flagTestConfigs {
		configs = append(configs, cluster.DefaultConfigs()...)
	}

	if flagConfig != nil && len(*flagConfig) > 0 {
		// Read the passed in config from the command line.
		var config cluster.TestConfig
		if err := json.Unmarshal([]byte(*flagConfig), &config); err != nil {
			t.Error(err)
		}
		configs = append(configs, config)
	}

	// Override duration in all configs if the flags are set.
	for i := 0; i < len(configs); i++ {
		// Override values.
		if flagDuration != nil && *flagDuration != cluster.DefaultDuration {
			configs[i].Duration = *flagDuration
		}
		// Set missing defaults.
		if configs[i].Duration == 0 {
			configs[i].Duration = cluster.DefaultDuration
		}
	}
	return configs
}

type configTestRunner func(*testing.T, cluster.Cluster, cluster.TestConfig)

// runTestOnConfigs retrieves the full list of test configurations and runs the
// passed in test against each on serially.
func runTestOnConfigs(t *testing.T, testFunc func(*testing.T, cluster.Cluster, cluster.TestConfig)) {
	cfgs := getConfigs(t)
	if len(cfgs) == 0 {
		t.Fatal("no config defined so most tests won't run")
	}
	for _, cfg := range cfgs {
		func() {
			cluster := StartCluster(t, cfg)
			log.Infof(context.Background(), "cluster started successfully")
			defer cluster.AssertAndStop(t)
			testFunc(t, cluster, cfg)
		}()
	}
}

// StartCluster starts a cluster from the relevant flags. All test clusters
// should be created through this command since it sets up the logging in a
// unified way.
func StartCluster(t *testing.T, cfg cluster.TestConfig) (c cluster.Cluster) {
	var completed bool
	defer func() {
		if !completed && c != nil {
			c.AssertAndStop(t)
		}
	}()
	if *flagRemote {
		f := farmer(t, "")
		c = f
		if err := f.Resize(*flagNodes); err != nil {
			t.Fatal(err)
		}
		if err := f.WaitReady(5 * time.Minute); err != nil {
			if destroyErr := f.Destroy(t); destroyErr != nil {
				t.Fatalf("could not destroy cluster after error %s: %s", err, destroyErr)
			}
			t.Fatalf("cluster not ready in time: %s", err)
		}
	} else {
		logDir := *flagLogDir
		if logDir != "" {
			logDir = func(d string) string {
				for i := 1; i < 100; i++ {
					_, _, fun := caller.Lookup(i)
					if testFuncRE.MatchString(fun) {
						return filepath.Join(d, fun)
					}
				}
				panic("no caller matching Test(.*) in stack trace")
			}(logDir)
		}
		l := cluster.CreateLocal(cfg, logDir, *flagPrivileged, stopper)
		l.Start()
		c = l
	}
	wantedReplicas := 3
	if numNodes := c.NumNodes(); numNodes < wantedReplicas {
		wantedReplicas = numNodes
	}

	// Looks silly, but we actually start zero-node clusters in the
	// reference tests.
	if wantedReplicas > 0 {
		ctx := context.TODO()

		log.Infof(ctx, "waiting for first range to have %d replicas", wantedReplicas)

		util.SucceedsSoon(t, func() error {
			select {
			case <-stopper:
				t.Fatal("interrupted")
			case <-time.After(time.Second):
			}

			// Reconnect on every iteration; gRPC will eagerly tank the connection
			// on transport errors. Always talk to node 0 because it's guaranteed
			// to exist.
			client, dbStopper := c.NewClient(t, 0)
			defer dbStopper.Stop()

			ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			var desc roachpb.RangeDescriptor
			if err := client.GetProto(ctxWithTimeout, keys.RangeDescriptorKey(roachpb.RKeyMin), &desc); err != nil {
				return err
			}
			foundReplicas := len(desc.Replicas)

			if log.V(1) {
				log.Infof(ctxWithTimeout, "found %d replicas", foundReplicas)
			}

			if foundReplicas < wantedReplicas {
				return errors.Errorf("expected %d replicas, only found %d", wantedReplicas, foundReplicas)
			}
			return nil
		})
	}

	// Ensure that all nodes are serving SQL by making sure a simple
	// read-only query succeeds.
	for i := 0; i < c.NumNodes(); i++ {
		util.SucceedsSoon(t, func() error {
			db, err := gosql.Open("postgres", c.PGUrl(i))
			if err != nil {
				return err
			}
			if _, err := db.Exec("SHOW DATABASES;"); err != nil {
				return err
			}
			return nil
		})
	}

	completed = true
	return c
}

// SkipUnlessLocal calls t.Skip if not running against a local cluster.
func SkipUnlessLocal(t *testing.T) {
	if *flagRemote {
		t.Skip("skipping since not run against local cluster")
	}
}

// SkipUnlessRemote calls t.Skip if not running against a remote cluster.
func SkipUnlessRemote(t *testing.T) {
	if !*flagRemote {
		t.Skip("skipping since not run against remote cluster")
	}
}

// SkipUnlessLocal calls t.Skip if not running with the privileged flag.
func SkipUnlessPrivileged(t *testing.T) {
	if !*flagPrivileged {
		t.Skip("skipping since not run in privileged mode")
	}
}

func makePGClient(t *testing.T, dest string) *gosql.DB {
	db, err := gosql.Open("postgres", dest)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func defaultContainerConfig() container.Config {
	return container.Config{
		Image: postgresTestImage,
		Env: []string{
			fmt.Sprintf("PGPORT=%s", base.DefaultPort),
			"PGSSLCERT=/certs/node.crt",
			"PGSSLKEY=/certs/node.key",
		},
	}
}

// testDockerFail ensures the specified docker cmd fails.
func testDockerFail(t *testing.T, name string, cmd []string) {
	containerConfig := defaultContainerConfig()
	containerConfig.Cmd = cmd
	if err := testDockerSingleNode(t, name, containerConfig); err == nil {
		t.Error("expected failure")
	}
}

// testDockerSuccess ensures the specified docker cmd succeeds.
func testDockerSuccess(t *testing.T, name string, cmd []string) {
	containerConfig := defaultContainerConfig()
	containerConfig.Cmd = cmd
	if err := testDockerSingleNode(t, name, containerConfig); err != nil {
		t.Error(err)
	}
}

const (
	// NB: postgresTestTag is grepped for in circle-deps.sh, so don't rename it.
	postgresTestTag = "20160913-0941"
	// Iterating against a locally built version of the docker image can be done
	// by changing postgresTestImage to the hash of the container.
	postgresTestImage = "cockroachdb/postgres-test:" + postgresTestTag
)

func testDocker(t *testing.T, num int32, name string, containerConfig container.Config) error {
	SkipUnlessLocal(t)
	cfg := cluster.TestConfig{
		Name:     name,
		Duration: *flagDuration,
		Nodes:    []cluster.NodeConfig{{Count: num, Stores: []cluster.StoreConfig{{Count: 1}}}},
	}
	l := StartCluster(t, cfg).(*cluster.LocalCluster)
	defer l.AssertAndStop(t)

	if len(l.Nodes) > 0 {
		containerConfig.Env = append(containerConfig.Env, "PGHOST="+l.Hostname(0))
	}
	hostConfig := container.HostConfig{NetworkMode: "host"}
	return l.OneShot(postgresTestImage, types.ImagePullOptions{}, containerConfig, hostConfig, "docker-"+name)
}

func testDockerSingleNode(t *testing.T, name string, containerConfig container.Config) error {
	return testDocker(t, 1, name, containerConfig)
}

func testDockerOneShot(t *testing.T, name string, containerConfig container.Config) error {
	return testDocker(t, 0, name, containerConfig)
}
