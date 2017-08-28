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
	gosql "database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"go/build"
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
	// Import postgres driver.
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/pkg/acceptance/terrafarm"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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

var flagDuration = flag.Duration("d", cluster.DefaultDuration, "duration to run the test")
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
var flagConfig = flag.String("config", "", "a json TestConfig proto, see testconfig.proto")

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

var stopper = stop.NewStopper()

// GetStopper returns the stopper used by acceptance tests.
func GetStopper() *stop.Stopper {
	return stopper
}

// RunTests runs the tests in a package while gracefully handling interrupts.
func RunTests(m *testing.M) {
	randutil.SeedForTests()
	go func() {
		// Shut down tests when interrupted (for example CTRL+C).
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig
		select {
		case <-stopper.ShouldStop():
		default:
			// There is a very tiny race here: the cluster might be closing
			// the stopper simultaneously.
			stopper.Stop(context.TODO())
		}
	}()
	os.Exit(m.Run())
}

// turns someTest#123 into someTest when invoked with ReplicaAllLiteralString.
// This is useful because the go test harness automatically disambiguates
// subtests in that way when they are invoked multiple times with the same name,
// and we sometimes call RunDocker multiple times in tests.
var reStripTestEnumeration = regexp.MustCompile(`#\d+$`)

const (
	bareTest   = "runMode=local"
	dockerTest = "runMode=docker"
	farmerTest = "runMode=farmer"
)

// RunLocal runs the given acceptance test using a bare cluster.
func RunLocal(t *testing.T, testee func(t *testing.T)) {
	t.Run(bareTest, testee)
}

// RunDocker runs the given acceptance test using a Docker cluster.
func RunDocker(t *testing.T, testee func(t *testing.T)) {
	t.Run(dockerTest, testee)
}

// RunTerraform runs the given acceptance test using a terraform cluster.
func RunTerraform(t *testing.T, testee func(t *testing.T)) {
	t.Run(farmerTest, testee)
}

var _ = RunTerraform // silence unused warning

// EphemeralStorageAccount returns the name of the storage account to use to
// store data that should be periodically purged. It returns a storage account
// in the region specified by the -tf.storage-location flag to avoid bandwidth
// egress charges.
//
// See /docs/CLOUD-RESOURCES.md for details.
func EphemeralStorageAccount() string {
	return "roachephemeral" + *flagTFStorageLocation
}

// FixtureStorageAccount returns the name of the storage account that contains
// permanent test data ("test fixtures"). It returns a storage account in the
// region specified by the -tf.storage-location flag to avoid bandwidth egress
// charges.
//
// See /docs/CLOUD-RESOURCES.md for details.
func FixtureStorageAccount() string {
	return "roachfixtures" + *flagTFStorageLocation
}

// FixtureURL returns the public URL at which the fixture with the given name
// can be downloaded from Azure Cloud Storage. Like FixtureStorageAccount(), it
// takes the -tf.storage-location flag into account.
func FixtureURL(name string) string {
	return fmt.Sprintf("https://%s.blob.core.windows.net/%s", FixtureStorageAccount(), name)
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

// MakeFarmer creates a terrafarm farmer for use in acceptance tests.
func MakeFarmer(t testing.TB, prefix string, stopper *stop.Stopper) *terrafarm.Farmer {
	SkipUnlessRemote(t)

	if *flagKeyName == "" {
		t.Fatal("-key-name is required") // saves a lot of trouble
	}

	switch {
	case strings.Contains(*flagCwd, "azure"):
		for _, e := range []string{"ARM_SUBSCRIPTION_ID", "ARM_CLIENT_ID", "ARM_CLIENT_SECRET", "ARM_TENANT_ID"} {
			if _, ok := os.LookupEnv(e); !ok {
				t.Errorf("%s environment variable must be set for Azure", e)
			}
		}
	case strings.Contains(*flagCwd, "gce"):
		project := []string{"GOOGLE_PROJECT", "GCLOUD_PROJECT", "CLOUDSDK_CORE_PROJECT"}

		found := false
		for _, e := range project {
			if _, ok := os.LookupEnv(e); ok {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("one of %+v environment variables must be set for GCE", project)
		}
	}
	if t.Failed() {
		t.FailNow()
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
		pwd, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		logDir = filepath.Join(pwd, logDir)
	}
	var stores string
	for j := 0; j < *flagStores; j++ {
		stores += " --store=/mnt/data" + strconv.Itoa(j)
	}

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

	// We need to configure a MaxOffset on this clock so that the rpc.Context will
	// enforce the offset. We're going to initialize the client.Txn using the
	// Context's clock and send them through the ExternalSender, so the client's
	// clock needs to be synchronized.
	//
	// TODO(andrei): It's unfortunate that this client, which is not part of the
	// cluster, needs to do offset checks. Also, we igore the env variable that
	// may control a different acceptable offset for the nodes in the cluster. We
	// should stop creating transaction outside of the cluster.
	clientClock := hlc.NewClock(hlc.UnixNano, base.DefaultMaxClockOffset)
	rpcContext := rpc.NewContext(log.AmbientContext{Tracer: tracing.NewTracer()}, &base.Config{
		Insecure: true,
		User:     security.NodeUser,
		// Set a bogus address, to be used by the clock skew checks as the ID of
		// this "node". We can't leave it blank.
		Addr: "acceptance test client",
	}, clientClock, stopper)
	rpcContext.HeartbeatCB = func() {
		if err := rpcContext.RemoteClocks.VerifyClockOffset(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	// Disable update checks for test clusters by setting the required
	// environment variable.
	cockroachEnv := "COCKROACH_SKIP_UPDATE_CHECK=1"
	if len(*flagTFCockroachEnv) > 0 {
		cockroachEnv += " " + strings.Join(strings.Split(*flagTFCockroachEnv, ","), " ")
	}

	f := &terrafarm.Farmer{
		Output:          os.Stderr,
		Cwd:             *flagCwd,
		LogDir:          logDir,
		KeyName:         *flagKeyName,
		CockroachBinary: *cluster.CockroachBinary,
		CockroachFlags:  stores + " " + *flagTFCockroachFlags,
		CockroachEnv:    cockroachEnv,
		Prefix:          name,
		StateFile:       name + ".tfstate",
		KeepCluster:     flagTFKeepCluster.String(),
		RPCContext:      rpcContext,
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
	if flagConfig == nil || len(*flagConfig) == 0 {
		return []cluster.TestConfig{readConfigFromFlags()}
	}

	var configs []cluster.TestConfig
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

// runTestOnConfigs retrieves the full list of test configurations and runs the
// passed in test against each on serially. If any options are specified, they may mutate
// the test config before it runs.
func runTestOnConfigs(
	t *testing.T,
	testFunc func(context.Context, *testing.T, cluster.Cluster, cluster.TestConfig),
	options ...func(*cluster.TestConfig),
) {
	cfgs := getConfigs(t)
	if len(cfgs) == 0 {
		t.Fatal("no config defined so most tests won't run")
	}
	ctx := context.Background()
	for _, cfg := range cfgs {
		for _, opt := range options {
			opt(&cfg)
		}
		func() {
			cluster := StartCluster(ctx, t, cfg)
			log.Infof(ctx, "cluster started successfully")
			defer cluster.AssertAndStop(ctx, t)
			testFunc(ctx, t, cluster, cfg)
		}()
	}
}

// StartCluster starts a cluster from the relevant flags. All test clusters
// should be created through this command since it sets up the logging in a
// unified way.
func StartCluster(ctx context.Context, t *testing.T, cfg cluster.TestConfig) (c cluster.Cluster) {
	var completed bool
	defer func() {
		if !completed && c != nil {
			c.AssertAndStop(ctx, t)
		}
	}()

	if *flagRemote { // force the test remote, no matter what run mode we think it should be run in
		f := MakeFarmer(t, "", stopper)
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
		parts := strings.Split(t.Name(), "/")
		if len(parts) < 2 {
			t.Fatal("must invoke RunLocal, RunDocker, or RunFarmer")
		}

		var runMode string
		for _, part := range parts[1:] {
			part = reStripTestEnumeration.ReplaceAllLiteralString(part, "")
			switch part {
			case bareTest:
				fallthrough
			case dockerTest:
				fallthrough
			case farmerTest:
				if runMode != "" {
					t.Fatalf("test has more than one run mode: %s and %s", runMode, part)
				}
				runMode = part
			}
		}

		switch runMode {
		case bareTest:
			pwd, err := os.Getwd()
			if err != nil {
				t.Fatal(err)
			}
			dataDir, err := ioutil.TempDir(pwd, ".localcluster")
			if err != nil {
				t.Fatal(err)
			}

			clusterCfg := localcluster.ClusterConfig{
				Ephemeral: true,
				DataDir:   dataDir,
				NumNodes:  int(cfg.Nodes[0].Count),
			}
			l := localcluster.New(clusterCfg)

			l.Start()
			c = &localcluster.LocalCluster{Cluster: l}

		case dockerTest:
			logDir := *flagLogDir
			if logDir != "" {
				logDir = filepath.Join(logDir, filepath.Clean(t.Name()))
			}
			l := cluster.CreateLocal(ctx, cfg, logDir, stopper)
			l.Start(ctx)
			c = l

		default:
			t.Fatalf("unable to run in mode %q, use either RunLocal, RunDocker, or RunFarmer", runMode)
		}
	}

	if cfg.InitMode != cluster.INIT_NONE {
		wantedReplicas := 3
		if numNodes := c.NumNodes(); numNodes < wantedReplicas {
			wantedReplicas = numNodes
		}

		// Looks silly, but we actually start zero-node clusters in the
		// reference tests.
		if wantedReplicas > 0 {

			log.Infof(ctx, "waiting for first range to have %d replicas", wantedReplicas)

			testutils.SucceedsSoon(t, func() error {
				select {
				case <-stopper.ShouldStop():
					t.Fatal("interrupted")
				case <-time.After(time.Second):
				}

				// Reconnect on every iteration; gRPC will eagerly tank the connection
				// on transport errors. Always talk to node 0 because it's guaranteed
				// to exist.
				client, err := c.NewClient(ctx, 0)
				if err != nil {
					t.Fatal(err)
				}

				var desc roachpb.RangeDescriptor
				if err := client.GetProto(ctx, keys.RangeDescriptorKey(roachpb.RKeyMin), &desc); err != nil {
					return err
				}
				foundReplicas := len(desc.Replicas)

				if log.V(1) {
					log.Infof(ctx, "found %d replicas", foundReplicas)
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
			testutils.SucceedsSoon(t, func() error {
				db, err := gosql.Open("postgres", c.PGUrl(ctx, i))
				if err != nil {
					return err
				}
				if _, err := db.Exec("SHOW DATABASES;"); err != nil {
					return err
				}
				return nil
			})
		}
	}

	completed = true
	return c
}

// SkipUnlessRemote calls t.Skip if not running against a remote cluster.
func SkipUnlessRemote(t testing.TB) {
	if !*flagRemote {
		t.Skip("skipping since not run against remote cluster")
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
func testDockerFail(ctx context.Context, t *testing.T, name string, cmd []string) {
	containerConfig := defaultContainerConfig()
	containerConfig.Cmd = cmd
	if err := testDockerSingleNode(ctx, t, name, containerConfig); err == nil {
		t.Error("expected failure")
	}
}

// testDockerSuccess ensures the specified docker cmd succeeds.
func testDockerSuccess(ctx context.Context, t *testing.T, name string, cmd []string) {
	containerConfig := defaultContainerConfig()
	containerConfig.Cmd = cmd
	if err := testDockerSingleNode(ctx, t, name, containerConfig); err != nil {
		t.Error(err)
	}
}

const (
	// Iterating against a locally built version of the docker image can be done
	// by changing postgresTestImage to the hash of the container.
	postgresTestImage = "docker.io/cockroachdb/postgres-test:20170423-1100"
)

func testDocker(
	ctx context.Context, t *testing.T, num int32, name string, containerConfig container.Config,
) error {
	var err error
	RunDocker(t, func(t *testing.T) {
		cfg := cluster.TestConfig{
			Name:     name,
			Duration: *flagDuration,
			Nodes:    []cluster.NodeConfig{{Count: num, Stores: []cluster.StoreConfig{{Count: 1}}}},
		}
		l := StartCluster(ctx, t, cfg).(*cluster.DockerCluster)
		defer l.AssertAndStop(ctx, t)

		if len(l.Nodes) > 0 {
			containerConfig.Env = append(containerConfig.Env, "PGHOST="+l.Hostname(0))
		}
		hostConfig := container.HostConfig{NetworkMode: "host"}
		err = l.OneShot(
			ctx, postgresTestImage, types.ImagePullOptions{}, containerConfig, hostConfig, "docker-"+name,
		)
		if err == nil {
			// Clean up the log files if the run was successful.
			l.Cleanup(ctx)
		}
	})
	return err
}

func testDockerSingleNode(
	ctx context.Context, t *testing.T, name string, containerConfig container.Config,
) error {
	return testDocker(ctx, t, 1, name, containerConfig)
}

func testDockerOneShot(
	ctx context.Context, t *testing.T, name string, containerConfig container.Config,
) error {
	return testDocker(ctx, t, 0, name, containerConfig)
}

// CheckGossipFunc is the type of callback used in CheckGossip.
type CheckGossipFunc func(map[string]gossip.Info) error

// CheckGossip fetches the gossip infoStore from each node and invokes the given
// function. The test passes if the function returns 0 for every node,
// retrying for up to the given duration.
func CheckGossip(ctx context.Context, c cluster.Cluster, d time.Duration, f CheckGossipFunc) error {
	return errors.Wrapf(util.RetryForDuration(d, func() error {
		var infoStatus gossip.InfoStatus
		for i := 0; i < c.NumNodes(); i++ {
			if err := httputil.GetJSON(cluster.HTTPClient, c.URL(ctx, i)+"/_status/gossip/local", &infoStatus); err != nil {
				return errors.Wrapf(err, "failed to get gossip status from node %d", i)
			}
			if err := f(infoStatus.Infos); err != nil {
				return errors.Wrapf(err, "node %d", i)
			}
		}

		return nil
	}), "condition failed to evaluate within %s", d)
}

// HasPeers returns a CheckGossipFunc that passes when the given
// number of peers are connected via gossip.
func HasPeers(expected int) CheckGossipFunc {
	return func(infos map[string]gossip.Info) error {
		count := 0
		for k := range infos {
			if strings.HasPrefix(k, "node:") {
				count++
			}
		}
		if count != expected {
			return errors.Errorf("expected %d peers, found %d", expected, count)
		}
		return nil
	}
}

// hasSentinel is a checkGossipFunc that passes when the sentinel gossip is present.
func hasSentinel(infos map[string]gossip.Info) error {
	if _, ok := infos[gossip.KeySentinel]; !ok {
		return errors.Errorf("sentinel not found")
	}
	return nil
}

// hasClusterID is a checkGossipFunc that passes when the cluster ID gossip is present.
func hasClusterID(infos map[string]gossip.Info) error {
	if _, ok := infos[gossip.KeyClusterID]; !ok {
		return errors.Errorf("cluster ID not found")
	}
	return nil
}
