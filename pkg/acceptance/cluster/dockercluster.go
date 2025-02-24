// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cluster

import (
	"bytes"
	"context"
	"crypto/rand"
	gosql "database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/containerd/containerd/platforms"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	specs "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	// We use a docker image mirror to avoid pulling from 3rd party repos, which sometimes have reliability issues.
	// See https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/3462594561/Docker+image+sync for the details.
	defaultImage  = "us-east1-docker.pkg.dev/crl-docker-sync/docker-io/library/ubuntu:focal-20210119"
	networkPrefix = "cockroachdb_acceptance"
)

// DefaultTCP is the default SQL/RPC port specification.
const (
	DefaultTCP  nat.Port = base.DefaultPort + "/tcp"
	defaultHTTP nat.Port = base.DefaultHTTPPort + "/tcp"
)

// CockroachBinaryInContainer is the container-side path to the CockroachDB
// binary.
const CockroachBinaryInContainer = "/cockroach/cockroach"

var (
	cockroachImage = flag.String("i", defaultImage, "the docker image to run")
	cockroachEntry = flag.String("e", "", "the entry point for the image")
	waitOnStop     = flag.Bool("w", false, "wait for the user to interrupt before tearing down the cluster")
	maxRangeBytes  = *zonepb.DefaultZoneConfig().RangeMaxBytes
)

// CockroachBinary is the path to the host-side binary to use.
var CockroachBinary = flag.String("b", "", "the host-side binary to run")

func exists(path string) bool {
	if _, err := os.Stat(path); oserror.IsNotExist(err) {
		return false
	}
	return true
}

func nodeStr(l *DockerCluster, i int) string {
	return fmt.Sprintf("roach-%s-%d", l.clusterID, i)
}

func storeStr(node, store int) string {
	return fmt.Sprintf("data%d.%d", node, store)
}

// The various event types.
const (
	eventDie     = "die"
	eventRestart = "restart"
)

// Event for a node containing a node index and the type of event.
type Event struct {
	NodeIndex int
	Status    string
}

type testStore struct {
	index  int
	dir    string
	config StoreConfig
}

type testNode struct {
	*Container
	index   int
	nodeStr string
	config  NodeConfig
	stores  []testStore
}

// DockerCluster manages a local cockroach cluster running on docker. The
// cluster is composed of a "volumes" container which manages the
// persistent volumes used for certs and node data and N cockroach nodes.
type DockerCluster struct {
	client               client.APIClient
	mu                   syncutil.Mutex // Protects the fields below
	vols                 *Container
	config               TestConfig
	Nodes                []*testNode
	events               chan Event
	expectedEvents       chan Event
	oneshot              *Container
	stopper              *stop.Stopper
	monitorCtx           context.Context
	monitorCtxCancelFunc func()
	monitorDone          chan struct{}
	clusterID            string
	networkID            string
	networkName          string
	// Careful! volumesDir will be emptied during cluster cleanup.
	volumesDir string
}

// CreateDocker creates a Docker-based cockroach cluster. The stopper is used to
// gracefully shutdown the channel (e.g. when a signal arrives). The cluster
// must be started before being used and keeps container volumes in the
// specified volumesDir, including logs and cockroach stores. If volumesDir is
// empty, a temporary directory is created.
func CreateDocker(
	ctx context.Context, cfg TestConfig, volumesDir string, stopper *stop.Stopper,
) *DockerCluster {
	select {
	case <-stopper.ShouldQuiesce():
		// The stopper was already closed, exit early.
		os.Exit(1)
	default:
	}

	if *cockroachImage == defaultImage && !exists(*CockroachBinary) {
		log.Fatalf(ctx, "\"%s\": does not exist", *CockroachBinary)
	}

	cli, err := client.NewClientWithOpts(client.FromEnv)
	maybePanic(err)

	cli.NegotiateAPIVersion(ctx)

	clusterID := uuid.MakeV4()
	clusterIDS := clusterID.Short().String()

	if volumesDir == "" {
		volumesDir, err = os.MkdirTemp(datapathutils.DebuggableTempDir(), fmt.Sprintf("cockroach-acceptance-%s", clusterIDS))
		maybePanic(err)
	} else {
		volumesDir = filepath.Join(volumesDir, clusterIDS)
	}
	if !filepath.IsAbs(volumesDir) {
		pwd, err := os.Getwd()
		maybePanic(err)
		volumesDir = filepath.Join(pwd, volumesDir)
	}
	maybePanic(os.MkdirAll(volumesDir, 0755))
	log.Infof(ctx, "cluster volume directory: %s", volumesDir)

	return &DockerCluster{
		clusterID: clusterIDS,
		client:    resilientDockerClient{APIClient: cli},
		config:    cfg,
		stopper:   stopper,
		// TODO(tschottdorf): deadlocks will occur if these channels fill up.
		events:         make(chan Event, 1000),
		expectedEvents: make(chan Event, 1000),
		volumesDir:     volumesDir,
	}
}

func (l *DockerCluster) expectEvent(c *Container, msgs ...string) {
	for index, ctr := range l.Nodes {
		if ctr.Container == nil || c.id != ctr.id {
			continue
		}
		for _, status := range msgs {
			select {
			case l.expectedEvents <- Event{NodeIndex: index, Status: status}:
			default:
				panic("expectedEvents channel filled up")
			}
		}
		break
	}
}

// OneShot runs a container, expecting it to successfully run to completion
// and die, after which it is removed. Not goroutine safe: only one OneShot
// can be running at once.
// Adds the same binds as the cluster containers (certs, binary, etc).
func (l *DockerCluster) OneShot(
	ctx context.Context,
	ref string,
	ipo types.ImagePullOptions,
	containerConfig container.Config,
	hostConfig container.HostConfig,
	platformSpec specs.Platform,
	name string,
) error {
	if err := pullImage(ctx, l, ref, ipo); err != nil {
		return err
	}
	hostConfig.VolumesFrom = []string{l.vols.id}
	c, err := createContainer(ctx, l, containerConfig, hostConfig, platformSpec, name)
	if err != nil {
		return err
	}
	l.oneshot = c
	defer func() {
		if err := l.oneshot.Remove(ctx); err != nil {
			log.Errorf(ctx, "ContainerRemove: %s", err)
		}
		l.oneshot = nil
	}()

	if err := l.oneshot.Start(ctx); err != nil {
		return err
	}
	return l.oneshot.WaitUntilNotRunning(ctx)
}

// stopOnPanic is invoked as a deferred function in Start in order to attempt
// to tear down the cluster if a panic occurs while starting it. If the panic
// was initiated by the stopper being closed (which panicOnStop notices) then
// the process is exited with a failure code.
func (l *DockerCluster) stopOnPanic(ctx context.Context) {
	if r := recover(); r != nil {
		l.stop(ctx)
		if r != l {
			panic(r)
		}
		os.Exit(1)
	}
}

// panicOnStop tests whether the stopper has been closed and panics if
// it has. This allows polling for whether to stop and avoids nasty locking
// complications with trying to call Stop at arbitrary points such as in the
// middle of creating a container.
func (l *DockerCluster) panicOnStop() {
	if l.stopper == nil {
		panic(l)
	}

	select {
	case <-l.stopper.IsStopped():
		l.stopper = nil
		panic(l)
	default:
	}
}

func (l *DockerCluster) createNetwork(ctx context.Context) {
	l.panicOnStop()

	l.networkName = fmt.Sprintf("%s-%s", networkPrefix, l.clusterID)
	log.Infof(ctx, "creating docker network with name: %s", l.networkName)
	net, err := l.client.NetworkInspect(ctx, l.networkName, types.NetworkInspectOptions{})
	if err == nil {
		// We need to destroy the network and any running containers inside of it.
		for containerID := range net.Containers {
			// This call could fail if the container terminated on its own after we call
			// NetworkInspect, but the likelihood of this seems low. If this line creates
			// a lot of panics we should do more careful error checking.
			maybePanic(l.client.ContainerKill(ctx, containerID, "9"))
		}
		maybePanic(l.client.NetworkRemove(ctx, l.networkName))
	} else if !client.IsErrNotFound(err) {
		panic(err)
	}

	resp, err := l.client.NetworkCreate(ctx, l.networkName, types.NetworkCreate{
		Driver: "bridge",
		// Docker gets very confused if two networks have the same name.
		CheckDuplicate: true,
	})
	maybePanic(err)
	if resp.Warning != "" {
		log.Warningf(ctx, "creating network: %s", resp.Warning)
	}
	l.networkID = resp.ID
}

// create the volumes container that keeps all of the volumes used by
// the cluster.
func (l *DockerCluster) initCluster(ctx context.Context) {
	configJSON, err := json.Marshal(l.config)
	maybePanic(err)
	log.Infof(ctx, "Initializing Cluster %s:\n%s", l.config.Name, configJSON)
	l.panicOnStop()

	pwd, err := os.Getwd()
	maybePanic(err)

	// Boot2docker's handling of binding local directories into the container is
	// very confusing. If the directory being bound has a parent directory that
	// exists in the boot2docker VM then that directory is bound into the
	// container. In particular, that means that binds of /tmp and /var will be
	// problematic.
	binds := []string{
		filepath.Join(pwd, certsDir) + ":/certs",
		filepath.Join(pwd, "..") + ":/go/src/github.com/cockroachdb/cockroach",
		filepath.Join(l.volumesDir, "logs") + ":/logs",
	}

	if *cockroachImage == defaultImage {
		path, err := filepath.Abs(*CockroachBinary)
		maybePanic(err)
		binds = append(binds, path+":"+CockroachBinaryInContainer)
	}

	l.Nodes = []*testNode{}
	// Expand the cluster configuration into nodes and stores per node.
	for i, nc := range l.config.Nodes {
		newTestNode := &testNode{
			config:  nc,
			index:   i,
			nodeStr: nodeStr(l, i),
		}
		for j, sc := range nc.Stores {
			hostDir := filepath.Join(l.volumesDir, storeStr(i, j))
			containerDir := "/" + storeStr(i, j)
			binds = append(binds, hostDir+":"+containerDir)
			newTestNode.stores = append(newTestNode.stores,
				testStore{
					config: sc,
					index:  j,
					dir:    containerDir,
				})
		}
		l.Nodes = append(l.Nodes, newTestNode)
	}

	if *cockroachImage == defaultImage {
		maybePanic(pullImage(ctx, l, defaultImage, types.ImagePullOptions{}))
	}
	c, err := createContainer(
		ctx,
		l,
		container.Config{
			Image:      *cockroachImage,
			Entrypoint: []string{"/bin/true"},
		}, container.HostConfig{
			Binds:           binds,
			PublishAllPorts: true,
		},
		platforms.DefaultSpec(),
		fmt.Sprintf("volumes-%s", l.clusterID),
	)
	maybePanic(err)
	// Make sure this assignment to l.vols is before the calls to Start and Wait.
	// Otherwise, if they trigger maybePanic, this container won't get cleaned up
	// and it'll get in the way of future runs.
	l.vols = c
	maybePanic(c.Start(ctx))
	maybePanic(c.WaitUntilNotRunning(ctx))
}

// cockroachEntryPoint returns the value to be used as
// container.Config.Entrypoint for a container running the cockroach
// binary under test.
// TODO(bdarnell): refactor this to minimize globals
func cockroachEntrypoint() []string {
	var entrypoint []string
	if *cockroachImage == defaultImage {
		entrypoint = append(entrypoint, CockroachBinaryInContainer)
	} else if *cockroachEntry != "" {
		entrypoint = append(entrypoint, *cockroachEntry)
	}
	return entrypoint
}

// createRoach creates the docker container for a testNode. It may be called in
// parallel to start many nodes at once, and thus should remain threadsafe.
func (l *DockerCluster) createRoach(
	ctx context.Context, node *testNode, vols *Container, env []string, cmd ...string,
) {
	l.panicOnStop()

	hostConfig := container.HostConfig{
		PublishAllPorts: true,
		NetworkMode:     container.NetworkMode(l.networkID),
	}

	if vols != nil {
		hostConfig.VolumesFrom = append(hostConfig.VolumesFrom, vols.id)
	}

	var hostname string
	if node.index >= 0 {
		hostname = fmt.Sprintf("roach-%s-%d", l.clusterID, node.index)
	}
	log.Infof(ctx, "creating docker container with name: %s", hostname)
	var err error
	node.Container, err = createContainer(
		ctx,
		l,
		container.Config{
			Hostname: hostname,
			Image:    *cockroachImage,
			ExposedPorts: map[nat.Port]struct{}{
				DefaultTCP:  {},
				defaultHTTP: {},
			},
			Entrypoint: cockroachEntrypoint(),
			Env:        env,
			Cmd:        cmd,
			Labels: map[string]string{
				// Allow for `docker ps --filter label=Hostname=roach-<id>-0` or `--filter label=Roach`.
				"Hostname":              hostname,
				"Roach":                 "",
				"Acceptance-cluster-id": l.clusterID,
			},
		},
		hostConfig,
		platforms.DefaultSpec(),
		node.nodeStr,
	)
	maybePanic(err)
}

func (l *DockerCluster) createNodeCerts() {
	// TODO(mjibson): Including "cockroach" here to mimic
	// GenerateCerts. Currently when this function is called it overwrites
	// the node.crt and node.key files generated by that function. Until
	// this is correctly fixed, make the certs generated here at least the
	// same as those.
	nodes := []string{"localhost", "cockroach", dockerIP().String()}
	for _, node := range l.Nodes {
		nodes = append(nodes, node.nodeStr)
	}
	maybePanic(security.CreateNodePair(
		certsDir,
		filepath.Join(certsDir, certnames.EmbeddedCAKey),
		keyLen, 48*time.Hour, true /* overwrite */, nodes))
}

// startNode starts a Docker container to run testNode. It may be called in
// parallel to start many nodes at once, and thus should remain threadsafe.
func (l *DockerCluster) startNode(ctx context.Context, node *testNode, singleNode bool) {
	startCmd := "start"
	if singleNode {
		startCmd = "start-single-node"
	}
	cmd := []string{
		startCmd,
		"--certs-dir=/certs/",
		"--listen-addr=" + node.nodeStr,
		"--vmodule=*=1",
	}

	// Forward the vmodule flag to the nodes.
	vmoduleFlag := flag.Lookup(logflags.VModuleName)
	if vmoduleFlag.Value.String() != "" {
		cmd = append(cmd, fmt.Sprintf("--%s=%s", vmoduleFlag.Name, vmoduleFlag.Value.String()))
	}

	for _, store := range node.stores {
		storeSpec := base.StoreSpec{
			Path: store.dir,
			Size: storagepb.SizeSpec{Capacity: int64(store.config.MaxRanges) * maxRangeBytes},
		}
		cmd = append(cmd, fmt.Sprintf("--store=%s", storeSpec))
	}
	// Append --join flag for all nodes.
	if !singleNode {
		firstNodeAddr := l.Nodes[0].nodeStr
		cmd = append(cmd, "--join="+net.JoinHostPort(firstNodeAddr, base.DefaultPort))
	}

	dockerLogDir := "/logs/" + node.nodeStr
	localLogDir := filepath.Join(l.volumesDir, "logs", node.nodeStr)
	cmd = append(
		cmd,
		"--logtostderr=ERROR",
		"--log-dir="+dockerLogDir)
	env := []string{
		"COCKROACH_SCAN_MAX_IDLE_TIME=200ms",
		"COCKROACH_SKIP_UPDATE_CHECK=1",
		"COCKROACH_CRASH_REPORTS=",
		// Needed for backward-compat on crdb_internal.ranges{_no_leases}.
		// Remove in v23.2.
		"COCKROACH_FORCE_DEPRECATED_SHOW_RANGE_BEHAVIOR=false",
		// Disable metamorphic testing for acceptance tests, since they are
		// end-to-end tests and metamorphic constants can make them too slow.
		"COCKROACH_INTERNAL_DISABLE_METAMORPHIC_TESTING=true",
	}
	l.createRoach(ctx, node, l.vols, env, cmd...)
	maybePanic(node.Start(ctx))
	httpAddr := node.Addr(ctx, defaultHTTP)

	log.Infof(ctx, `*** started %[1]s ***
  ui:        %[2]s
  trace:     %[2]s/debug/requests
  logs:      %[3]s/cockroach.INFO
  pprof:     docker exec -it %[4]s pprof https+insecure://$(hostname):%[5]s/debug/pprof/heap
  cockroach: %[6]s

  cli-env:   COCKROACH_INSECURE=false COCKROACH_CERTS_DIR=%[7]s COCKROACH_HOST=%s:%d`,
		node.Name(), "https://"+httpAddr.String(), localLogDir, node.Container.id[:5],
		base.DefaultHTTPPort, cmd, certsDir, httpAddr.IP, httpAddr.Port)
}

// RunInitCommand runs the `cockroach init` command. Normally called
// automatically, but exposed for tests that use INIT_NONE. nodeIdx
// may designate any node in the cluster as the target of the command.
func (l *DockerCluster) RunInitCommand(ctx context.Context, nodeIdx int) {
	// Add a randomID to the container name to avoid overlap between tests running on
	// different shards.
	nBig, err := rand.Int(rand.Reader, big.NewInt(10000000))
	if err != nil {
		panic(err)
	}
	randomID := strconv.Itoa(int(nBig.Int64()))
	containerConfig := container.Config{
		Image:      *cockroachImage,
		Entrypoint: cockroachEntrypoint(),
		Cmd: []string{
			"init",
			"--certs-dir=/certs/",
			"--host=" + l.Nodes[nodeIdx].nodeStr,
			"--log-dir=/logs/init-command-" + randomID,
			"--logtostderr=NONE",
		},
	}

	log.Infof(ctx, "trying to initialize via %v", containerConfig.Cmd)
	maybePanic(l.OneShot(ctx, defaultImage, types.ImagePullOptions{},
		containerConfig, container.HostConfig{}, platforms.DefaultSpec(), "init-command-"+randomID))
	log.Info(ctx, "cluster successfully initialized")
}

// returns false is the event
func (l *DockerCluster) processEvent(ctx context.Context, event events.Message) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Logging everything we get from Docker in service of finding the root
	// cause of #58955.
	log.Infof(ctx, "processing event from Docker: %+v", event)

	// If there's currently a oneshot container, ignore any die messages from
	// it because those are expected.
	if l.oneshot != nil && event.ID == l.oneshot.id && event.Status == eventDie {
		log.Infof(ctx, "Docker event was: the oneshot container terminated")
		return true
	}

	for i, n := range l.Nodes {
		if n != nil && n.id == event.ID {
			if log.V(1) {
				log.Errorf(ctx, "node=%d status=%s", i, event.Status)
			}
			select {
			case l.events <- Event{NodeIndex: i, Status: event.Status}:
			default:
				panic("events channel filled up")
			}
			return true
		}
	}

	log.Infof(ctx, "received docker event for unrecognized container: %+v",
		event)

	// An event on any other container is unexpected. Die.
	select {
	case <-l.stopper.ShouldQuiesce():
	case <-l.monitorCtx.Done():
	default:
		// There is a very tiny race here: the signal handler might be closing the
		// stopper simultaneously.
		log.Errorf(ctx, "stopping due to unexpected event: %+v", event)
		if rc, err := l.client.ContainerLogs(context.Background(), event.Actor.ID, types.ContainerLogsOptions{
			ShowStdout: true,
			ShowStderr: true,
		}); err == nil {
			defer rc.Close()
			if _, err := io.Copy(os.Stderr, rc); err != nil {
				log.Infof(ctx, "error listing logs: %s", err)
			}
		}
	}
	return false
}

func (l *DockerCluster) monitor(ctx context.Context, monitorDone chan struct{}) {
	defer close(monitorDone)

	if log.V(1) {
		log.Infof(ctx, "events monitor starts")
		defer log.Infof(ctx, "events monitor exits")
	}
	longPoll := func() bool {
		// If our context was canceled, it's time to go home.
		if l.monitorCtx.Err() != nil {
			return false
		}

		eventq, errq := l.client.Events(l.monitorCtx, types.EventsOptions{
			Filters: filters.NewArgs(
				filters.Arg("label", "Acceptance-cluster-id="+l.clusterID),
			),
		})
		for {
			select {
			case <-l.monitorCtx.Done():
				log.Infof(ctx, "monitor shutting down")
				return false
			case err := <-errq:
				log.Infof(ctx, "event stream done, resetting...: %s", err)
				// Sometimes we get a random string-wrapped EOF error back.
				// Hard to assert on, so we just let this goroutine spin.
				return true
			case event := <-eventq:
				// Currently, the only events generated (and asserted against) are "die"
				// and "restart", to maximize compatibility across different versions of
				// Docker.
				switch event.Status {
				case eventDie, eventRestart:
					if !l.processEvent(ctx, event) {
						return false
					}
				}
			}
		}
	}

	for longPoll() {
	}
}

// Start starts the cluster.
func (l *DockerCluster) Start(ctx context.Context) {
	defer l.stopOnPanic(ctx)

	l.mu.Lock()
	defer l.mu.Unlock()

	l.createNetwork(ctx)
	l.initCluster(ctx)
	log.Infof(ctx, "creating node certs (%dbit) in: %s", keyLen, certsDir)
	l.createNodeCerts()

	log.Infof(ctx, "starting %d nodes", len(l.Nodes))
	l.monitorCtx, l.monitorCtxCancelFunc = context.WithCancel(context.Background())
	l.monitorDone = make(chan struct{})
	go l.monitor(ctx, l.monitorDone)
	var wg sync.WaitGroup
	wg.Add(len(l.Nodes))
	singleNode := len(l.Nodes) == 1
	for _, node := range l.Nodes {
		go func(node *testNode) {
			defer wg.Done()
			l.startNode(ctx, node, singleNode)
		}(node)
	}
	wg.Wait()

	if l.config.InitMode == INIT_COMMAND && len(l.Nodes) > 1 {
		l.RunInitCommand(ctx, 0)
	}
}

// Assert drains the Events channel and compares the actual events with those
// expected to have been generated by the operations performed on the nodes in
// the cluster (restart, kill, ...). In the event of a mismatch, the passed
// Tester receives a fatal error.
func (l *DockerCluster) Assert(ctx context.Context, t testing.TB) {
	filter := func(ch chan Event, wait time.Duration) *Event {
		select {
		case act := <-ch:
			return &act
		case <-time.After(wait):
		}
		return nil
	}

	var events []Event
	for {
		// The expected event channel is buffered and should contain
		// all expected events already.
		const almostZero = 15 * time.Millisecond
		exp := filter(l.expectedEvents, almostZero)
		if exp == nil {
			break
		}
		t.Logf("expecting event: %v", exp)
		// l.events is connected to the docker controller and may
		// receive events more slowly.
		const waitForDockerEvent = 15 * time.Second
		act := filter(l.events, waitForDockerEvent)
		t.Logf("got event: %v", act)
		if act == nil || *exp != *act {
			t.Fatalf("expected event %v, got %v (after %v)", exp, act, events)
		}
		events = append(events, *exp)
	}
	// At the end, we leave docker a bit more time to report a final event,
	// if any.
	const waitForLastDockerEvent = 1 * time.Second
	if cur := filter(l.events, waitForLastDockerEvent); cur != nil {
		t.Fatalf("unexpected extra event %v (after %v)", cur, events)
	}
	if log.V(2) {
		log.Infof(ctx, "asserted %v", events)
	}
}

// AssertAndStop calls Assert and then stops the cluster. It is safe to stop
// the cluster multiple times.
func (l *DockerCluster) AssertAndStop(ctx context.Context, t testing.TB) {
	defer l.stop(ctx)
	l.Assert(ctx, t)
}

// stop stops the cluster.
func (l *DockerCluster) stop(ctx context.Context) {
	if *waitOnStop {
		log.Infof(ctx, "waiting for interrupt")
		<-l.stopper.ShouldQuiesce()
	}

	log.Infof(ctx, "stopping")

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.monitorCtxCancelFunc != nil {
		l.monitorCtxCancelFunc()
		l.monitorCtxCancelFunc = nil
		<-l.monitorDone
	}

	for i, n := range l.Nodes {
		if n.Container == nil {
			continue
		}
		ci, err := n.Inspect(ctx)
		crashed := err != nil || (!ci.State.Running && ci.State.ExitCode != 0)
		maybePanic(n.Kill(ctx))
		// TODO(bdarnell): make these filenames more consistent with structured
		// logs?
		file := filepath.Join(l.volumesDir, "logs", nodeStr(l, i),
			fmt.Sprintf("stderr.%s.log", strings.Replace(
				timeutil.Now().Format(time.RFC3339), ":", "_", -1)))
		maybePanic(os.MkdirAll(filepath.Dir(file), 0755))
		w, err := os.Create(file)
		maybePanic(err)
		//nolint:deferloop TODO(#137605)
		defer w.Close()
		maybePanic(n.Logs(ctx, w))
		log.Infof(ctx, "node %d: stderr at %s", i, file)
		if crashed {
			log.Infof(ctx, "~~~ node %d CRASHED ~~~~", i)
		}
		maybePanic(n.Remove(ctx))
		n.Container = nil
	}

	if l.vols != nil {
		maybePanic(l.vols.Kill(ctx))
		maybePanic(l.vols.Remove(ctx))
		l.vols = nil
	}

	if l.networkID != "" {
		maybePanic(
			l.client.NetworkRemove(ctx, l.networkID))
		l.networkID = ""
		l.networkName = ""
	}
}

// NewDB implements the Cluster interface.
func (l *DockerCluster) NewDB(ctx context.Context, i int) (*gosql.DB, error) {
	return gosql.Open("postgres", l.PGUrl(ctx, i))
}

// InternalIP returns the IP address used for inter-node communication.
func (l *DockerCluster) InternalIP(ctx context.Context, i int) net.IP {
	c := l.Nodes[i]
	containerInfo, err := c.Inspect(ctx)
	if err != nil {
		return nil
	}
	return net.ParseIP(containerInfo.NetworkSettings.Networks[l.networkName].IPAddress)
}

// PGUrl returns a URL string for the given node postgres server.
func (l *DockerCluster) PGUrl(ctx context.Context, i int) string {
	certUser := username.RootUser
	options := url.Values{}
	options.Add("sslmode", "verify-full")
	options.Add("sslcert", filepath.Join(certsDir, certnames.EmbeddedRootCert))
	options.Add("sslkey", filepath.Join(certsDir, certnames.EmbeddedRootKey))
	options.Add("sslrootcert", filepath.Join(certsDir, certnames.EmbeddedCACert))
	pgURL := url.URL{
		Scheme:   "postgres",
		User:     url.User(certUser),
		Host:     l.Nodes[i].Addr(ctx, DefaultTCP).String(),
		RawQuery: options.Encode(),
	}
	return pgURL.String()
}

// NumNodes returns the number of nodes in the cluster.
func (l *DockerCluster) NumNodes() int {
	return len(l.Nodes)
}

// Kill kills the i-th node.
func (l *DockerCluster) Kill(ctx context.Context, i int) error {
	if err := l.Nodes[i].Kill(ctx); err != nil {
		return errors.Wrapf(err, "failed to kill node %d", i)
	}
	return nil
}

// Restart restarts the given node. If the node isn't running, this starts it.
func (l *DockerCluster) Restart(ctx context.Context, i int) error {
	// The default timeout is 10 seconds.
	if err := l.Nodes[i].Restart(ctx, nil); err != nil {
		return errors.Wrapf(err, "failed to restart node %d", i)
	}
	return nil
}

// URL returns the base url.
func (l *DockerCluster) URL(ctx context.Context, i int) string {
	return "https://" + l.Nodes[i].Addr(ctx, defaultHTTP).String()
}

// Addr returns the host and port from the node in the format HOST:PORT.
func (l *DockerCluster) Addr(ctx context.Context, i int, port string) string {
	return l.Nodes[i].Addr(ctx, nat.Port(port+"/tcp")).String()
}

// Hostname implements the Cluster interface.
func (l *DockerCluster) Hostname(i int) string {
	return l.Nodes[i].nodeStr
}

// ExecCLI runs ./cockroach <args> with sane defaults.
func (l *DockerCluster) ExecCLI(ctx context.Context, i int, cmd []string) (string, string, error) {
	cmd = append([]string{CockroachBinaryInContainer}, cmd...)
	cmd = append(cmd, "--host", l.Hostname(i), "--certs-dir=/certs")
	cfg := types.ExecConfig{
		Cmd:          cmd,
		AttachStderr: true,
		AttachStdout: true,
	}
	createResp, err := l.client.ContainerExecCreate(ctx, l.Nodes[i].Container.id, cfg)
	if err != nil {
		return "", "", err
	}
	var outputStream, errorStream bytes.Buffer
	{
		resp, err := l.client.ContainerExecAttach(ctx, createResp.ID, types.ExecStartCheck{})
		if err != nil {
			return "", "", err
		}
		defer resp.Close()
		ch := make(chan error)
		go func() {
			_, err := stdcopy.StdCopy(&outputStream, &errorStream, resp.Reader)
			ch <- err
		}()
		if err := <-ch; err != nil {
			return "", "", err
		}
	}
	{
		resp, err := l.client.ContainerExecInspect(ctx, createResp.ID)
		if err != nil {
			return "", "", err
		}
		if resp.Running {
			return "", "", errors.Errorf("command still running")
		}
		if resp.ExitCode != 0 {
			o, e := outputStream.String(), errorStream.String()
			return o, e, fmt.Errorf("error executing %s:\n%s\n%s",
				cmd, o, e)
		}
	}
	return outputStream.String(), errorStream.String(), nil
}

// Cleanup removes the cluster's volumes directory, optionally preserving the
// logs directory.
func (l *DockerCluster) Cleanup(ctx context.Context, preserveLogs bool) {
	volumes, err := os.ReadDir(l.volumesDir)
	if err != nil {
		log.Warningf(ctx, "%v", err)
		return
	}
	for _, v := range volumes {
		if preserveLogs && v.Name() == "logs" {
			log.Infof(ctx, "preserving log directory: %s", l.volumesDir)
			continue
		}
		if err := os.RemoveAll(filepath.Join(l.volumesDir, v.Name())); err != nil {
			log.Warningf(ctx, "%v", err)
		}
	}
}
