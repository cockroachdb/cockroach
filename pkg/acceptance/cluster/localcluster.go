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

package cluster

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	roachClient "github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	builderImage     = "docker.io/cockroachdb/builder"
	builderTag       = "20170525-114009"
	builderImageFull = builderImage + ":" + builderTag
	networkPrefix    = "cockroachdb_acceptance"
)

// DefaultTCP is the default SQL/RPC port specification.
const DefaultTCP nat.Port = base.DefaultPort + "/tcp"
const defaultHTTP nat.Port = base.DefaultHTTPPort + "/tcp"

// CockroachBinaryInContainer is the container-side path to the CockroachDB
// binary.
const CockroachBinaryInContainer = "/cockroach/cockroach"

var cockroachImage = flag.String("i", builderImageFull, "the docker image to run")
var cockroachBinary = flag.String("b", defaultBinary(), "the host-side binary to run (if image == "+builderImage+")")
var cockroachEntry = flag.String("e", "", "the entry point for the image")
var waitOnStop = flag.Bool("w", false, "wait for the user to interrupt before tearing down the cluster")
var maxRangeBytes = config.DefaultZoneConfig().RangeMaxBytes

// keyLen is the length (in bits) of the generated CA and node certs.
const keyLen = 1024

func defaultBinary() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	// The repository root, as seen by the caller.
	for i := 0; i < 2; i++ {
		dir = filepath.Dir(dir)
	}
	// NB: This is the binary produced by our linux-gnu build target. Changes
	// to the Makefile must be reflected here.
	return filepath.Join(dir, "cockroach-linux-2.6.32-gnu-amd64")
}

func exists(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}

func nodeStr(l *LocalCluster, i int) string {
	return fmt.Sprintf("roach-%s-%d", l.clusterID, i)
}

func dataStr(node, store int) string {
	return fmt.Sprintf("/data%d.%d", node, store)
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
	index   int
	dataStr string
	config  StoreConfig
}

type testNode struct {
	*Container
	index   int
	nodeStr string
	config  NodeConfig
	stores  []testStore
}

// LocalCluster manages a local cockroach cluster running on docker. The
// cluster is composed of a "volumes" container which manages the
// persistent volumes used for certs and node data and N cockroach nodes.
type LocalCluster struct {
	client               client.APIClient
	mu                   syncutil.Mutex // Protects the fields below
	vols                 *Container
	config               TestConfig
	Nodes                []*testNode
	events               chan Event
	expectedEvents       chan Event
	oneshot              *Container
	CertsDir             string
	stopper              *stop.Stopper
	monitorCtx           context.Context
	monitorCtxCancelFunc func()
	clusterID            string
	networkID            string
	networkName          string
	privileged           bool   // whether to run containers in privileged mode
	logDir               string // no logging if empty
	logDirRemovable      bool   // if true, the log directory can be removed after use
}

// CreateLocal creates a new local cockroach cluster. The stopper is used to
// gracefully shutdown the channel (e.g. when a signal arrives). The cluster
// must be started before being used and keeps logs in the specified logDir, if
// supplied.
func CreateLocal(
	ctx context.Context, cfg TestConfig, logDir string, privileged bool, stopper *stop.Stopper,
) *LocalCluster {
	select {
	case <-stopper.ShouldStop():
		// The stopper was already closed, exit early.
		os.Exit(1)
	default:
	}

	if *cockroachImage == builderImageFull && !exists(*cockroachBinary) {
		log.Fatalf(ctx, "\"%s\": does not exist", *cockroachBinary)
	}

	cli, err := client.NewEnvClient()
	maybePanic(err)

	retryingClient := retryingDockerClient{
		resilientDockerClient: resilientDockerClient{APIClient: cli},
		attempts:              10,
		timeout:               10 * time.Second,
	}

	clusterID := uuid.MakeV4()
	clusterIDS := clusterID.Short()
	// Only pass a nonzero logDir down to LocalCluster when instructed to keep
	// logs.
	var uniqueLogDir string
	logDirRemovable := false
	if logDir != "" {
		pwd, err := os.Getwd()
		maybePanic(err)

		uniqueLogDir = fmt.Sprintf("%s-%s", logDir, clusterIDS)
		if !filepath.IsAbs(uniqueLogDir) {
			uniqueLogDir = filepath.Join(pwd, uniqueLogDir)
		}
		ensureLogDirExists(uniqueLogDir)
		logDirRemovable = true
		log.Infof(ctx, "local cluster log directory: %s", uniqueLogDir)
	}
	return &LocalCluster{
		clusterID: clusterIDS,
		client:    retryingClient,
		config:    cfg,
		stopper:   stopper,
		// TODO(tschottdorf): deadlocks will occur if these channels fill up.
		events:          make(chan Event, 1000),
		expectedEvents:  make(chan Event, 1000),
		logDir:          uniqueLogDir,
		logDirRemovable: logDirRemovable,
		privileged:      privileged,
	}
}

func (l *LocalCluster) expectEvent(c *Container, msgs ...string) {
	for index, ctr := range l.Nodes {
		if c.id != ctr.id {
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
func (l *LocalCluster) OneShot(
	ctx context.Context,
	ref string,
	ipo types.ImagePullOptions,
	containerConfig container.Config,
	hostConfig container.HostConfig,
	name string,
) error {
	if err := pullImage(ctx, l, ref, ipo); err != nil {
		return err
	}
	hostConfig.VolumesFrom = []string{l.vols.id}
	container, err := createContainer(ctx, l, containerConfig, hostConfig, name)
	if err != nil {
		return err
	}
	l.oneshot = container
	defer func() {
		if err := l.oneshot.Remove(ctx); err != nil {
			log.Errorf(ctx, "ContainerRemove: %s", err)
		}
		l.oneshot = nil
	}()

	if err := l.oneshot.Start(ctx); err != nil {
		return err
	}
	if err := l.oneshot.Wait(ctx); err != nil {
		return err
	}
	return nil
}

// stopOnPanic is invoked as a deferred function in Start in order to attempt
// to tear down the cluster if a panic occurs while starting it. If the panic
// was initiated by the stopper being closed (which panicOnStop notices) then
// the process is exited with a failure code.
func (l *LocalCluster) stopOnPanic(ctx context.Context) {
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
func (l *LocalCluster) panicOnStop() {
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

func (l *LocalCluster) createNetwork(ctx context.Context) {
	l.panicOnStop()

	l.networkName = fmt.Sprintf("%s-%s", networkPrefix, l.clusterID)
	log.Infof(ctx, "creating docker network with name: %s", l.networkName)
	net, err := l.client.NetworkInspect(ctx, l.networkName)
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
func (l *LocalCluster) initCluster(ctx context.Context) {
	configJSON, err := json.Marshal(l.config)
	maybePanic(err)
	log.Infof(ctx, "Initializing Cluster %s:\n%s", l.config.Name, configJSON)
	l.panicOnStop()

	pwd, err := os.Getwd()
	maybePanic(err)

	// Create the temporary certs directory in the current working
	// directory. Boot2docker's handling of binding local directories
	// into the container is very confusing. If the directory being
	// bound has a parent directory that exists in the boot2docker VM
	// then that directory is bound into the container. In particular,
	// that means that binds of /tmp and /var will be problematic.
	l.CertsDir, err = ioutil.TempDir(pwd, ".localcluster.certs.")
	maybePanic(err)

	binds := []string{
		l.CertsDir + ":/certs",
		filepath.Join(pwd, "..") + ":/go/src/github.com/cockroachdb/cockroach",
	}

	if l.logDir != "" {
		binds = append(binds, l.logDir+":/logs")
	}
	if *cockroachImage == builderImageFull {
		path, err := filepath.Abs(*cockroachBinary)
		maybePanic(err)
		binds = append(binds, path+":"+CockroachBinaryInContainer)
	}

	l.Nodes = []*testNode{}
	vols := map[string]struct{}{}
	// Expand the cluster configuration into nodes and stores per node.
	var nodeCount int
	for _, nc := range l.config.Nodes {
		for i := 0; i < int(nc.Count); i++ {
			newTestNode := &testNode{
				config:  nc,
				index:   nodeCount,
				nodeStr: nodeStr(l, nodeCount),
			}
			nodeCount++
			var storeCount int
			for _, sc := range nc.Stores {
				for j := 0; j < int(sc.Count); j++ {
					vols[dataStr(nodeCount, storeCount)] = struct{}{}
					newTestNode.stores = append(newTestNode.stores,
						testStore{
							config:  sc,
							index:   j,
							dataStr: dataStr(nodeCount, storeCount),
						})
					storeCount++
				}
			}
			l.Nodes = append(l.Nodes, newTestNode)
		}
	}

	if *cockroachImage == builderImageFull {
		maybePanic(pullImage(ctx, l, builderImageFull, types.ImagePullOptions{}))
	}
	c, err := createContainer(
		ctx,
		l,
		container.Config{
			Image:      *cockroachImage,
			Volumes:    vols,
			Entrypoint: []string{"/bin/true"},
		}, container.HostConfig{
			Binds:           binds,
			PublishAllPorts: true,
		},
		fmt.Sprintf("volumes-%s", l.clusterID),
	)
	maybePanic(err)
	// Make sure this assignment to l.vols is before the calls to Start and Wait.
	// Otherwise, if they trigger maybePanic, this container won't get cleaned up
	// and it'll get in the way of future runs.
	l.vols = c
	maybePanic(c.Start(ctx))
	maybePanic(c.Wait(ctx))
}

// createRoach creates the docker container for a testNode. It may be called in
// parallel to start many nodes at once, and thus should remain threadsafe.
func (l *LocalCluster) createRoach(
	ctx context.Context, node *testNode, vols *Container, env []string, cmd ...string,
) {
	l.panicOnStop()

	hostConfig := container.HostConfig{
		PublishAllPorts: true,
		NetworkMode:     container.NetworkMode(l.networkID),
		Privileged:      l.privileged,
	}

	if vols != nil {
		hostConfig.VolumesFrom = append(hostConfig.VolumesFrom, vols.id)
	}

	var hostname string
	if node.index >= 0 {
		hostname = fmt.Sprintf("roach-%s-%d", l.clusterID, node.index)
	}
	log.Infof(ctx, "creating docker container with name: %s", hostname)
	var entrypoint []string
	if *cockroachImage == builderImageFull {
		entrypoint = append(entrypoint, CockroachBinaryInContainer)
	} else if *cockroachEntry != "" {
		entrypoint = append(entrypoint, *cockroachEntry)
	}
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
			Entrypoint: entrypoint,
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
		node.nodeStr,
	)
	maybePanic(err)
}

func (l *LocalCluster) createCACert() {
	maybePanic(security.CreateCAPair(
		l.CertsDir, filepath.Join(l.CertsDir, security.EmbeddedCAKey),
		keyLen, 48*time.Hour, false, false))
}

func (l *LocalCluster) createNodeCerts() {
	nodes := []string{"localhost", dockerIP().String()}
	for _, node := range l.Nodes {
		nodes = append(nodes, node.nodeStr)
	}
	maybePanic(security.CreateNodePair(
		l.CertsDir,
		filepath.Join(l.CertsDir, security.EmbeddedCAKey),
		keyLen, 48*time.Hour, false, nodes))
}

// startNode starts a Docker container to run testNode. It may be called in
// parallel to start many nodes at once, and thus should remain threadsafe.
func (l *LocalCluster) startNode(ctx context.Context, node *testNode) {
	cmd := []string{
		"start",
		"--certs-dir=/certs/",
		"--host=" + node.nodeStr,
		"--verbosity=1",
	}

	// Forward the vmodule flag to the nodes.
	vmoduleFlag := flag.Lookup(logflags.VModuleName)
	if vmoduleFlag.Value.String() != "" {
		cmd = append(cmd, fmt.Sprintf("--%s=%s", vmoduleFlag.Name, vmoduleFlag.Value.String()))
	}

	for _, store := range node.stores {
		storeSpec := base.StoreSpec{
			Path:        store.dataStr,
			SizeInBytes: int64(store.config.MaxRanges) * maxRangeBytes,
		}
		cmd = append(cmd, fmt.Sprintf("--store=%s", storeSpec))
	}
	// Append --join flag for all nodes except first.
	if node.index > 0 {
		cmd = append(cmd, "--join="+net.JoinHostPort(l.Nodes[0].nodeStr, base.DefaultPort))
	}

	var localLogDir string
	if len(l.logDir) > 0 {
		dockerLogDir := "/logs/" + node.nodeStr
		localLogDir = filepath.Join(l.logDir, node.nodeStr)
		ensureLogDirExists(localLogDir)
		cmd = append(
			cmd,
			"--logtostderr=ERROR",
			"--log-dir="+dockerLogDir)
	} else {
		cmd = append(cmd, "--logtostderr=INFO")
	}
	env := []string{
		"COCKROACH_SCAN_MAX_IDLE_TIME=200ms",
		"COCKROACH_CONSISTENCY_CHECK_PANIC_ON_FAILURE=true",
		"COCKROACH_SKIP_UPDATE_CHECK=1",
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

  cli-env:   COCKROACH_INSECURE=false COCKROACH_CERTS_DIR=%[7]s COCKROACH_HOST=%s COCKROACH_PORT=%d`,
		node.Name(), "https://"+httpAddr.String(), localLogDir, node.Container.id[:5],
		base.DefaultHTTPPort, cmd, l.CertsDir, httpAddr.IP, httpAddr.Port)
}

// returns false is the event
func (l *LocalCluster) processEvent(ctx context.Context, event events.Message) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// If there's currently a oneshot container, ignore any die messages from
	// it because those are expected.
	if l.oneshot != nil && event.ID == l.oneshot.id && event.Status == eventDie {
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
	case <-l.stopper.ShouldStop():
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

func (l *LocalCluster) monitor(ctx context.Context) {
	if log.V(1) {
		log.Infof(ctx, "events monitor starts")
		defer log.Infof(ctx, "events monitor exits")
	}
	longPoll := func() bool {
		// If our context was cancelled, it's time to go home.
		if l.monitorCtx.Err() != nil {
			return false
		}

		args, err := filters.ParseFlag(
			fmt.Sprintf("label=Acceptance-cluster-id=%s", l.clusterID), filters.NewArgs())
		maybePanic(err)

		eventq, errq := l.client.Events(l.monitorCtx, types.EventsOptions{
			Filters: args,
		})
		for {
			select {
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
func (l *LocalCluster) Start(ctx context.Context) {
	defer l.stopOnPanic(ctx)

	l.mu.Lock()
	defer l.mu.Unlock()

	l.createNetwork(ctx)
	l.initCluster(ctx)
	log.Infof(ctx, "creating certs (%dbit) in: %s", keyLen, l.CertsDir)
	l.createCACert()
	l.createNodeCerts()
	maybePanic(security.CreateClientPair(
		l.CertsDir, filepath.Join(l.CertsDir, security.EmbeddedCAKey),
		512, 48*time.Hour, false, security.RootUser))

	l.monitorCtx, l.monitorCtxCancelFunc = context.WithCancel(context.Background())
	go l.monitor(ctx)
	var wg sync.WaitGroup
	wg.Add(len(l.Nodes))
	for _, node := range l.Nodes {
		go func(node *testNode) {
			l.startNode(ctx, node)
			wg.Done()
		}(node)
	}
	wg.Wait()
}

// Assert drains the Events channel and compares the actual events with those
// expected to have been generated by the operations performed on the nodes in
// the cluster (restart, kill, ...). In the event of a mismatch, the passed
// Tester receives a fatal error.
func (l *LocalCluster) Assert(ctx context.Context, t testing.TB) {
	const almostZero = 50 * time.Millisecond
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
		exp := filter(l.expectedEvents, almostZero)
		if exp == nil {
			break
		}
		act := filter(l.events, 15*time.Second)
		if act == nil || *exp != *act {
			t.Fatalf("expected event %v, got %v (after %v)", exp, act, events)
		}
		events = append(events, *exp)
	}
	if cur := filter(l.events, almostZero); cur != nil {
		t.Fatalf("unexpected extra event %v (after %v)", cur, events)
	}
	if log.V(2) {
		log.Infof(ctx, "asserted %v", events)
	}
}

// AssertAndStop calls Assert and then stops the cluster. It is safe to stop
// the cluster multiple times.
func (l *LocalCluster) AssertAndStop(ctx context.Context, t testing.TB) {
	defer l.stop(ctx)
	l.Assert(ctx, t)
}

// stop stops the cluster.
func (l *LocalCluster) stop(ctx context.Context) {
	if *waitOnStop {
		log.Infof(ctx, "waiting for interrupt")
		<-l.stopper.ShouldStop()
	}

	log.Infof(ctx, "stopping")

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.monitorCtxCancelFunc != nil {
		l.monitorCtxCancelFunc()
		l.monitorCtxCancelFunc = nil
	}

	if l.vols != nil {
		maybePanic(l.vols.Kill(ctx))
		maybePanic(l.vols.Remove(ctx))
		l.vols = nil
	}
	if l.CertsDir != "" {
		_ = os.RemoveAll(l.CertsDir)
		l.CertsDir = ""
	}
	outputLogDir := l.logDir
	for i, n := range l.Nodes {
		if n.Container == nil {
			continue
		}
		ci, err := n.Inspect(ctx)
		crashed := err != nil || (!ci.State.Running && ci.State.ExitCode != 0)
		maybePanic(n.Kill(ctx))
		if crashed && outputLogDir == "" {
			outputLogDir, err = ioutil.TempDir("", "crashed_nodes")
			if err != nil {
				panic(err)
			}
		}
		if crashed || l.logDir != "" {
			// TODO(bdarnell): make these filenames more consistent with
			// structured logs?
			file := filepath.Join(outputLogDir, nodeStr(l, i),
				fmt.Sprintf("stderr.%s.log", strings.Replace(
					timeutil.Now().Format(time.RFC3339), ":", "_", -1)))

			maybePanic(os.MkdirAll(filepath.Dir(file), 0777))
			w, err := os.Create(file)
			maybePanic(err)
			defer w.Close()
			maybePanic(n.Logs(ctx, w))
			log.Infof(ctx, "node %d: stderr at %s", i, file)
			if crashed {
				log.Infof(ctx, "~~~ node %d CRASHED ~~~~", i)
			}
		}
		maybePanic(n.Remove(ctx))
	}
	l.Nodes = nil

	if l.networkID != "" {
		maybePanic(
			l.client.NetworkRemove(ctx, l.networkID))
		l.networkID = ""
		l.networkName = ""
	}
}

// NewClient implements the Cluster interface.
func (l *LocalCluster) NewClient(ctx context.Context, i int) (*roachClient.DB, error) {
	clock := hlc.NewClock(hlc.UnixNano, 0)
	rpcContext := rpc.NewContext(log.AmbientContext{}, &base.Config{
		User:        security.NodeUser,
		SSLCertsDir: l.CertsDir,
	}, clock, l.stopper)
	conn, err := rpcContext.GRPCDial(l.Nodes[i].Addr(ctx, DefaultTCP).String())
	if err != nil {
		return nil, err
	}
	return roachClient.NewDB(roachClient.NewSender(conn), clock), nil
}

// InternalIP returns the IP address used for inter-node communication.
func (l *LocalCluster) InternalIP(ctx context.Context, i int) net.IP {
	c := l.Nodes[i]
	containerInfo, err := c.Inspect(ctx)
	if err != nil {
		return nil
	}
	return net.ParseIP(containerInfo.NetworkSettings.Networks[l.networkName].IPAddress)
}

// PGUrl returns a URL string for the given node postgres server.
func (l *LocalCluster) PGUrl(ctx context.Context, i int) string {
	certUser := security.RootUser
	options := url.Values{}
	options.Add("sslmode", "verify-full")
	options.Add("sslcert", filepath.Join(l.CertsDir, security.EmbeddedRootCert))
	options.Add("sslkey", filepath.Join(l.CertsDir, security.EmbeddedRootKey))
	options.Add("sslrootcert", filepath.Join(l.CertsDir, security.EmbeddedCACert))
	pgURL := url.URL{
		Scheme:   "postgres",
		User:     url.User(certUser),
		Host:     l.Nodes[i].Addr(ctx, DefaultTCP).String(),
		RawQuery: options.Encode(),
	}
	return pgURL.String()
}

// NumNodes returns the number of nodes in the cluster.
func (l *LocalCluster) NumNodes() int {
	return len(l.Nodes)
}

// Kill kills the i-th node.
func (l *LocalCluster) Kill(ctx context.Context, i int) error {
	if err := l.Nodes[i].Kill(ctx); err != nil {
		return errors.Wrapf(err, "failed to kill node %d", i)
	}
	return nil
}

// Restart restarts the given node. If the node isn't running, this starts it.
func (l *LocalCluster) Restart(ctx context.Context, i int) error {
	// The default timeout is 10 seconds.
	if err := l.Nodes[i].Restart(ctx, nil); err != nil {
		return errors.Wrapf(err, "failed to restart node %d", i)
	}
	return nil
}

// URL returns the base url.
func (l *LocalCluster) URL(ctx context.Context, i int) string {
	return "https://" + l.Nodes[i].Addr(ctx, defaultHTTP).String()
}

// Addr returns the host and port from the node in the format HOST:PORT.
func (l *LocalCluster) Addr(ctx context.Context, i int, port string) string {
	return l.Nodes[i].Addr(ctx, nat.Port(port+"/tcp")).String()
}

// Hostname implements the Cluster interface.
func (l *LocalCluster) Hostname(i int) string {
	return l.Nodes[i].nodeStr
}

// ExecRoot runs a command as root.
func (l *LocalCluster) ExecRoot(ctx context.Context, i int, cmd []string) error {
	execRoot := func(ctx context.Context) error {
		cfg := types.ExecConfig{
			User:         "root",
			Privileged:   true,
			Cmd:          cmd,
			AttachStderr: true,
			AttachStdout: true,
		}
		createResp, err := l.client.ContainerExecCreate(ctx, l.Nodes[i].Container.id, cfg)
		if err != nil {
			return err
		}
		var outputStream, errorStream bytes.Buffer
		{
			resp, err := l.client.ContainerExecAttach(ctx, createResp.ID, cfg)
			if err != nil {
				return err
			}
			defer resp.Close()
			ch := make(chan error)
			go func() {
				_, err := stdcopy.StdCopy(&outputStream, &errorStream, resp.Reader)
				ch <- err
			}()
			if err := <-ch; err != nil {
				return err
			}
		}
		{
			resp, err := l.client.ContainerExecInspect(ctx, createResp.ID)
			if err != nil {
				return err
			}
			if resp.Running {
				return errors.Errorf("command still running")
			}
			if resp.ExitCode != 0 {
				return fmt.Errorf("error executing %s:\n%s\n%s",
					cmd, outputStream.String(),
					errorStream.String())
			}
		}
		return nil
	}

	return retry(ctx, 3, 10*time.Second, "ExecRoot",
		matchNone, execRoot)
}

func ensureLogDirExists(logDir string) {
	// Ensure that the path exists, with all its parents.
	// If we don't make sure the directory exists, Docker will and then we
	// may run into ownership issues (think Docker running as root, but us
	// running as a regular Joe as it happens on CircleCI).
	maybePanic(os.MkdirAll(logDir, 0755))
	// Now make the last component, and just the last one, writable by
	// anyone, and set the gid and uid bit so that the owner is
	// propagated to sub-directories.
	maybePanic(os.Chmod(logDir, 0777|os.ModeSetuid|os.ModeSetgid))
}

// Cleanup removes the log directory if it was initially created
// by this LocalCluster.
func (l *LocalCluster) Cleanup(ctx context.Context) {
	if l.logDir != "" && l.logDirRemovable {
		if err := os.RemoveAll(l.logDir); err != nil {
			log.Warning(ctx, err)
		}
	}
}
