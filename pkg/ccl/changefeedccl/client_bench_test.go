// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl_test

import (
	"bufio"
	"context"
	"database/sql"
	gosql "database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	_ "github.com/cockroachdb/cockroach/pkg/workload/ycsb"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func BenchmarkChangeFeed(b *testing.B) {
	defer leaktest.AfterTest(b)()

	b.Run("sinkless", func(b *testing.B) {
		cb := newChangefeedBenchmark(b, b.N, sinklessSink)
		defer cb.cleanup()
		cb.run()
	})
	b.Run("http", func(b *testing.B) {
		cb := newChangefeedBenchmark(b, b.N, httpSink)
		defer cb.cleanup()
		cb.run()
	})
}

type changefeedBenchmark struct {
	tb           testing.TB
	httpServer   http.Server
	httpListener net.Listener

	tempDir string

	n int

	serverSubprocess struct {
		sync.Mutex
		running                    bool // set to true by /serverstart request
		started, done              chan struct{}
		sqlAddr, httpAddr, rpcAddr string
		sslCertsDir                string
	}

	sink struct {
		sync.Mutex
		startTime     time.Time
		started, done chan struct{}
		sinkAddr      string
	}
	localServer serverutils.TestServerInterface
	cleanupFunc func()
	sinkType    sinkType
}

const (
	// Arguments for serverStart
	sqlAddrArg     = "sqladdr"
	sslCertsDirArg = "sslcertsdir"
	httpAddrArg    = "httpaddr"
	listenAddrArg  = "listenaddr"

	// Arguments for sinkStart
	sinkAddrArg = "sinkaddr"

	serverStartPath = "/serverstart"
	sinkStartPath   = "/sinkstart"
	sinkDonePath    = "/sinkdone"
)

type sinkType int

const (
	httpSink sinkType = iota
	sinklessSink
	kafkaSink
)

func newChangefeedBenchmark(tb testing.TB, n int, sink sinkType) *changefeedBenchmark {
	cb := &changefeedBenchmark{tb: tb}
	var err error
	cb.tempDir, err = ioutil.TempDir("", "BenchmarkChangeFeed")
	require.NoError(tb, err)
	cb.sink.started = make(chan struct{})
	cb.sink.done = make(chan struct{})
	cb.serverSubprocess.started = make(chan struct{})
	cb.sinkType = sink
	cb.n = n
	cb.setupMux()
	return cb
}

func (cb *changefeedBenchmark) run() {
	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)
	defer func() { require.NoError(cb.tb, g.Wait()) }()
	defer cancel()
	{
		// Run the local coordinating test server.
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(cb.tb, err)
		cb.httpListener = l
		g.Go(swallowError(ctx, func() error {
			return cb.httpServer.Serve(l)
		}))
		g.Go(swallowError(ctx, func() error {
			<-ctx.Done()
			return cb.httpServer.Close()
		}))
	}

	// Run a cockroach server which stores data in a subprocess.
	// This subprocess will also set up the data set.
	// It will send a request to the local process when it finishes leading to the
	// closure of serverSubprocess.started. That server will also set up zone
	// configurations to have constraints against the the "coordinator" tag that
	// we'll later user for the server we start inside of this process.
	g.Go(swallowError(ctx, func() error {
		return cb.runCockroachServer(ctx)
	}))

	// Wait for the server subprocess to start up.
	<-cb.serverSubprocess.started

	// Launch a local SQL server that joins the cluster created in the subprocess.
	// This server will use the "coordinator" tag and thus avoid hosting any
	// replicas.
	cb.runLocalServer()

	// Launch the sink process. The sink process will connect to the local server.
	// It will create a changefeed and read from the sink until all of the
	// expected rows have been seen.
	g.Go(swallowError(ctx, func() error {
		return cb.runSink(ctx)
	}))

	// Wait for the sink to start.
	select {
	case <-ctx.Done():
	case <-cb.sink.started:
	}
	// Wait for the sink to finish.
	select {
	case <-cb.sink.done:
	case <-ctx.Done():
	}
	// Tear everything down.
	cancel()
}

func (cb *changefeedBenchmark) handleServerStart(
	writer http.ResponseWriter, request *http.Request,
) {
	cb.serverSubprocess.Lock()
	defer cb.serverSubprocess.Unlock()
	if cb.serverSubprocess.running {
		cb.tb.Error("already called")
	}
	q := request.URL.Query()
	cb.serverSubprocess.sqlAddr = q.Get(sqlAddrArg)
	cb.serverSubprocess.httpAddr = q.Get(httpAddrArg)
	cb.serverSubprocess.rpcAddr = q.Get(listenAddrArg)
	cb.serverSubprocess.sslCertsDir = q.Get(sslCertsDirArg)
	close(cb.serverSubprocess.started)
	writer.WriteHeader(200)
}

func (cb *changefeedBenchmark) handleSinkStart(writer http.ResponseWriter, request *http.Request) {
	cb.sink.Lock()
	defer cb.sink.Unlock()
	q := request.URL.Query()
	cb.sink.sinkAddr = q.Get("sinkaddr")
	cb.sink.startTime = time.Now()
	if b, isBench := cb.tb.(*testing.B); isBench {
		b.ResetTimer()
	}
	writer.WriteHeader(200)
	close(cb.sink.started)
}

func (cb *changefeedBenchmark) handleSinkDone(writer http.ResponseWriter, request *http.Request) {
	cb.sink.Lock()
	defer cb.sink.Unlock()
	if b, isBench := cb.tb.(*testing.B); isBench {
		b.StopTimer()
	}

	defer close(cb.sink.done)
	writer.WriteHeader(200)
}

func (cb *changefeedBenchmark) setupMux() {
	m := http.NewServeMux()
	m.HandleFunc(serverStartPath, cb.handleServerStart)
	m.HandleFunc(sinkStartPath, cb.handleSinkStart)
	m.HandleFunc(sinkDonePath, cb.handleSinkDone)
	cb.httpServer.Handler = m
}

// swallowError is a helper to swallow errors from functions used with an
// errgroup to ensure that an error is not returned if the context has been
// canceled.
func swallowError(ctx context.Context, f func() error) func() error {
	return func() error {
		if err := f(); err != nil && ctx.Err() == nil {
			return err
		}
		return nil
	}
}

// cleanup should be called at the end of a test.
func (cb *changefeedBenchmark) cleanup() {
	if cb.localServer != nil {
		cb.localServer.Stopper().Stop(context.Background())
	}
	if cb.cleanupFunc != nil {
		cb.cleanupFunc()
	}
	os.RemoveAll(cb.tempDir)
}

func (cb *changefeedBenchmark) runCockroachServer(ctx context.Context) error {
	serverProc := helperProcess(ctx, "server",
		"-n", strconv.Itoa(cb.n),
		"--test-addr", cb.httpListener.Addr().String())
	cb.serverSubprocess.done = make(chan struct{})
	defer close(cb.serverSubprocess.done)
	if err := serverProc.Run(); err != nil && ctx.Err() == nil {
		return errors.Wrapf(err, "output: %s", "out")
	}
	return nil
}

func (cb *changefeedBenchmark) runSink(ctx context.Context) error {
	pgURL, cleanup := sqlutils.PGUrl(cb.tb, cb.localServer.ServingSQLAddr(), "", url.User(security.RootUser))
	cb.cleanupFunc = cleanup

	// Let's now set up some data
	var sink *exec.Cmd
	args := []string{
		"-n", strconv.Itoa(cb.n),
		"--pgurl", pgURL.String(),
		"--test-addr", cb.httpListener.Addr().String(),
	}
	switch cb.sinkType {
	case sinklessSink:
		sink = helperProcess(ctx, "sink-sinkless", args...)
	case httpSink:
		sink = helperProcess(ctx, "sink-http", args...)
	case kafkaSink:
		return fmt.Errorf("kafka sink not yet support")
	default:
		return fmt.Errorf("unknown sink type %v", cb.sinkType)
	}

	if err := sink.Run(); err != nil {
		return errors.Wrapf(err, "output: %s", "out")
	}
	return nil
}

func (cb *changefeedBenchmark) runLocalServer() {
	spec := base.DefaultTestStoreSpec
	spec.Attributes.Attrs = append(spec.Attributes.Attrs, "coordinator")
	tc := testcluster.StartTestCluster(cb.tb, 1, base.TestClusterArgs{})
	socketFile := filepath.Join(cb.tempDir, "test.crdb.socket")
	tc.StopServer(0) // This is a horrible hack to get a testServer
	tc.AddServer(cb.tb, base.TestServerArgs{
		Addr:          "127.0.0.1:0",
		PartOfCluster: true,
		SSLCertsDir:   cb.serverSubprocess.sslCertsDir,
		JoinAddr:      cb.serverSubprocess.rpcAddr,
		StoreSpecs:    []base.StoreSpec{spec},
		SocketFile:    socketFile,
	})
	cb.localServer = tc.Server(1)
}

func helperProcess(ctx context.Context, c string, args ...string) *exec.Cmd {
	cs := append(os.Args[1:], append([]string{"-test.v", "-test.run=TestHelper", "--", c}, args...)...)
	env := []string{"GO_WANT_HELPER_PROCESS=1"}
	cmd := exec.CommandContext(ctx, os.Args[0], cs...)
	cmd.Env = append(env, os.Environ()...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd
}

func TestHelper(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	switch args := findArgs(); args[0] {
	case "sink-http":
		newSinkHTTP(args[1:]).run(t)
	case "sink-sinkless":
		newSinkSinkless(args[1:]).run(t)
	case "server":
		runServer(t, args[1:])
	default:
		panic(fmt.Sprintf("unknown command: %v", args))
	}
}

// Sink subprocess code.

type sinkHTTP struct {
	sinkConfig

	l net.Listener
	s http.Server

	mu struct {
		syncutil.Mutex
		gotAllCh chan struct{}
		seen     int
	}
}

func newSinkHTTP(args []string) *sinkHTTP {
	s := &sinkHTTP{
		sinkConfig: parseSinkFlags(args),
	}
	s.s = http.Server{Handler: s}
	s.mu.gotAllCh = make(chan struct{})
	return s
}

func (s *sinkHTTP) ServeHTTP(writer http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		writer.WriteHeader(http.StatusNotImplemented)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	defer func() {
		if s.mu.seen >= s.n && s.mu.gotAllCh != nil {
			close(s.mu.gotAllCh)
			s.mu.gotAllCh = nil
		}
	}()
	if strings.Contains(r.URL.Path, "RESOLVED") {
		return
	}
	// TODO(ajwerner): we could do something to track whether we get rows more
	// than once but for now let's just assume that that does not happen.
	sc := bufio.NewScanner(r.Body)
	for sc.Scan() {
		if strings.Contains(sc.Text(), "resolved") {
			if s.mu.seen >= s.n {
				break
			}
		} else {
			s.mu.seen++
		}
	}
}

func (s *sinkHTTP) run(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)
	defer g.Wait()
	defer cancel()

	c := newConn(t, s.pgURL)
	defer c.Close()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	gotAll := s.mu.gotAllCh
	g.Go(swallowError(ctx, func() error { return s.s.Serve(l) }))
	g.Go(swallowError(ctx, func() error {
		<-ctx.Done()
		return s.s.Close()
	}))
	sendSinkStart(t, s.testURL, l.Addr().String())
	require.NoError(t, err)
	var jobID int
	require.NoError(t, c.QueryRow(
		"CREATE CHANGEFEED FOR usertable "+
			"INTO 'experimental-http://"+l.Addr().String()+
			"' WITH resolved = '10ms'").
		Scan(&jobID))
	<-gotAll
	sendSinkDone(t, s.testURL)
	require.NoError(t, err)
}

type sinkSinkless struct {
	sinkConfig
}

func newSinkSinkless(args []string) *sinkSinkless {
	return &sinkSinkless{sinkConfig: parseSinkFlags(args)}
}

func (s *sinkSinkless) run(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := newConn(t, s.pgURL)
	defer c.Close()

	sendSinkStart(t, s.testURL, "")
	rows, err := c.QueryEx(ctx, "CREATE CHANGEFEED FOR usertable WITH resolved = '10ms'", nil /* options */)
	require.NoError(t, err)
	var table, key, value sql.NullString
	var seen int
	for rows.Next() {
		rows.Scan(&table, &key, &value)
		if strings.Contains(value.String, "resolved") {
			if seen >= s.n {
				break
			}
		} else {
			seen++
		}
	}
	if rows.Err() != nil {
		require.NoError(t, rows.Err())
	}
	sendSinkDone(t, s.testURL)
}

func sendSinkStart(t testing.TB, testURL, sinkAddr string) {
	vals := url.Values{}
	vals.Add(sinkAddrArg, sinkAddr)
	_, err := http.Post("http://"+testURL+sinkStartPath+"?"+vals.Encode(),
		"text/plain", nil)
	require.NoError(t, err)
}

func sendSinkDone(t testing.TB, testURL string) {
	_, err := http.Post("http://"+testURL+sinkDonePath, "text/plain", nil)
	require.NoError(t, err)
}

func newConn(t *testing.T, pgURL string) *pgx.Conn {
	pgConf, err := pgx.ParseConnectionString(pgURL)
	require.NoError(t, err)
	c, err := pgx.Connect(pgConf)
	require.NoError(t, err)
	_, err = c.Exec("SELECT 1")
	require.NoError(t, err)
	return c
}

type sinkConfig struct {
	testURL string
	pgURL   string
	n       int
}

func parseSinkFlags(args []string) sinkConfig {
	var sk sinkConfig
	flags := flag.NewFlagSet("sink", flag.PanicOnError)
	flags.IntVar(&sk.n, "n", 0, "benchmark n")
	flags.StringVar(&sk.pgURL, "pgurl", "", "pgurl")
	flags.StringVar(&sk.testURL, "test-addr", "", "test-addr")
	flags.Parse(args)
	return sk
}

// Server subprocess code

func runServer(t *testing.T, args []string) {
	conf := parseServerFlags(args)
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Addr:          "0.0.0.0:0",
			PartOfCluster: true,
		},
	})
	defer tc.Stopper().Stop(ctx)
	tc.WaitForFullReplication()
	s := tc.Server(0)
	c := tc.ServerConn(0)
	setUpCluster(t, c)
	insertData(ctx, t, c, conf.N)
	sendServerStartupDone(t, s, conf.TestURL)
	<-(chan struct{})(nil) // block forever
}

type serverConfig struct {
	TestURL string
	N       int
}

func parseServerFlags(args []string) serverConfig {
	var sk serverConfig
	flags := flag.NewFlagSet("sink", flag.PanicOnError)
	flags.IntVar(&sk.N, "n", 0, "benchmark n")
	flags.StringVar(&sk.TestURL, "test-addr", "", "test-addr")
	flags.Parse(args)
	if sk.TestURL == "" {
		panic(fmt.Sprintf("huh %v", args))
	}
	return sk
}

func sendServerStartupDone(t *testing.T, s serverutils.TestServerInterface, testAddr string) {
	vals := url.Values{}
	vals.Set(listenAddrArg, s.ServingRPCAddr())
	vals.Set(sqlAddrArg, s.ServingSQLAddr())
	vals.Set(httpAddrArg, s.HTTPAddr())
	vals.Set(sslCertsDirArg, filepath.Dir(s.RPCContext().SecurityContext.ClientCACertPath()))
	startUrl := "http://" + testAddr + serverStartPath + "?" + vals.Encode()
	resp, err := http.Post(startUrl, "text/plain", nil)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
}

func setUpCluster(t *testing.T, c *gosql.DB) {
	sqlDB := sqlutils.MakeSQLRunner(c)
	sqlDB.Exec(t, "ALTER RANGE default  CONFIGURE ZONE USING constraints = '[-coordinator]';")
	sqlDB.Exec(t, "ALTER RANGE meta     CONFIGURE ZONE USING constraints = '[-coordinator]';")
	sqlDB.Exec(t, "ALTER RANGE system   CONFIGURE ZONE USING constraints = '[-coordinator]';")
	sqlDB.Exec(t, "ALTER TABLE system.public.jobs CONFIGURE ZONE USING constraints = '[-coordinator]';")
	sqlDB.Exec(t, "ALTER RANGE liveness CONFIGURE ZONE USING constraints = '[-coordinator]';")
	sqlDB.Exec(t, "ALTER DATABASE system CONFIGURE ZONE USING constraints = '[-coordinator]';")
	sqlDB.Exec(t, "SET CLUSTER SETTING kv.rangefeed.enabled = true")
	sqlDB.Exec(t, "SET CLUSTER SETTING server.remote_debugging.mode = 'any';")
	sqlDB.Exec(t, "SET CLUSTER SETTING sql.defaults.distsql = 'off';")
	return
}

func insertData(ctx context.Context, t *testing.T, conn *gosql.DB, n int) {
	idl := workloadsql.InsertsDataLoader{
		BatchSize:   512,
		Concurrency: 4,
	}
	meta, err := workload.Get("ycsb")
	gen := meta.New().(workload.Flagser)
	require.NoError(t, gen.Flags().Parse([]string{
		"--insert-count=" + strconv.Itoa(n),
		"--families=false",
	}))
	_, err = workloadsql.Setup(ctx, conn, gen, idl)
	require.NoError(t, err)
	var rows int
	sqlutils.MakeSQLRunner(conn).QueryRow(t,
		"SELECT COUNT(*) FROM usertable").Scan(&rows)
	require.Equal(t, n, rows)
}

func findArgs() []string {
	var i int
	for i = 0; i < len(os.Args); i++ {
		if os.Args[i] == "--" {
			i++
			break
		}
	}
	return os.Args[i:]
}
