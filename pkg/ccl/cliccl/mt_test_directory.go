// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cliccl

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenantdirsvr"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/spf13/cobra"
)

var mtTestDirectorySvr = &cobra.Command{
	Use:   "test-directory",
	Short: "run a test directory service",
	Long: `
Run a test directory service that starts and manages tenant SQL instances as
processes on the local machine.

Use two dashes (--) to separate the test directory command's arguments from
the remaining arguments that specify the executable (and the arguments) that 
will be ran when starting each tenant.

For example:
cockroach mt test-directory --port 1234 -- cockroach mt start-sql --kv-addrs=:2222 --certs-dir=./certs --base-dir=./base
or 
cockroach mt test-directory --port 1234 -- bash -c ./tenant_start.sh 

test-directory command will always add the following arguments (in that order):
--sql-addr <addr/host>[:<port>] 
--http-addr <addr/host>[:<port>]
--tenant-id number
`,
	Args: nil,
	RunE: clierrorplus.MaybeDecorateError(runDirectorySvr),
}

func runDirectorySvr(cmd *cobra.Command, args []string) (returnErr error) {
	ctx := context.Background()
	stopper, err := cli.ClearStoresAndSetupLoggingForMTCommands(cmd, ctx)
	if err != nil {
		return err
	}
	defer stopper.Stop(ctx)

	tds, err := tenantdirsvr.New(stopper, func(ctx context.Context, stopper *stop.Stopper, tenantID uint64) (string, error) {
		return startTenant(ctx, stopper, tenantID, args)
	})
	if err != nil {
		return err
	}

	listenPort, err := net.Listen(
		"tcp", fmt.Sprintf(":%d", testDirectorySvrContext.port),
	)
	if err != nil {
		return err
	}
	stopper.AddCloser(stop.CloserFn(func() { _ = listenPort.Close() }))
	return tds.Serve(listenPort)
}

func startTenant(
	ctx context.Context, stopper *stop.Stopper, tenantID uint64, args []string,
) (sqlAddress string, err error) {
	// A hackish way to have the sql tenant process listen on known ports.
	sqlListener, err := net.Listen("tcp", "")
	if err != nil {
		return "", err
	}
	httpListener, err := net.Listen("tcp", "")
	if err != nil {
		return "", err
	}

	if len(args) == 0 {
		cockroachExecutable, err := os.Executable()
		if err != nil {
			return "", err
		}
		args = []string{
			cockroachExecutable, "mt", "start-sql", "--kv-addrs=:26257", "--insecure",
		}
	}
	args = append(args,
		fmt.Sprintf("--sql-addr=%s", sqlListener.Addr().String()),
		fmt.Sprintf("--http-addr=%s", httpListener.Addr().String()),
		fmt.Sprintf("--tenant-id=%d", tenantID),
	)
	if err = sqlListener.Close(); err != nil {
		return "", err
	}
	if err = httpListener.Close(); err != nil {
		return "", err
	}

	c := exec.Command(args[0], args[1:]...)
	c.Env = append(os.Environ(), "COCKROACH_TRUST_CLIENT_PROVIDED_SQL_REMOTE_ADDR=true")
	var f writerFunc = func(p []byte) (int, error) {
		sc := bufio.NewScanner(strings.NewReader(string(p)))
		for sc.Scan() {
			log.Infof(ctx, "%s", sc.Text())
		}
		return len(p), nil
	}
	c.Stdout = f
	c.Stderr = f
	err = c.Start()
	if err != nil {
		return "", err
	}
	stopper.AddCloser(stop.CloserFn(func() {
		_ = c.Process.Kill()
	}))
	err = stopper.RunAsyncTask(ctx, "cmd-wait", func(ctx context.Context) {
		if err := c.Wait(); err != nil {
			log.Infof(ctx, "finished %s with err %s", c.Args, err)
			return
		}
		log.Infof(ctx, "finished %s with success", c.Args)
		stopper.Stop(ctx)
	})
	if err != nil {
		return "", err
	}

	// Wait for the tenant to show healthy
	start := timeutil.Now()
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: transport}
	for {
		time.Sleep(300 * time.Millisecond)
		resp, err := client.Get(fmt.Sprintf("https://%s/health", httpListener.Addr().String()))
		waitTime := timeutil.Since(start)
		if err == nil {
			resp.Body.Close()
			log.Infof(ctx, "tenant is healthy")
			break
		}
		if waitTime > 5*time.Second {
			log.Infof(ctx, "waited more than 5 sec for the tenant to get healthy and it still isn't")
			break
		}
		log.Infof(ctx, "waiting %s for healthy tenant: %s", waitTime, err)
	}
	return sqlAddress, nil
}

type writerFunc func(p []byte) (int, error)

func (wf writerFunc) Write(p []byte) (int, error) { return wf(p) }
