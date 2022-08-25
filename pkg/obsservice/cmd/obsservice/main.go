// Copyright 2022 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/httpproxy"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/ingest"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/leasing"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/migrations"
	_ "github.com/cockroachdb/cockroach/pkg/ui/distoss" // web UI init hooks
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

// drainSignals are the signals that will cause the server to drain and exit.
//
// The signals will initiate a graceful shutdown. If received a second time,
// SIGINT will be reraised without a signal handler and the default action
// terminate the process abruptly.
//
// Receiving SIGTERM a second time does not do a brutal shutdown, as SIGTERM is
// named termSignal below.
var drainSignals = []os.Signal{unix.SIGINT, unix.SIGTERM}

// termSignal is the signal that causes an idempotent graceful
// shutdown (i.e. second occurrence does not incur hard shutdown).
var termSignal os.Signal = unix.SIGTERM

// defaultSinkDBName is the name of the database to be used by default.
const defaultSinkDBName = "obsservice"

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "obsservice",
	Short: "An observability service for CockroachDB",
	Long: `The Observability Service ingests monitoring and observability data 
from one or more CockroachDB clusters.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		cfg := httpproxy.ReverseHTTPProxyConfig{
			HTTPAddr:      httpAddr,
			TargetURL:     targetURL,
			CACertPath:    caCertPath,
			UICertPath:    uiCertPath,
			UICertKeyPath: uiCertKeyPath,
		}

		connCfg, err := pgxpool.ParseConfig(sinkPGURL)
		if err != nil {
			return errors.Wrapf(err, "invalid value for --sink-pgurl: %q", sinkPGURL)
		}
		if connCfg.ConnConfig.Database == "" {
			fmt.Printf("No database explicitly provided in --sink-pgurl. Using %q.\n", defaultSinkDBName)
			connCfg.ConnConfig.Database = defaultSinkDBName
		}

		pool, err := pgxpool.ConnectConfig(ctx, connCfg)
		if err != nil {
			return errors.Wrapf(err, "failed to connect to sink database %q", sinkPGURL)
		}

		// TODO(andrei): Figure out how to handle races between multiple nodes
		// attempting to perform migrations at the same time.
		if err := migrations.RunDBMigrations(ctx, connCfg.ConnConfig); err != nil {
			return errors.Wrapf(err, "failed to run DB migrations")
		}

		errOnFailureToLock := true
		if sessionIDFile == "" {
			errOnFailureToLock = false
			sessionIDFile = defaultSessionIDFile
		}
		sessionID, cleanup, err := readSessionID(ctx, sessionIDFile, errOnFailureToLock)
		if cleanup != nil {
			defer cleanup()
		}
		if err != nil {
			return err
		}

		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, drainSignals...)

		stop := stop.NewStopper()

		// Run the event ingestion in the background.
		if eventsAddr != "" {
			clock := timeutil.DefaultTimeSource{}
			leaseMgr := leasing.NewSession(sessionID, pool, clock, stop)
			if err := leaseMgr.Start(); err != nil {
				return err
			}

			ingester := ingest.MakeEventIngester(pool, stop, leaseMgr)
			// TODO(andrei): Figure out the targetID somehow.
			if err := ingester.StartIngestEvents(ctx, 0 /* targetID */, eventsAddr); err != nil {
				return err
			}
		}
		// Run the reverse HTTP proxy in the background.
		if err := httpproxy.NewReverseHTTPProxy(ctx, cfg).Start(ctx, stop); err != nil {
			return err
		}

		// Block until the process is signaled to terminate.
		sig := <-signalCh
		log.Infof(ctx, "received signal %s. Shutting down.", sig)
		go func() {
			stop.Stop(ctx)
		}()

		// Print the shutdown progress every 5 seconds.
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					log.Infof(ctx, "%d running tasks", stop.NumTasks())
				case <-stop.IsStopped():
					return
				}
			}
		}()

		// Wait until the shutdown is complete or we receive another signal.
		select {
		case <-stop.IsStopped():
			log.Infof(ctx, "shutdown complete")
		case sig = <-signalCh:
			switch sig {
			case termSignal:
				log.Infof(ctx, "received SIGTERM while shutting down. Continuing shutdown.")
			default:
				// Crash.
				handleSignalDuringShutdown(sig)
			}
		}
		return nil
	},
}

// Flags.
var (
	httpAddr                  string
	targetURL                 string
	caCertPath                string
	uiCertPath, uiCertKeyPath string
	sinkPGURL                 string
	eventsAddr                string
	sessionIDFile             string
)

// defaultSessionIDFile represents the path that will be used
// for the id file if --id-file is not specified.
var defaultSessionIDFile = os.TempDir() + string(os.PathSeparator) + "obs-svc.id"

func main() {
	// Add all the flags registered with the standard "flag" package. Useful for
	// --vmodule, for example.
	RootCmd.PersistentFlags().AddGoFlagSet(flag.CommandLine)

	RootCmd.PersistentFlags().StringVar(
		&httpAddr,
		"http-addr",
		"localhost:8081",
		"The address on which to listen for HTTP requests.")
	RootCmd.PersistentFlags().StringVar(
		&targetURL,
		"crdb-http-url",
		"http://localhost:8080",
		"The base URL to which HTTP requests are proxied.")
	RootCmd.PersistentFlags().StringVar(
		&caCertPath,
		"ca-cert",
		"",
		"Path to the certificate authority certificate file. If specified,"+
			" HTTP requests are only proxied to CRDB nodes that present certificates signed by this CA."+
			" If not specified, the system's CA list is used.")
	RootCmd.PersistentFlags().StringVar(
		&uiCertPath,
		"ui-cert",
		"",
		"Path to the certificate used used by the Observability Service.")
	RootCmd.PersistentFlags().StringVar(
		&uiCertKeyPath,
		"ui-cert-key",
		"",
		"Path to the private key used by the Observability Service. "+
			"This is the key corresponding to the --ui-cert certificate.")

	// Flags about connecting to the sink cluster.
	RootCmd.PersistentFlags().StringVar(
		&sinkPGURL,
		"sink-pgurl",
		"postgresql://root@localhost:26257?sslmode=disable",
		"PGURL for the sink cluster. If the url does not include a database name, "+
			"then \"obsservice\" will be used.")

	RootCmd.PersistentFlags().StringVar(
		&eventsAddr,
		"crdb-events-addr",
		"localhost:26257",
		"Address of a CRDB node that events will be ingested from.")

	RootCmd.PersistentFlags().StringVar(
		&sessionIDFile,
		"session-id-file",
		"",
		"Path to a file used to store an identity of this Observability Service "+
			"node in between process restarts. When a node is restarted, using the previous identity is beneficial "+
			"so that monitoring leases held before the restart can be reused (if still valid) without having "+
			"to wait for them to expire.\n"+
			"If not specified, "+defaultSessionIDFile+" is used; however, if this file is locked by another "+
			"process, no identity file is used so that multiple processes can run on the same machine without "+
			"needing this file to be specified.")
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		exit.WithCode(exit.UnspecifiedError())
	}
}

func handleSignalDuringShutdown(sig os.Signal) {
	// On Unix, a signal that was not handled gracefully by the application
	// should be reraised so it is visible in the exit code.

	// Reset signal to its original disposition.
	signal.Reset(sig)

	// Reraise the signal. os.Signal is always sysutil.Signal.
	if err := unix.Kill(unix.Getpid(), sig.(sysutil.Signal)); err != nil {
		// Sending a valid signal to ourselves should never fail.
		//
		// Unfortunately it appears (#34354) that some users
		// run CockroachDB in containers that only support
		// a subset of all syscalls. If this ever happens, we
		// still need to quit immediately.
		log.Fatalf(context.Background(), "unable to forward signal %v: %v", sig, err)
	}

	// Block while we wait for the signal to be delivered.
	select {}
}

// readSessionID attempts to lock and read a session ID from sessionIDFile.
// errOnFailureToLock controls what happens if the file is locked by another
// process: return an error, or continue to generate a new session ID.
//
// If the file can be locked and is empty, a session ID is generated and written
// to the file.
func readSessionID(
	ctx context.Context, sessionIDFile string, errOnFailureToLock bool,
) (sessionID uuid.UUID, cleanup func(), _ error) {
	lockedFile, cleanup, err := tryFilesystemLock(sessionIDFile)
	if err != nil {
		return uuid.Nil, nil, err
	}
	if cleanup != nil {
		defer cleanup()
	}
	if lockedFile {
		// If I managed to lock the file, we attempt to read the ID from it.
		sessionID, lockedFile, err = readSessionIDFromFile(sessionIDFile)
		if err != nil {
			return uuid.Nil, cleanup, err
		}
		if lockedFile {
			fmt.Printf("Found session ID: %s.\n", sessionID)
		}
	} else {
		// We didn't manage to lock the file; another node must be running locally.
		if errOnFailureToLock {
			return uuid.Nil, cleanup, errors.Newf("failed to lock %q and --id-file specified; is another process running?", sessionIDFile)
		}
		fmt.Printf("Another node appears to be running locally; not reusing leasing ID.\n")
	}

	// If we failed to read a session ID, generate one now and maybe write it to
	// the file.
	if sessionID == uuid.Nil {
		sessionID = uuid.MakeV4()
		if lockedFile {
			// Save the ID to the lock file.
			if err := writeSessionIDToFile(sessionIDFile, sessionID); err != nil {
				log.Warningf(ctx, "error writing leasing ID to lock file: %s", err)
			}
		}
	}
	return sessionID, cleanup, nil
}

// tryFilesystemLock attempts to acquire a filesystem lock on lockFile. The call is non-blocking
func tryFilesystemLock(lockFile string) (ok bool, unlockFn func(), _ error) {
	f, err := os.OpenFile(lockFile, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return false, nil, errors.Wrapf(err, "creating lock file %q", lockFile)
	}
	if err := unix.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return false, nil, nil
		}
		return false, nil, errors.Wrapf(err, "acquiring lock on %q", lockFile)
	}
	return true, func() { _ = f.Close() }, nil
}

func readSessionIDFromFile(lockfile string) (uuid.UUID, bool, error) {
	buf, err := os.ReadFile(lockfile)
	if err != nil {
		return uuid.Nil, false, err
	}
	if len(buf) == 0 {
		return uuid.Nil, false, nil
	}
	s := strings.TrimSpace(string(buf))
	id, err := uuid.FromString(s)
	if err != nil {
		return uuid.Nil, false, err
	}
	return id, true, err
}

func writeSessionIDToFile(lockfile string, id uuid.UUID) error {
	return os.WriteFile(lockfile, []byte(id.String()), 0666)
}
