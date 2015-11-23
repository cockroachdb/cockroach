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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Bram Gruneir

// Monitor is a tool designed to occasionally poll an active cluster and save
// the status to disk. The monitor program will exit if the status of the
// cluster can not be determined.

package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
)

const (
	// urlPath is the http path of the status endpoint at each monitored address.
	urlPath = "health"
)

var interval = flag.Duration("interval", 10*time.Second, "Interval in which to poll the status of each monitored address.")
var addrs = flag.String("addrs", ":26257", "Comma-separated list of host:port addresses to monitor.")
var insecure = flag.Bool("insecure", false, "True if using an insecure connection.")
var user = flag.String("user", security.RootUser, "User used to connect to the cluster.")
var certs = flag.String("certs", "certs", "Directory containing RSA key and x509 certs. This flag is required if --insecure=false.")

var retryOptions = retry.Options{
	InitialBackoff: 100 * time.Millisecond,
	MaxRetries:     10,
	Multiplier:     2,
}

type statusMonitor struct {
	addr       string
	url        string
	httpClient *http.Client
	file       *io.Writer
}

func newStatusMonitor(context *base.Context, addr string) (*statusMonitor, error) {
	monitor := &statusMonitor{
		addr: addr,
	}
	var err error
	monitor.httpClient, err = context.GetHTTPClient()
	if err != nil {
		return nil, err
	}
	monitor.url = fmt.Sprintf("%s://%s/%s", context.HTTPRequestScheme(), monitor.addr, urlPath)
	return monitor, nil
}

func (m *statusMonitor) queryStatus() error {
	var queryErr error
	for r := retry.Start(retryOptions); r.Next(); {
		if log.V(1) && queryErr != nil {
			log.Infof("retrying after error: %s", queryErr)
		}

		// Construct a new HTTP GET Request.
		req, err := http.NewRequest("GET", m.url, nil)
		if err != nil {
			queryErr = fmt.Errorf("could not create http request for %s: %s", m.url, err)
			// Break immediately, this is not recoverable.
			break
		}
		req.Header.Set(util.AcceptHeader, util.JSONContentType)

		// Execute request.
		resp, err := m.httpClient.Do(req)
		if err != nil {
			queryErr = fmt.Errorf("could not GET %s - %s", m.url, err)
			continue
		}
		defer resp.Body.Close()

		// Read and verify body of response.
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			queryErr = fmt.Errorf("could not ready body for %s - %s", m.url, err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			queryErr = fmt.Errorf("could not GET %s - statuscode: %d - body: %s", m.url, resp.StatusCode, body)
			continue
		}
		returnedContentType := resp.Header.Get(util.ContentTypeHeader)
		if returnedContentType != util.JSONContentType {
			queryErr = fmt.Errorf("unexpected content type: %v", returnedContentType)
			continue
		}
		return nil
	}
	return queryErr
}

func main() {
	flag.Parse()
	parsedAddrs := strings.Split(*addrs, ",")

	ctx := base.Context{Insecure: *insecure, Certs: *certs, User: *user}

	startTime := time.Now()
	stopper := stop.NewStopper()
	for _, addr := range parsedAddrs {
		client, err := newStatusMonitor(&ctx, addr)
		if err != nil {
			log.Errorf("error creating client: %s", err)
			return
		}
		log.Infof("Monitoring Status URL: %s", client.url)
		stopper.RunWorker(func() {
			timer := time.Tick(*interval)
			for {
				select {
				case <-stopper.ShouldStop():
					return
				case <-timer:
					elapsed := time.Since(startTime)
					if err := client.queryStatus(); err != nil {
						log.Warningf("Could not get status from url %s. Time since monitor started %s.", client.url, elapsed)
						stopper.Stop()
						continue
					}
					log.Infof("Got status from url %s. Time since start: %s", client.url, elapsed)
				}
			}
		})
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, os.Kill, syscall.SIGTERM)
	// Block until a termination signal is received, or the stopper is closed by
	// an error in one of the client routines.
	select {
	case <-stopper.ShouldStop():
		log.Infof("Monitor stopped by error...")
		os.Exit(1)
	case <-signalCh:
		log.Infof("Stopping status monitor...")
		stopper.Stop()
	}
}
