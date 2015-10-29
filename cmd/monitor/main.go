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
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
)

var outputDir = flag.String("dir", ".", "Directory in which to output the status logs to.")
var interval = flag.Duration("interval", 10*time.Second, "Interval in which to poll the cluster's status.")
var addr = flag.String("addr", ":26257", "The host:port of the cockroach cluster.")
var insecure = flag.Bool("insecure", false, "True if using an insecure connection.")
var user = flag.String("user", security.RootUser, "User used to connect to the cluster.")
var certs = flag.String("certs", "certs", "Directory containing RSA key and x509 certs. This flag is required if --insecure=false.")
var endpoint = flag.String("endpoint", "_status/nodes", "Status endpoint to monitor and log.")

var retryOptions = retry.Options{
	InitialBackoff: 100 * time.Millisecond,
	MaxRetries:     10,
	Multiplier:     2,
}

// request returns the result of performing a http get request.
func request(url string, httpClient *http.Client) ([]byte, bool) {
	for r := retry.Start(retryOptions); r.Next(); {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Fatal(err)
			return nil, false
		}
		req.Header.Set(util.AcceptHeader, util.JSONContentType)
		resp, err := httpClient.Do(req)
		if err != nil {
			log.Infof("could not GET %s - %s", url, err)
			continue
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Infof("could not ready body for %s - %s", url, err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			log.Infof("could not GET %s - statuscode: %d - body: %s", url, resp.StatusCode, body)
			continue
		}
		returnedContentType := resp.Header.Get(util.ContentTypeHeader)
		if returnedContentType != util.JSONContentType {
			log.Infof("unexpected content type: %v", returnedContentType)
			continue
		}
		log.Infof("OK response from %s", url)
		return body, true
	}
	log.Warningf("There was an error retrieving %s", url)
	return nil, false
}

func main() {
	flag.Parse()

	ctx := base.Context{Insecure: *insecure, Certs: *certs, User: *user}
	httpClient, err := ctx.GetHTTPClient()
	if err != nil {
		panic(err)
	}

	startTime := time.Now()
	file := filepath.Join(*outputDir, fmt.Sprintf("monitor.%s", strings.Replace(
		startTime.Format(time.RFC3339), ":", "_", -1)))
	log.Infof("Logging cluster status to: %s.\n", file)
	w, err := os.Create(file)
	if err != nil {
		panic(err)
	}
	defer w.Close()

	url := fmt.Sprintf("%s://%s/%s", ctx.HTTPRequestScheme(), *addr, *endpoint)
	log.Infof("Cluster Status URL: %s\n", url)

	for range time.Tick(*interval) {
		resp, found := request(url, httpClient)
		if !found {
			log.Warningf("Could not get cluster status. Time since monitor started %s.", time.Since(startTime))
			break
		}
		log.Infof("Got cluster status.")
		fmt.Fprintf(w, "%s\n", resp)
	}
}
