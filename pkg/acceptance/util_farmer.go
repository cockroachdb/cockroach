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
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/acceptance/terrafarm"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	cl "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

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
		stores += fmt.Sprintf(" --store=/mnt/data%d/cockroach-data", j)
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
	}, clientClock, stopper, &cl.MakeTestingClusterSettings().Version)
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
