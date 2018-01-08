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
	gosql "database/sql"
	"os"
	"os/signal"
	"strings"
	"testing"

	"github.com/docker/docker/pkg/namesgenerator"
	// Import postgres driver.
	_ "github.com/lib/pq"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

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

// getRandomName generates a random, human-readable name to ease identification
// of different test resources.
func getRandomName() string {
	// Remove characters that aren't allowed in hostnames for machines allocated
	// by Terraform.
	return strings.Replace(namesgenerator.GetRandomName(0), "_", "", -1)
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
