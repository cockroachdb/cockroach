// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package allccl

import (
	"bytes"
	"log"
	"os"
	"strings"
	"text/template"

	_ "github.com/cockroachdb/cockroach/pkg/ccl/workloadccl/allccl" // Register all workloads.
	"github.com/cockroachdb/cockroach/pkg/workload"
)

// GenerateTest generates test.
func GenerateTest(dir string, prefix string) {
	f, err := os.Create(dir + "all_registered_setup_test.go")
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()

	_, err = f.WriteString(prefix)
	if err != nil {
		log.Fatalln(err)
	}

	_, err = f.WriteString(packageHeader)
	if err != nil {
		log.Fatalln(err)
	}

	t, err := template.New("test").Funcs(template.FuncMap{"Title": strings.Title}).Parse(testTmpl)
	if err != nil {
		log.Fatalln(err)
	}
	for _, meta := range workload.Registered() {
		var b bytes.Buffer

		err := t.Execute(&b, testData{meta.Name})
		if err != nil {
			log.Panicln(err)
		}

		_, err = f.Write(b.Bytes())
		if err != nil {
			log.Fatalln(err)
		}
	}
}

type testData struct {
	Name string
}

var packageHeader = `
package allccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
)
`

var testTmpl = `
func Test{{.Name|Title}}Setup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	meta, err := workload.Get("{{.Name}}")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if bigInitialData(meta) {
		return
	}

	// This test is big enough that it causes timeout issues under race, so only
	// run one workload. Doing any more than this doesn't get us enough to be
	// worth the hassle.
	if util.RaceEnabled && meta.Name != "bank" {
		return
	}

	gen := meta.New()
	switch meta.Name {
	case "roachmart":
		// TODO(dan): It'd be nice to test this with the default flags. For now,
		// this is better than nothing.
		if err := gen.(workload.Flagser).Flags().Parse([]string{
			"--users=10", "--orders=100", "--partition=false",
		}); err != nil {
			t.Fatal(err)
		}
	case "interleavedpartitioned":
		// This require a specific node locality setup
		return
	}

	t.Run(meta.Name, func(t *testing.T) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
		})
		defer s.Stopper().Stop(ctx)
		sqlutils.MakeSQLRunner(db).Exec(t, "CREATE DATABASE d")
		sqlutils.MakeSQLRunner(db).Exec(t, "SET CLUSTER SETTING kv.range_merge.queue_enabled = false")

		var l workloadsql.InsertsDataLoader
		if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
			t.Fatalf("%+v", err)
		}

		// Run the consistency check if this workload has one.
		if h, ok := gen.(workload.Hookser); ok {
			if checkConsistencyFn := h.Hooks().CheckConsistency; checkConsistencyFn != nil {
				if err := checkConsistencyFn(ctx, db); err != nil {
					t.Errorf("%+v", err)
				}
			}
		}
	})
}
`
