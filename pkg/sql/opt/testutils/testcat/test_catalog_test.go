// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testcat_test

import (
	"testing"
	"time"
)

func init() {
	// Sleep during package initialization, before any tests run
	panic("init")
	//time.Sleep(10 * time.Second)
}

func TestCatalog(t *testing.T) {
	//defer leaktest.AfterTest(t)()
	//
	//datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
	//	catalog := testcat.New()
	//
	//	datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
	//		tester := opttester.New(catalog, d.Input)
	//		return tester.RunCommand(t, d)
	//	})
	//})
	time.Sleep(10)
	t.FailNow()
}
