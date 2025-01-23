// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

// These are lists of known gopg test errors and failures.
// When the gopg test suite is run, the results are compared to this list.
// Any failed test that is on this list is reported as FAIL - expected.
// Any failed test that is not on this list is reported as FAIL - unexpected.
//
// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blocklist should be available
// in the test log.
var gopgBlockList = blocklist{
	"pg | CopyFrom/CopyTo | copies corrupted data to a table":         "41608",
	"pg | CopyFrom/CopyTo | copies data from a table and to a table":  "41608",
	"pg | CountEstimate | works":                                      "17511",
	"pg | CountEstimate | works when there are no results":            "17511",
	"pg | CountEstimate | works with GROUP":                           "17511",
	"pg | CountEstimate | works with GROUP when there are no results": "17511",
	"pg | Listener | is closed when DB is closed":                     "41522",
	"pg | Listener | listens for notifications":                       "41522",
	"pg | Listener | reconnects on receive error":                     "41522",
	"pg | Listener | returns an error on timeout":                     "41522",
	"pg | Listener | supports concurrent Listen and Receive":          "41522",
	"v10.ExampleDB_Model_postgresArrayStructTag":                      "32552",
	"v10.TestConversion":                                              "32552",
	"v10.TestGinkgo":                                                  "41522",
	"v10.TestReadColumnValue":                                         "26925",
	"v10.TestUnixSocket":                                              "31113",
}

var gopgIgnoreList = blocklist{
	// These "fetching" tests assume a particular order when ORDER BY clause is
	// omitted from the query by the ORM itself.
	"pg | ORM slice model | fetches Book relations":       "41690",
	"pg | ORM slice model | fetches Genre relations":      "41690",
	"pg | ORM slice model | fetches Translation relation": "41690",
	"pg | ORM struct model | fetches Author relations":    "41690",
	"pg | ORM struct model | fetches Book relations":      "41690",
	"pg | ORM struct model | fetches Genre relations":     "41690",
	// Different error message for context cancellation timeout.
	"pg | OnConnect | does not panic on timeout": "41690",
	// These tests assume different transaction isolation level (READ COMMITTED).
	"pg | Tx | supports CopyFrom and CopyIn":             "41690",
	"pg | Tx | supports CopyFrom and CopyIn with errors": "41690",
	// These tests sometimes failed and we haven't diagnosed it
	"pg | DB race | SelectOrInsert with OnConflict is race free":    "unknown",
	"pg | DB race | SelectOrInsert without OnConflict is race free": "unknown",
	`pg | ORM | relation with no results does not panic`:            "unknown",
	// This test flakes sometimes because of connection reuse.
	`v10.TestColumnReuse`: "unknown",
}
