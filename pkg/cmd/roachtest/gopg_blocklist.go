// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

var gopgBlocklists = blocklistsForVersion{
	{"v19.2", "gopgBlockList19_2", gopgBlockList19_2, "gopgIgnoreList19_2", gopgIgnoreList19_2},
	{"v20.1", "gopgBlockList20_1", gopgBlockList20_1, "gopgIgnoreList20_1", gopgIgnoreList20_1},
	{"v20.2", "gopgBlockList20_2", gopgBlockList20_2, "gopgIgnoreList20_2", gopgIgnoreList20_2},
}

// These are lists of known gopg test errors and failures.
// When the gopg test suite is run, the results are compared to this list.
// Any failed test that is on this list is reported as FAIL - expected.
// Any failed test that is not on this list is reported as FAIL - unexpected.
//
// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blocklist should be available
// in the test log.

var gopgBlockList20_2 = gopgBlockList20_1

var gopgBlockList20_1 = blocklist{
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
	"v9.ExampleDB_Model_postgresArrayStructTag":                       "32552",
	"v9.TestBigColumn":  "41608",
	"v9.TestConversion": "32552",
	"v9.TestGinkgo":     "41522",
	"v9.TestGocheck":    "17511",
	"v9.TestUnixSocket": "31113",
}

var gopgBlockList19_2 = blocklist{
	"pg | CopyFrom/CopyTo | copies corrupted data to a table":                              "5807",
	"pg | CopyFrom/CopyTo | copies data from a table and to a table":                       "5807",
	"pg | CountEstimate | works":                                                           "17511",
	"pg | CountEstimate | works when there are no results":                                 "17511",
	"pg | CountEstimate | works with GROUP":                                                "17511",
	"pg | CountEstimate | works with GROUP when there are no results":                      "17511",
	"pg | DB nulls | nil ptr inserts non-null value":                                       "5807",
	"pg | DB nulls | nil ptr inserts null value":                                           "5807",
	"pg | DB nulls | sql.NullInt64 inserts non-null value":                                 "5807",
	"pg | DB nulls | sql.NullInt64 inserts null value":                                     "5807",
	"pg | DB uint64 in struct field | is appended and scanned as int64":                    "5807",
	"pg | DB.Select | selects bytea":                                                       "5807",
	"pg | DB.Select | selects into embedded struct pointer":                                "5807",
	"pg | HookTest | calls AfterSelect for a slice model":                                  "5807",
	"pg | HookTest | calls AfterSelect for a struct model":                                 "5807",
	"pg | HookTest | calls BeforeDelete and AfterDelete":                                   "5807",
	"pg | HookTest | calls BeforeInsert and AfterInsert":                                   "5807",
	"pg | HookTest | calls BeforeUpdate and AfterUpdate":                                   "5807",
	"pg | HookTest | does not call BeforeDelete and AfterDelete for nil model":             "5807",
	"pg | HookTest | does not call BeforeUpdate and AfterUpdate for nil model":             "5807",
	"pg | Listener | is closed when DB is closed":                                          "41522",
	"pg | Listener | listens for notifications":                                            "41522",
	"pg | Listener | reconnects on receive error":                                          "41522",
	"pg | Listener | returns an error on timeout":                                          "41522",
	"pg | Listener | supports concurrent Listen and Receive":                               "41522",
	"pg | soft delete with int column | model Deleted allows to select deleted model":      "5807",
	"pg | soft delete with int column | model ForceDelete deletes the model":               "5807",
	"pg | soft delete with int column | model soft deletes the model":                      "5807",
	"pg | soft delete with int column | nil model Deleted allows to select deleted model":  "5807",
	"pg | soft delete with int column | nil model ForceDelete deletes the model":           "5807",
	"pg | soft delete with int column | nil model soft deletes the model":                  "5807",
	"pg | soft delete with time column | model Deleted allows to select deleted model":     "5807",
	"pg | soft delete with time column | model ForceDelete deletes the model":              "5807",
	"pg | soft delete with time column | model soft deletes the model":                     "5807",
	"pg | soft delete with time column | nil model Deleted allows to select deleted model": "5807",
	"pg | soft delete with time column | nil model ForceDelete deletes the model":          "5807",
	"pg | soft delete with time column | nil model soft deletes the model":                 "5807",
	"v9.ExampleDB_Model_postgresArrayStructTag":                                            "5807",
	"v9.TestBigColumn":  "5807",
	"v9.TestConversion": "32552",
	"v9.TestGinkgo":     "5807",
	"v9.TestGocheck":    "5807",
	"v9.TestUnixSocket": "31113",
}

var gopgIgnoreList20_2 = gopgIgnoreList20_1

var gopgIgnoreList20_1 = gopgIgnoreList19_2

var gopgIgnoreList19_2 = blocklist{
	// These "fetching" tests assume a particular order when ORDER BY clause is
	// omitted from the query by the ORM itself.
	"pg | ORM slice model | fetches Book relations":       "41690",
	"pg | ORM slice model | fetches Genre relations":      "41690",
	"pg | ORM slice model | fetches Translation relation": "41690",
	"pg | ORM struct model | fetches Author relations":    "41690",
	"pg | ORM struct model | fetches Book relations":      "41690",
	"pg | ORM struct model | fetches Genre relations":     "41690",
	// These tests assume different transaction isolation level (READ COMMITTED).
	"pg | Tx | supports CopyFrom and CopyIn":             "41690",
	"pg | Tx | supports CopyFrom and CopyIn with errors": "41690",
	// These tests sometimes failed and we haven't diagnosed it
	"pg | DB race | SelectOrInsert with OnConflict is race free":    "unknown",
	"pg | DB race | SelectOrInsert without OnConflict is race free": "unknown",
}
