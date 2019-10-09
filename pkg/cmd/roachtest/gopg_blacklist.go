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

var gopgBlacklists = blacklistsForVersion{
	{
		"v19.2",
		"gopgBlackList19_2",
		gopgBlackList19_2,
		"",
		nil,
	},
	{
		"v20.1",
		"gopgBlackList20_1",
		gopgBlackList20_1,
		"",
		nil,
	},
}

// These are lists of known gopg test errors and failures.
// When the gopg test suite is run, the results are compared to this list.
// Any failed test that is on this list is reported as FAIL - expected.
// Any failed test that is not on this list is reported as FAIL - unexpected.
//
// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blacklist should be available
// in the test log.

var gopgBlackList20_1 = blacklist{
	"CopyFrom/CopyTo/BeforeEach/copies corrupted data to a table":        "5807",
	"CopyFrom/CopyTo/BeforeEach/copies data from a table and to a table": "5807",
	"CountEstimate/It/works":                                                       "17511",
	"CountEstimate/It/works when there are no results":                             "17511",
	"CountEstimate/It/works with GROUP":                                            "17511",
	"CountEstimate/It/works with GROUP when there are no results":                  "17511",
	"DB Prepare/It/returns an error when query can't be prepared":                  "unknown",
	"DB nulls/BeforeEach/nil ptr inserts non-null value":                           "5807",
	"DB nulls/BeforeEach/nil ptr inserts null value":                               "5807",
	"DB nulls/BeforeEach/sql.NullInt64 inserts non-null value":                     "5807",
	"DB nulls/BeforeEach/sql.NullInt64 inserts null value":                         "5807",
	"DB uint64 in struct field/It/is appended and scanned as int64":                "5807",
	"DB.Select/It/selects bytea":                                                   "5807",
	"DB.Select/It/selects into embedded struct pointer":                            "5807",
	"HookTest/BeforeEach/calls AfterSelect for a slice model":                      "5807",
	"HookTest/BeforeEach/calls AfterSelect for a struct model":                     "5807",
	"HookTest/BeforeEach/calls BeforeDelete and AfterDelete":                       "5807",
	"HookTest/BeforeEach/calls BeforeInsert and AfterInsert":                       "5807",
	"HookTest/BeforeEach/calls BeforeUpdate and AfterUpdate":                       "5807",
	"HookTest/BeforeEach/does not call BeforeDelete and AfterDelete for nil model": "5807",
	"HookTest/BeforeEach/does not call BeforeUpdate and AfterUpdate for nil model": "5807",
	"Listener/It/is closed when DB is closed":                                      "unknown",
	"Listener/It/listens for notifications":                                        "unknown",
	"Listener/It/reconnects on receive error":                                      "unknown",
	"Listener/It/returns an error on timeout":                                      "unknown",
	"Listener/It/supports concurrent Listen and Receive":                           "unknown",
	"ORM slice model/It/fetches Book relations":                                    "unknown",
	"ORM slice model/It/fetches Genre relations":                                   "unknown",
	"ORM slice model/It/fetches Translation relation":                              "unknown",
	"ORM struct model/It/fetches Author relations":                                 "unknown",
	"ORM struct model/It/fetches Book relations":                                   "unknown",
	"ORM struct model/It/fetches Genre relations":                                  "unknown",
	"Tx/It/supports CopyFrom and CopyIn":                                           "unknown",
	"soft delete/BeforeEach/model Deleted allows to select deleted model":          "5807",
	"soft delete/BeforeEach/model ForceDelete deletes the model":                   "5807",
	"soft delete/BeforeEach/model soft deletes the model":                          "5807",
	"soft delete/BeforeEach/nil model Deleted allows to select deleted model":      "5807",
	"soft delete/BeforeEach/nil model ForceDelete deletes the model":               "5807",
	"soft delete/BeforeEach/nil model soft deletes the model":                      "5807",
}

var gopgBlackList19_2 = blacklist{
	"CopyFrom/CopyTo/BeforeEach/copies corrupted data to a table":        "5807",
	"CopyFrom/CopyTo/BeforeEach/copies data from a table and to a table": "5807",
	"CountEstimate/It/works":                                                       "17511",
	"CountEstimate/It/works when there are no results":                             "17511",
	"CountEstimate/It/works with GROUP":                                            "17511",
	"CountEstimate/It/works with GROUP when there are no results":                  "17511",
	"DB Prepare/It/returns an error when query can't be prepared":                  "unknown",
	"DB nulls/BeforeEach/nil ptr inserts non-null value":                           "5807",
	"DB nulls/BeforeEach/nil ptr inserts null value":                               "5807",
	"DB nulls/BeforeEach/sql.NullInt64 inserts non-null value":                     "5807",
	"DB nulls/BeforeEach/sql.NullInt64 inserts null value":                         "5807",
	"DB uint64 in struct field/It/is appended and scanned as int64":                "5807",
	"DB.Select/It/selects bytea":                                                   "5807",
	"DB.Select/It/selects into embedded struct pointer":                            "5807",
	"HookTest/BeforeEach/calls AfterSelect for a slice model":                      "5807",
	"HookTest/BeforeEach/calls AfterSelect for a struct model":                     "5807",
	"HookTest/BeforeEach/calls BeforeDelete and AfterDelete":                       "5807",
	"HookTest/BeforeEach/calls BeforeInsert and AfterInsert":                       "5807",
	"HookTest/BeforeEach/calls BeforeUpdate and AfterUpdate":                       "5807",
	"HookTest/BeforeEach/does not call BeforeDelete and AfterDelete for nil model": "5807",
	"HookTest/BeforeEach/does not call BeforeUpdate and AfterUpdate for nil model": "5807",
	"Listener/It/is closed when DB is closed":                                      "unknown",
	"Listener/It/listens for notifications":                                        "unknown",
	"Listener/It/reconnects on receive error":                                      "unknown",
	"Listener/It/returns an error on timeout":                                      "unknown",
	"Listener/It/supports concurrent Listen and Receive":                           "unknown",
	"ORM slice model/It/fetches Book relations":                                    "unknown",
	"ORM slice model/It/fetches Genre relations":                                   "unknown",
	"ORM slice model/It/fetches Translation relation":                              "unknown",
	"ORM struct model/It/fetches Author relations":                                 "unknown",
	"ORM struct model/It/fetches Book relations":                                   "unknown",
	"ORM struct model/It/fetches Genre relations":                                  "unknown",
	"Tx/It/supports CopyFrom and CopyIn":                                           "unknown",
	"soft delete/BeforeEach/model Deleted allows to select deleted model":          "5807",
	"soft delete/BeforeEach/model ForceDelete deletes the model":                   "5807",
	"soft delete/BeforeEach/model soft deletes the model":                          "5807",
	"soft delete/BeforeEach/nil model Deleted allows to select deleted model":      "5807",
	"soft delete/BeforeEach/nil model ForceDelete deletes the model":               "5807",
	"soft delete/BeforeEach/nil model soft deletes the model":                      "5807",
}
