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

var pgxBlocklists = blocklistsForVersion{
	{"v19.2", "pgxBlocklist19_2", pgxBlocklist19_2, "pgxIgnorelist19_2", pgxIgnorelist19_2},
	{"v20.1", "pgxBlocklist20_1", pgxBlocklist20_1, "pgxIgnorelist20_1", pgxIgnorelist20_1},
	{"v20.2", "pgxBlocklist20_2", pgxBlocklist20_2, "pgxIgnorelist20_2", pgxIgnorelist20_2},
}

// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blocklist should be available
// in the test log.
var pgxBlocklist20_2 = pgxBlocklist20_1

var pgxBlocklist20_1 = blocklist{
	"v4.Example_CustomType":                                                   "27796",
	"v4.TestConnBeginBatchDeferredError":                                      "31632",
	"v4.TestConnCopyFromEnum":                                                 "unknown",
	"v4.TestConnCopyFromJSON":                                                 "19603",
	"v4.TestConnCopyFromLarge":                                                "19603",
	"v4.TestConnCopyFromSmall":                                                "19603",
	"v4.TestConnQueryDeferredError":                                           "31632",
	"v4.TestConnQueryErrorWhileReturningRows":                                 "26925",
	"v4.TestConnQueryReadRowMultipleTimes":                                    "26925",
	"v4.TestConnQueryValues":                                                  "26925",
	"v4.TestConnQueryValuesWithUnknownOID":                                    "26925",
	"v4.TestConnSendBatch":                                                    "44712",
	"v4.TestConnSendBatchWithPreparedStatement":                               "41558",
	"v4.TestConnSimpleProtocol":                                               "21286",
	"v4.TestConnSimpleProtocolRefusesNonStandardConformingStrings":            "36215",
	"v4.TestConnSimpleProtocolRefusesNonUTF8ClientEncoding":                   "37129",
	"v4.TestDomainType":                                                       "27796",
	"v4.TestDomainType/DefaultProto":                                          "unknown",
	"v4.TestDomainType/SimpleProto":                                           "unknown",
	"v4.TestFatalRxError":                                                     "35897",
	"v4.TestFatalTxError":                                                     "35897",
	"v4.TestInetCIDRArrayTranscodeIP":                                         "18846",
	"v4.TestInetCIDRArrayTranscodeIP/DefaultProto":                            "unknown",
	"v4.TestInetCIDRArrayTranscodeIP/SimpleProto":                             "unknown",
	"v4.TestInetCIDRArrayTranscodeIPNet":                                      "18846",
	"v4.TestInetCIDRArrayTranscodeIPNet/DefaultProto":                         "unknown",
	"v4.TestInetCIDRArrayTranscodeIPNet/SimpleProto":                          "unknown",
	"v4.TestInetCIDRTranscodeIP":                                              "18846",
	"v4.TestInetCIDRTranscodeIP/DefaultProto":                                 "unknown",
	"v4.TestInetCIDRTranscodeIP/SimpleProto":                                  "unknown",
	"v4.TestInetCIDRTranscodeIPNet":                                           "18846",
	"v4.TestInetCIDRTranscodeIPNet/DefaultProto":                              "unknown",
	"v4.TestInetCIDRTranscodeIPNet/SimpleProto":                               "unknown",
	"v4.TestInetCIDRTranscodeWithJustIP":                                      "18846",
	"v4.TestInetCIDRTranscodeWithJustIP/DefaultProto":                         "unknown",
	"v4.TestInetCIDRTranscodeWithJustIP/SimpleProto":                          "unknown",
	"v4.TestInsertBoolArray":                                                  "unknown",
	"v4.TestInsertBoolArray/SimpleProto":                                      "unknown",
	"v4.TestInsertTimestampArray":                                             "unknown",
	"v4.TestInsertTimestampArray/SimpleProto":                                 "unknown",
	"v4.TestLargeObjects":                                                     "26725",
	"v4.TestLargeObjectsMultipleTransactions":                                 "26725",
	"v4.TestLargeObjectsPreferSimpleProtocol":                                 "26725",
	"v4.TestListenNotify":                                                     "41522",
	"v4.TestListenNotifySelfNotification":                                     "41522",
	"v4.TestListenNotifyWhileBusyIsSafe":                                      "41522",
	"v4.TestQueryContextErrorWhileReceivingRows":                              "26925",
	"v4.TestRowDecodeBinary":                                                  "unknown",
	"v4.TestSendBatchSimpleProtocol":                                          "unknown",
	"v4.TestTransactionSuccessfulCommit":                                      "31632",
	"v4.TestTransactionSuccessfulRollback":                                    "31632",
	"v4.TestTxCommitSerializationFailure":                                     "12701",
	"v4.TestTxCommitWhenTxBroken":                                             "31632",
	"v4.TestTxNestedTransactionCommit":                                        "31632",
	"v4.TestTxNestedTransactionRollback":                                      "31632",
	"v4.TestUnregisteredTypeUsableAsStringArgumentAndBaseResult":              "27796",
	"v4.TestUnregisteredTypeUsableAsStringArgumentAndBaseResult/DefaultProto": "unknown",
	"v4.TestUnregisteredTypeUsableAsStringArgumentAndBaseResult/SimpleProto":  "unknown",
}

var pgxIgnorelist20_2 = pgxIgnorelist20_1

var pgxIgnorelist20_1 = blocklist{
	"v4.TestBeginIsoLevels":   "We don't support isolation levels",
	"v4.TestQueryEncodeError": "This test checks the exact error message",
}

var pgxBlocklist19_2 = blocklist{
	"v4.Example_CustomType":                                                   "27796",
	"v4.TestConnBeginBatchDeferredError":                                      "31632",
	"v4.TestConnCopyFromCopyFromSourceErrorEnd":                               "5807",
	"v4.TestConnCopyFromCopyFromSourceErrorMidway":                            "5807",
	"v4.TestConnCopyFromFailServerSideMidway":                                 "5807",
	"v4.TestConnCopyFromFailServerSideMidwayAbortsWithoutWaiting":             "5807",
	"v4.TestConnCopyFromJSON":                                                 "5807",
	"v4.TestConnCopyFromLarge":                                                "5807",
	"v4.TestConnCopyFromSlowFailRace":                                         "5807",
	"v4.TestConnCopyFromSmall":                                                "5807",
	"v4.TestConnQueryDatabaseSQLDriverValuerWithAutoGeneratedPointerReceiver": "5807",
	"v4.TestConnQueryDeferredError":                                           "31632",
	"v4.TestConnQueryErrorWhileReturningRows":                                 "26925",
	"v4.TestConnQueryReadRowMultipleTimes":                                    "26925",
	"v4.TestConnQueryValues":                                                  "26925",
	"v4.TestConnQueryValuesWithUnknownOID":                                    "24873",
	"v4.TestConnQueryWithoutResultSetCommandTag":                              "5807",
	"v4.TestConnSendBatch":                                                    "5807",
	"v4.TestConnSendBatchMany":                                                "5807",
	"v4.TestConnSendBatchQueryPartialReadInsert":                              "5807",
	"v4.TestConnSendBatchQueryRowInsert":                                      "5807",
	"v4.TestConnSendBatchWithPreparedStatement":                               "41558",
	"v4.TestConnSimpleProtocol":                                               "21286",
	"v4.TestConnSimpleProtocolRefusesNonStandardConformingStrings":            "36215",
	"v4.TestConnSimpleProtocolRefusesNonUTF8ClientEncoding":                   "37129",
	"v4.TestDomainType":                                                       "27796",
	"v4.TestExec":                                                             "5807",
	"v4.TestExecContextWithoutCancelation":                                    "5807",
	"v4.TestExecExtendedProtocol":                                             "5807",
	"v4.TestExecSimpleProtocol":                                               "5807",
	"v4.TestFatalRxError":                                                     "35897",
	"v4.TestFatalTxError":                                                     "35897",
	"v4.TestInetCIDRArrayTranscodeIP":                                         "18846",
	"v4.TestInetCIDRArrayTranscodeIPNet":                                      "18846",
	"v4.TestInetCIDRTranscodeIP":                                              "18846",
	"v4.TestInetCIDRTranscodeIPNet":                                           "18846",
	"v4.TestInetCIDRTranscodeWithJustIP":                                      "18846",
	"v4.TestInsertBoolArray":                                                  "5807",
	"v4.TestInsertTimestampArray":                                             "5807",
	"v4.TestLargeObjects":                                                     "26725",
	"v4.TestLargeObjectsMultipleTransactions":                                 "26725",
	"v4.TestLargeObjectsPreferSimpleProtocol":                                 "26725",
	"v4.TestListenNotify":                                                     "41522",
	"v4.TestListenNotifySelfNotification":                                     "41522",
	"v4.TestListenNotifyWhileBusyIsSafe":                                      "41522",
	"v4.TestQueryContextErrorWhileReceivingRows":                              "26925",
	"v4.TestRowDecode":                                                        "26925",
	"v4.TestTransactionSuccessfulCommit":                                      "31632",
	"v4.TestTransactionSuccessfulRollback":                                    "31632",
	"v4.TestTxCommitSerializationFailure":                                     "5807",
	"v4.TestTxCommitWhenTxBroken":                                             "31632",
	"v4.TestTxNestedTransactionCommit":                                        "31632",
	"v4.TestTxNestedTransactionRollback":                                      "31632",
	"v4.TestTxSendBatch":                                                      "5807",
	"v4.TestTxSendBatchRollback":                                              "5807",
	"v4.TestUnregisteredTypeUsableAsStringArgumentAndBaseResult":              "27796",
}

var pgxIgnorelist19_2 = blocklist{
	"v4.TestBeginIsoLevels":   "We don't support isolation levels",
	"v4.TestQueryEncodeError": "This test checks the exact error message",
}
