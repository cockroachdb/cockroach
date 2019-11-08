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

// As of now, we only run a subset of the test apps within the django
// testing suite. The full set we run is below, and should be kept
// in alphabetical order. As more progress is made with adding compatibility,
// more test apps should be added here to prevent against regression.
var enabledDjangoTests = []string{
	"app_loading",
	"apps",
	// This is a really large test. Uncomment for later runs.
	// "auth_tests",
	"backends",
	"base",
	"bash_completion",
	"update",
}

var djangoBlacklists = blacklistsForVersion{
	{"v19.2", "djangoBlacklist19_2", djangoBlacklist19_2, "djangoIgnoreList19_2", djangoIgnoreList19_2},
	{"v20.1", "djangoBlacklist20_1", djangoBlacklist20_1, "djangoIgnoreList20_1", djangoIgnoreList20_1},
}

// Maintain that this list is alphabetized.
// TODO (rohany): The django tests are very flaky right now, so the blacklist
//  doesn't stay consistent across runs.
var djangoBlacklist20_1 = blacklist{
	// Blacklist generated from running the tests above.
	"app_loading.tests.GetModelsTest.test_get_models_only_returns_installed_models":    "unknown",
	"apps.tests.AppsTests.test_models_not_loaded":                                      "unknown",
	"backends.base.test_base.ExecuteWrapperTests.test_wrapper_connection_specific":     "unknown",
	"backends.tests.DateQuotingTest.test_django_date_trunc":                            "unknown",
	"backends.tests.FkConstraintsTests.test_check_constraints":                         "unknown",
	"backends.tests.FkConstraintsTests.test_disable_constraint_checks_context_manager": "unknown",
	"backends.tests.FkConstraintsTests.test_disable_constraint_checks_manually":        "unknown",
	"backends.tests.ThreadTests.test_connections_thread_local":                         "unknown",
	"update.tests.SimpleTest.test_nonempty_update_with_inheritance":                    "unknown",
	// TODO (rohany): The postgres_tests suite within Django is not in a automatically
	//  runnable state right now.
	//"postgres_tests.test_aggregates.TestGeneralAggregate.test_bit_and_empty_result":                                 "41334",
	//"postgres_tests.test_aggregates.TestGeneralAggregate.test_bit_and_general":                                      "41334",
	//"postgres_tests.test_aggregates.TestGeneralAggregate.test_bit_and_on_only_false_values":                         "41334",
	//"postgres_tests.test_aggregates.TestGeneralAggregate.test_bit_and_on_only_true_values":                          "41334",
	//"postgres_tests.test_aggregates.TestGeneralAggregate.test_bit_or_empty_result":                                  "41334",
	//"postgres_tests.test_aggregates.TestGeneralAggregate.test_bit_or_general":                                       "41334",
	//"postgres_tests.test_aggregates.TestGeneralAggregate.test_bit_or_on_only_false_values":                          "41334",
	//"postgres_tests.test_aggregates.TestGeneralAggregate.test_bit_or_on_only_true_values":                           "41334",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_corr_empty_result":                                 "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_corr_general":                                      "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_covar_pop_empty_result":                            "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_covar_pop_general":                                 "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_covar_pop_sample":                                  "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_covar_pop_sample_empty_result":                     "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_avgx_empty_result":                            "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_avgx_general":                                 "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_avgx_with_related_obj_and_number_as_argument": "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_avgy_empty_result":                            "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_avgy_general":                                 "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_count_empty_result":                           "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_count_general":                                "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_intercept_empty_result":                       "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_intercept_general":                            "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_r2_empty_result":                              "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_r2_general":                                   "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_slope_empty_result":                           "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_slope_general":                                "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_sxx_empty_result":                             "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_sxx_general":                                  "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_sxy_empty_result":                             "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_sxy_general":                                  "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_syy_empty_result":                             "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_syy_general":                                  "41274",
	//"postgres_tests.test_array.TestOtherTypesExactQuerying.test_exact_decimals":                                     "23468",
}

var djangoBlacklist19_2 = blacklist{
	// Blacklist generated from running the tests above.
	"app_loading.tests.GetModelsTest.test_get_models_only_returns_installed_models":    "unknown",
	"apps.tests.AppsTests.test_models_not_loaded":                                      "unknown",
	"backends.base.test_base.ExecuteWrapperTests.test_wrapper_connection_specific":     "unknown",
	"backends.tests.DateQuotingTest.test_django_date_trunc":                            "unknown",
	"backends.tests.FkConstraintsTests.test_check_constraints":                         "unknown",
	"backends.tests.FkConstraintsTests.test_disable_constraint_checks_context_manager": "unknown",
	"backends.tests.FkConstraintsTests.test_disable_constraint_checks_manually":        "unknown",
	"backends.tests.ThreadTests.test_connections_thread_local":                         "unknown",
	"update.tests.SimpleTest.test_nonempty_update_with_inheritance":                    "unknown",
	// TODO (rohany): The postgres_tests suite within Django is not in a automatically
	//  runnable state right now.
	//"postgres_tests.test_aggregates.TestGeneralAggregate.test_bit_and_empty_result":                                 "41334",
	//"postgres_tests.test_aggregates.TestGeneralAggregate.test_bit_and_general":                                      "41334",
	//"postgres_tests.test_aggregates.TestGeneralAggregate.test_bit_and_on_only_false_values":                         "41334",
	//"postgres_tests.test_aggregates.TestGeneralAggregate.test_bit_and_on_only_true_values":                          "41334",
	//"postgres_tests.test_aggregates.TestGeneralAggregate.test_bit_or_empty_result":                                  "41334",
	//"postgres_tests.test_aggregates.TestGeneralAggregate.test_bit_or_general":                                       "41334",
	//"postgres_tests.test_aggregates.TestGeneralAggregate.test_bit_or_on_only_false_values":                          "41334",
	//"postgres_tests.test_aggregates.TestGeneralAggregate.test_bit_or_on_only_true_values":                           "41334",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_corr_empty_result":                                 "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_corr_general":                                      "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_covar_pop_empty_result":                            "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_covar_pop_general":                                 "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_covar_pop_sample":                                  "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_covar_pop_sample_empty_result":                     "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_avgx_empty_result":                            "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_avgx_general":                                 "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_avgx_with_related_obj_and_number_as_argument": "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_avgy_empty_result":                            "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_avgy_general":                                 "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_count_empty_result":                           "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_count_general":                                "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_intercept_empty_result":                       "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_intercept_general":                            "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_r2_empty_result":                              "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_r2_general":                                   "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_slope_empty_result":                           "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_slope_general":                                "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_sxx_empty_result":                             "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_sxx_general":                                  "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_sxy_empty_result":                             "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_sxy_general":                                  "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_syy_empty_result":                             "41274",
	//"postgres_tests.test_aggregates.TestStatisticsAggregate.test_regr_syy_general":                                  "41274",
	//"postgres_tests.test_array.TestOtherTypesExactQuerying.test_exact_decimals":                                     "23468",
}

var djangoIgnoreList20_1 = blacklist{}

var djangoIgnoreList19_2 = blacklist{}
