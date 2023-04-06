// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { SortSetting } from "../../sortedtable";
import { ViewMode } from "../../databaseDetailsPage";

export const defaultStmtsTxnsSortSetting: SortSetting = {
  ascending: false,
  columnTitle: "executionCount",
};

export const defaultSortSettingActiveExecutions: SortSetting = {
  ascending: false,
  columnTitle: "startTime",
};

export const defaultSortSettingInsights: SortSetting = {
  ascending: false,
  columnTitle: "startTime",
};

export const defaultSortSettingSchemaInsights: SortSetting = {
  ascending: false,
  columnTitle: "insights",
};

export const defaultFiltersActiveExecutions = {
  app: "",
  executionStatus: "",
};

export const defaultFiltersInsights = {
  app: "",
  workloadInsightType: "",
};

export const defaultFiltersSchemaInsights = {
  database: "",
  schemaInsightType: "",
};

export const defaultSessionsSortSetting: SortSetting = {
  ascending: false,
  columnTitle: "statementAge",
};

export const defaultJobsSortSetting: SortSetting = {
  ascending: false,
  columnTitle: "lastExecutionTime",
};

export const defaultDatabasesSortSetting: SortSetting = {
  ascending: true,
  columnTitle: "name",
};

export const defaultDatabaseDetailsTableSortSetting: SortSetting = {
  ascending: true,
  columnTitle: "name",
};

export const defaultDatabaseDetailsGrantSortSetting: SortSetting = {
  ascending: true,
  columnTitle: "name",
};

export const defaultJobStatusSetting = "";

export const defaultJobShowSetting = "0";

export const defaultJobTypeSetting = 0;

export const defaultDatabaseDetailsViewMode = ViewMode.Tables;
