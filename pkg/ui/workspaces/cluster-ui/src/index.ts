// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import "./protobufInit";
import * as api from "./api";
import * as util from "./util";
export * from "./anchor";
export * from "./badge";
export * from "./barCharts";
export * from "./button";
export * from "./common";
export * from "./delayed";
export * from "./downloadFile";
export * from "./dropdown";
export * from "./empty";
export * from "./filter";
export * from "./highlightedText";
export * from "./indexDetailsPage";
export * from "./insights";
export * from "./jobs";
export * from "./loading";
export * from "./modal";
export * from "./pageConfig";
export * from "./pagination";
export * from "./queryFilter";
export * from "./search";
export * from "./schedules";
export * from "./sortedtable";
export * from "./statementsDiagnostics";
export * from "./statementsPage";
export * from "./statementDetails";
export * from "./statementsTable";
export * from "./sql";
export * from "./table";
export * from "./store";
export * from "./transactionsPage";
export * from "./transactionDetails";
export * from "./text";
export * from "./tracez";
export { util, api };
export * from "./sessions";
export * from "./timeScaleDropdown";
export * from "./activeExecutions";
export * from "./graphs";
export * from "./selectors";
export * from "./contexts";
export * from "./timestamp";
export * from "./antdTheme";
export * from "./databasesV2";
export * from "./databaseDetailsV2";
export * from "./tableDetailsV2";
// Reexport ConfigProvider instance from cluster-ui as exact instance
// required in Db Console to apply Antd theme in Db Console.
// TODO (koorosh): is it possible to define antd pacakge as peerDependency
// to make sure that the same instance used in Db Console and Cluster UI?
export { ConfigProvider } from "antd";
