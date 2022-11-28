// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { storiesOf } from "@storybook/react";
import _ from "lodash";

import { withBackground, withRouterProvider } from "src/storybook/decorators";
import {
  randomName,
  randomRole,
  randomTablePrivilege,
} from "src/storybook/fixtures";
import {
  DatabaseDetailsPage,
  DatabaseDetailsPageDataTable,
  DatabaseDetailsPageProps,
  ViewMode,
} from "./databaseDetailsPage";

import * as H from "history";
import moment from "moment";
import { defaultFilters } from "src/queryFilter";
const history = H.createHashHistory();

const withLoadingIndicator: DatabaseDetailsPageProps = {
  loading: true,
  loaded: false,
  lastError: undefined,
  name: randomName(),
  tables: [],
  viewMode: ViewMode.Tables,
  sortSettingTables: {
    ascending: false,
    columnTitle: "name",
  },
  sortSettingGrants: {
    ascending: false,
    columnTitle: "name",
  },
  onSortingTablesChange: () => {},
  onSortingGrantsChange: () => {},
  refreshDatabaseDetails: () => {},
  refreshTableDetails: () => {},
  refreshTableStats: () => {},
  search: null,
  filters: defaultFilters,
  nodeRegions: {},
  location: history.location,
  history,
  match: {
    url: "",
    path: history.location.pathname,
    isExact: false,
    params: {},
  },
};

const withoutData: DatabaseDetailsPageProps = {
  loading: false,
  loaded: true,
  lastError: null,
  name: randomName(),
  tables: [],
  viewMode: ViewMode.Tables,
  sortSettingTables: {
    ascending: false,
    columnTitle: "name",
  },
  sortSettingGrants: {
    ascending: false,
    columnTitle: "name",
  },
  onSortingTablesChange: () => {},
  onSortingGrantsChange: () => {},
  refreshDatabaseDetails: () => {},
  refreshTableDetails: () => {},
  refreshTableStats: () => {},
  search: null,
  filters: defaultFilters,
  nodeRegions: {},
  location: history.location,
  history,
  match: {
    url: "",
    path: history.location.pathname,
    isExact: false,
    params: {},
  },
};

function createTable(): DatabaseDetailsPageDataTable {
  const roles = _.uniq(new Array(_.random(1, 3)).map(() => randomRole()));
  const grants = _.uniq(
    new Array(_.random(1, 5)).map(() => randomTablePrivilege()),
  );

  return {
    name: randomName(),
    details: {
      loading: false,
      loaded: true,
      lastError: null,
      columnCount: _.random(5, 42),
      indexCount: _.random(1, 6),
      userCount: roles.length,
      roles: roles,
      grants: grants,
      statsLastUpdated: moment("0001-01-01T00:00:00Z"),
      hasIndexRecommendations: false,
      livePercentage: _.random(0, 100),
      liveBytes: _.random(0, 10000),
      totalBytes: _.random(0, 10000),
    },
    stats: {
      loading: false,
      loaded: true,
      lastError: null,
      replicationSizeInBytes: _.random(1000.0) * 1024 ** _.random(1, 2),
      rangeCount: _.random(50, 500),
      nodesByRegionString:
        "gcp-europe-west1(n8), gcp-us-east1(n1), gcp-us-west1(n6)",
    },
  };
}

const withData: DatabaseDetailsPageProps = {
  loading: false,
  loaded: true,
  lastError: null,
  name: randomName(),
  tables: [createTable()],
  viewMode: ViewMode.Tables,
  sortSettingTables: {
    ascending: false,
    columnTitle: "name",
  },
  sortSettingGrants: {
    ascending: false,
    columnTitle: "name",
  },
  onSortingTablesChange: () => {},
  onSortingGrantsChange: () => {},
  refreshDatabaseDetails: () => {},
  refreshTableDetails: () => {},
  refreshTableStats: () => {},
  search: null,
  filters: defaultFilters,
  nodeRegions: {},
  location: history.location,
  history,
  match: {
    url: "",
    path: history.location.pathname,
    isExact: false,
    params: {},
  },
};

storiesOf("Database Details Page", module)
  .addDecorator(withRouterProvider)
  .addDecorator(withBackground)
  .add("with data", () => <DatabaseDetailsPage {...withData} />)
  .add("without data", () => <DatabaseDetailsPage {...withoutData} />)
  .add("with loading indicator", () => (
    <DatabaseDetailsPage {...withLoadingIndicator} />
  ));
