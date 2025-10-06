// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import * as H from "history";
import random from "lodash/random";
import uniq from "lodash/uniq";
import moment from "moment-timezone";
import React from "react";

import { defaultFilters } from "src/queryFilter";
import { withBackground, withRouterProvider } from "src/storybook/decorators";
import {
  randomName,
  randomRole,
  randomTablePrivilege,
} from "src/storybook/fixtures";
import { indexUnusedDuration } from "src/util/constants";

import {
  DatabaseDetailsPage,
  DatabaseDetailsPageDataTable,
  DatabaseDetailsPageProps,
} from "./databaseDetailsPage";
import { ViewMode } from "./types";

const history = H.createHashHistory();

const withLoadingIndicator: DatabaseDetailsPageProps = {
  loading: true,
  loaded: false,
  requestError: undefined,
  queryError: undefined,
  showIndexRecommendations: false,
  csIndexUnusedDuration: indexUnusedDuration,
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
  refreshNodes: () => {},
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
  requestError: null,
  queryError: undefined,
  showIndexRecommendations: false,
  csIndexUnusedDuration: indexUnusedDuration,
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
  refreshNodes: () => {},
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
  const roles = uniq(new Array(random(1, 3)).map(() => randomRole()));
  const privileges = uniq(
    new Array(random(1, 5)).map(() => randomTablePrivilege()),
  );
  const columns = uniq(new Array(random(1, 5)).map(() => randomName()));
  const indexes = uniq(new Array(random(1, 5)).map(() => randomName()));

  return {
    loading: false,
    loaded: true,
    requestError: null,
    queryError: undefined,
    name: {
      qualifiedNameWithSchemaAndTable: "public.table",
      schema: "public",
      table: "table",
    },
    qualifiedDisplayName: "public.table",
    details: {
      grants: {
        roles,
        privileges,
      },
      schemaDetails: {
        columns,
        indexes,
      },
      statsLastUpdated: {
        stats_last_created_at: moment("0001-01-01T00:00:00Z"),
      },
      indexStatRecs: {
        has_index_recommendations: false,
      },
      spanStats: {
        live_percentage: random(0, 100),
        live_bytes: random(0, 10000),
        total_bytes: random(0, 10000),
        approximate_disk_bytes: random(0, 10000),
        range_count: random(0, 10000),
      },
    },
  };
}

const withData: DatabaseDetailsPageProps = {
  loading: false,
  loaded: true,
  requestError: null,
  queryError: undefined,
  showIndexRecommendations: true,
  csIndexUnusedDuration: indexUnusedDuration,
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
  refreshNodes: () => {},
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
