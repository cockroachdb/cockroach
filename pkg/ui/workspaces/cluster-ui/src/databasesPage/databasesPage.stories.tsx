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
import { randomName } from "src/storybook/fixtures";
import { DatabasesPage, DatabasesPageProps } from "./databasesPage";

import * as H from "history";
import { defaultFilters } from "src/queryFilter";
import { indexUnusedDuration } from "src/util/constants";
const history = H.createHashHistory();

const withLoadingIndicator: DatabasesPageProps = {
  loading: true,
  loaded: false,
  requestError: undefined,
  queryError: undefined,
  automaticStatsCollectionEnabled: true,
  indexRecommendationsEnabled: false,
  csIndexUnusedDuration: indexUnusedDuration,
  databases: [],
  sortSetting: {
    ascending: false,
    columnTitle: "name",
  },
  search: "",
  filters: defaultFilters,
  nodeRegions: {},
  refreshDatabases: () => {},
  refreshSettings: () => {},
  refreshDatabaseDetails: () => {},
  refreshDatabaseSpanStats: () => {},
  location: history.location,
  history,
  match: {
    url: "",
    path: history.location.pathname,
    isExact: false,
    params: {},
  },
};

const withoutData: DatabasesPageProps = {
  loading: false,
  loaded: true,
  requestError: undefined,
  queryError: undefined,
  automaticStatsCollectionEnabled: true,
  indexRecommendationsEnabled: false,
  csIndexUnusedDuration: indexUnusedDuration,
  databases: [],
  sortSetting: {
    ascending: false,
    columnTitle: "name",
  },
  search: "",
  filters: defaultFilters,
  nodeRegions: {},
  onSortingChange: () => {},
  refreshDatabases: () => {},
  refreshSettings: () => {},
  refreshDatabaseDetails: () => {},
  refreshDatabaseSpanStats: () => {},
  location: history.location,
  history,
  match: {
    url: "",
    path: history.location.pathname,
    isExact: false,
    params: {},
  },
};

const withData: DatabasesPageProps = {
  loading: false,
  loaded: true,
  requestError: undefined,
  queryError: undefined,
  showNodeRegionsColumn: true,
  automaticStatsCollectionEnabled: true,
  indexRecommendationsEnabled: true,
  csIndexUnusedDuration: indexUnusedDuration,
  sortSetting: {
    ascending: false,
    columnTitle: "name",
  },
  search: "",
  filters: defaultFilters,
  nodeRegions: {
    "1": "gcp-us-east1",
    "6": "gcp-us-west1",
    "8": "gcp-europe-west1",
  },
  databases: Array(42).map(() => {
    return {
      detailsLoading: false,
      detailsLoaded: false,
      spanStatsLoading: false,
      spanStatsLoaded: false,
      detailsRequestError: undefined,
      detailsQueryError: undefined,
      spanStatsRequestError: undefined,
      spanStatsQueryError: undefined,
      name: randomName(),
      sizeInBytes: _.random(1000.0) * 1024 ** _.random(1, 2),
      tableCount: _.random(5, 100),
      rangeCount: _.random(50, 500),
      nodesByRegionString:
        "gcp-europe-west1(n8), gcp-us-east1(n1), gcp-us-west1(n6)",
      numIndexRecommendations: 0,
    };
  }),
  onSortingChange: () => {},
  refreshDatabases: () => {},
  refreshSettings: () => {},
  refreshDatabaseDetails: () => {},
  refreshDatabaseSpanStats: () => {},
  location: history.location,
  history,
  match: {
    url: "",
    path: history.location.pathname,
    isExact: false,
    params: {},
  },
};

storiesOf("Databases Page", module)
  .addDecorator(withRouterProvider)
  .addDecorator(withBackground)
  .add("with data", () => <DatabasesPage {...withData} />)
  .add("without data", () => <DatabasesPage {...withoutData} />)
  .add("with loading indicator", () => (
    <DatabasesPage {...withLoadingIndicator} />
  ));
