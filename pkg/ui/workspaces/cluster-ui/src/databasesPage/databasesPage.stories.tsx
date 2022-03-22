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
const history = H.createHashHistory();

const withLoadingIndicator: DatabasesPageProps = {
  loading: true,
  loaded: false,
  automaticStatsCollectionEnabled: true,
  databases: [],
  sortSetting: {
    ascending: false,
    columnTitle: "name",
  },
  onSortingChange: () => {},
  refreshDatabases: () => {},
  refreshSettings: () => {},
  refreshDatabaseDetails: () => {},
  refreshTableStats: () => {},
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
  automaticStatsCollectionEnabled: true,
  databases: [],
  sortSetting: {
    ascending: false,
    columnTitle: "name",
  },
  onSortingChange: () => {},
  refreshDatabases: () => {},
  refreshSettings: () => {},
  refreshDatabaseDetails: () => {},
  refreshTableStats: () => {},
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
  showNodeRegionsColumn: true,
  automaticStatsCollectionEnabled: true,
  sortSetting: {
    ascending: false,
    columnTitle: "name",
  },
  databases: _.map(Array(42), _item => {
    return {
      loading: false,
      loaded: true,
      name: randomName(),
      sizeInBytes: _.random(1000.0) * 1024 ** _.random(1, 2),
      tableCount: _.random(5, 100),
      rangeCount: _.random(50, 500),
      missingTables: [],
      nodesByRegionString:
        "gcp-europe-west1(n8), gcp-us-east1(n1), gcp-us-west1(n6)",
    };
  }),
  onSortingChange: () => {},
  refreshDatabases: () => {},
  refreshSettings: () => {},
  refreshDatabaseDetails: () => {},
  refreshTableStats: () => {},
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
