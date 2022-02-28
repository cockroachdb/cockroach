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
  DatabaseDetailsPageProps,
  ViewMode,
} from "./databaseDetailsPage";

import * as H from "history";
import moment from "moment";
const history = H.createHashHistory();

const withLoadingIndicator: DatabaseDetailsPageProps = {
  loading: true,
  loaded: false,
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
  location: history.location,
  history,
  match: {
    url: "",
    path: history.location.pathname,
    isExact: false,
    params: {},
  },
};

const withData: DatabaseDetailsPageProps = {
  loading: false,
  loaded: true,
  name: randomName(),
  tables: _.map(Array(42), _item => {
    const roles = _.uniq(
      _.map(new Array(_.random(1, 3)), _item => randomRole()),
    );
    const grants = _.uniq(
      _.map(new Array(_.random(1, 5)), _item => randomTablePrivilege()),
    );

    return {
      name: randomName(),
      details: {
        loading: false,
        loaded: true,
        columnCount: _.random(5, 42),
        indexCount: _.random(1, 6),
        userCount: roles.length,
        roles: roles,
        grants: grants,
        statsLastUpdated: moment("0001-01-01T00:00:00Z"),
      },
      showNodeRegionsColumn: true,
      stats: {
        loading: false,
        loaded: true,
        replicationSizeInBytes: _.random(1000.0) * 1024 ** _.random(1, 2),
        rangeCount: _.random(50, 500),
        nodesByRegionString:
          "gcp-europe-west1(n8), gcp-us-east1(n1), gcp-us-west1(n6)",
      },
    };
  }),
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
