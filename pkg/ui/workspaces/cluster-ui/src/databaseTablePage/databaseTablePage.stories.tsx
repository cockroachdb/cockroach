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

import { withBackground, withRouterProvider } from "src/storybook/decorators";
import {
  randomName,
  randomRole,
  randomTablePrivilege,
} from "src/storybook/fixtures";
import { indexUnusedDuration } from "src/util/constants";

import { DatabaseTablePage, DatabaseTablePageProps } from "./databaseTablePage";
const history = H.createHashHistory();

const withLoadingIndicator: DatabaseTablePageProps = {
  databaseName: randomName(),
  name: randomName(),
  automaticStatsCollectionEnabled: true,
  schemaName: randomName(),
  indexUsageStatsEnabled: false,
  showIndexRecommendations: false,
  csIndexUnusedDuration: indexUnusedDuration,
  details: {
    loading: true,
    loaded: false,
    requestError: null,
    queryError: undefined,
    createStatement: { create_statement: "" },
    replicaData: {
      storeIDs: [],
      replicaCount: 0,
    },
    indexData: { columns: [], indexes: [] },
    grants: { all: [] },
    statsLastUpdated: { stats_last_created_at: moment("0001-01-01T00:00:00Z") },
    spanStats: {
      live_percentage: 0,
      live_bytes: 0,
      total_bytes: 0,
      approximate_disk_bytes: 0,
      range_count: 0,
    },
  },
  indexStats: {
    loading: true,
    loaded: false,
    lastError: undefined,
    stats: [],
    lastReset: moment("2021-09-04T13:55:00Z"),
  },
  location: history.location,
  history,
  match: {
    url: "",
    path: history.location.pathname,
    isExact: false,
    params: {},
  },
  refreshTableDetails: () => {},
  refreshIndexStats: () => {},
  resetIndexUsageStats: () => {},
  refreshSettings: () => {},
  refreshUserSQLRoles: () => {},
};

const name = randomName();

const withData: DatabaseTablePageProps = {
  databaseName: randomName(),
  name: name,
  automaticStatsCollectionEnabled: true,
  schemaName: randomName(),
  indexUsageStatsEnabled: true,
  showIndexRecommendations: true,
  csIndexUnusedDuration: indexUnusedDuration,
  details: {
    loading: false,
    loaded: true,
    requestError: null,
    queryError: undefined,
    createStatement: {
      create_statement: `
      CREATE TABLE public.${name} (
        id UUID NOT NULL,
        city VARCHAR NOT NULL,
        name VARCHAR NULL,
        address VARCHAR NULL,
        credit_card VARCHAR NULL,
        CONSTRAINT "primary" PRIMARY KEY (city ASC, id ASC),
        FAMILY "primary" (id, city, name, address, credit_card)
      )
    `,
    },
    replicaData: {
      storeIDs: [1, 2, 3, 4, 5, 6, 7],
      replicaCount: 7,
    },
    indexData: {
      columns: Array(3).map(randomName),
      indexes: Array(3).map(randomName),
    },
    grants: {
      all: [
        {
          user: randomRole(),
          privileges: uniq(
            new Array(random(1, 5)).map(() => randomTablePrivilege()),
          ),
        },
      ],
    },
    statsLastUpdated: { stats_last_created_at: moment("0001-01-01T00:00:00Z") },
    spanStats: {
      live_percentage: 2.7,
      live_bytes: 12345,
      total_bytes: 456789,
      approximate_disk_bytes: 44040192,
      range_count: 4200,
    },
  },
  showNodeRegionsSection: true,
  indexStats: {
    loading: false,
    loaded: true,
    lastError: null,
    stats: [
      {
        totalReads: 0,
        lastUsed: moment("2021-10-11T11:29:00Z"),
        lastUsedType: "read",
        indexName: "primary",
        indexRecommendations: [],
      },
      {
        totalReads: 3,
        lastUsed: moment("2021-11-10T16:29:00Z"),
        lastUsedType: "read",
        indexName: "primary",
        indexRecommendations: [],
      },
      {
        totalReads: 2,
        lastUsed: moment("2021-09-04T13:55:00Z"),
        lastUsedType: "reset",
        indexName: "secondary",
        indexRecommendations: [],
      },
      {
        totalReads: 0,
        lastUsed: moment("2022-03-12T14:31:00Z"),
        lastUsedType: "created",
        indexName: "secondary",
        indexRecommendations: [
          {
            type: "DROP_UNUSED",
            reason:
              "This index has not been used and can be removed for better write performance.",
          },
        ],
      },
    ],
    lastReset: moment("2021-09-04T13:55:00Z"),
  },
  location: history.location,
  history,
  match: {
    url: "",
    path: history.location.pathname,
    isExact: false,
    params: {},
  },
  refreshTableDetails: () => {},
  refreshIndexStats: () => {},
  resetIndexUsageStats: () => {},
  refreshSettings: () => {},
  refreshUserSQLRoles: () => {},
};

storiesOf("Database Table Page", module)
  .addDecorator(withRouterProvider)
  .addDecorator(withBackground)
  .add("with data", () => <DatabaseTablePage {...withData} />)
  .add("with loading indicator", () => (
    <DatabaseTablePage {...withLoadingIndicator} />
  ));
