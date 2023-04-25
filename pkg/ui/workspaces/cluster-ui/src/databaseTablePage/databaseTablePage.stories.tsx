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
import { DatabaseTablePage, DatabaseTablePageProps } from "./databaseTablePage";
import moment from "moment-timezone";
import * as H from "history";
const history = H.createHashHistory();

const withLoadingIndicator: DatabaseTablePageProps = {
  databaseName: randomName(),
  name: randomName(),
  automaticStatsCollectionEnabled: true,
  details: {
    loading: true,
    loaded: false,
    lastError: undefined,
    createStatement: "",
    replicaCount: 0,
    indexNames: [],
    grants: [],
    statsLastUpdated: moment("0001-01-01T00:00:00Z"),
    livePercentage: 2.7,
    liveBytes: 12345,
    totalBytes: 456789,
    sizeInBytes: 0,
    rangeCount: 0,
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
  details: {
    loading: false,
    loaded: true,
    lastError: null,
    createStatement: `
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
    replicaCount: 7,
    indexNames: Array(3).map(randomName),
    grants: [
      {
        user: randomRole(),
        privileges: _.uniq(
          new Array(_.random(1, 5)).map(() => randomTablePrivilege()),
        ),
      },
    ],
    statsLastUpdated: moment("0001-01-01T00:00:00Z"),
    livePercentage: 2.7,
    liveBytes: 12345,
    totalBytes: 456789,
    sizeInBytes: 44040192,
    rangeCount: 4200,
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
