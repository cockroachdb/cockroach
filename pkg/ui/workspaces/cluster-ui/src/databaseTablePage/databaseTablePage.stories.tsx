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
import moment from "moment";
import * as H from "history";
const history = H.createHashHistory();

const withLoadingIndicator: DatabaseTablePageProps = {
  databaseName: randomName(),
  name: randomName(),
  automaticStatsCollectionEnabled: true,
  details: {
    loading: true,
    loaded: false,
    createStatement: "",
    replicaCount: 0,
    indexNames: [],
    grants: [],
    statsLastUpdated: moment("0001-01-01T00:00:00Z"),
  },
  stats: {
    loading: true,
    loaded: false,
    sizeInBytes: 0,
    rangeCount: 0,
  },
  indexStats: {
    loading: true,
    loaded: false,
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
  refreshTableStats: () => {},
  refreshIndexStats: () => {},
  resetIndexUsageStats: () => {},
  refreshSettings: () => {},
};

const name = randomName();

const withData: DatabaseTablePageProps = {
  databaseName: randomName(),
  name: name,
  automaticStatsCollectionEnabled: true,
  details: {
    loading: false,
    loaded: true,
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
    indexNames: _.map(Array(3), randomName),
    grants: _.uniq(
      _.map(Array(12), () => {
        return {
          user: randomRole(),
          privilege: randomTablePrivilege(),
        };
      }),
    ),
    statsLastUpdated: moment("0001-01-01T00:00:00Z"),
  },
  showNodeRegionsSection: true,
  stats: {
    loading: false,
    loaded: true,
    sizeInBytes: 44040192,
    rangeCount: 4200,
    nodesByRegionString:
      "gcp-europe-west1(n8), gcp-us-east1(n1), gcp-us-west1(n6)",
  },
  indexStats: {
    loading: false,
    loaded: true,
    stats: [
      {
        totalReads: 0,
        lastUsed: moment("2021-10-11T11:29:00Z"),
        lastUsedType: "read",
        indexName: "primary",
      },
      {
        totalReads: 3,
        lastUsed: moment("2021-11-10T16:29:00Z"),
        lastUsedType: "read",
        indexName: "primary",
      },
      {
        totalReads: 2,
        lastUsed: moment("2021-09-04T13:55:00Z"),
        lastUsedType: "reset",
        indexName: "secondary",
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
  refreshTableStats: () => {},
  refreshIndexStats: () => {},
  resetIndexUsageStats: () => {},
  refreshSettings: () => {},
};

storiesOf("Database Table Page", module)
  .addDecorator(withRouterProvider)
  .addDecorator(withBackground)
  .add("with data", () => <DatabaseTablePage {...withData} />)
  .add("with loading indicator", () => (
    <DatabaseTablePage {...withLoadingIndicator} />
  ));
