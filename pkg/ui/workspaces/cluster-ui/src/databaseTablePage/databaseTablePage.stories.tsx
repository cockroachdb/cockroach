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

const withLoadingIndicator: DatabaseTablePageProps = {
  databaseName: randomName(),
  name: randomName(),
  details: {
    loading: true,
    loaded: false,
    createStatement: "",
    replicaCount: 0,
    indexNames: [],
    grants: [],
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
    stats: [
      {
        totalReads: 0,
        lastUsedTime: moment("2021-11-10T16:29:00Z"),
        lastUsedString: "Nov 10, 2021 at 4:29 PM",
        indexName: "primary",
      },
    ],
    lastReset: "Nov 12, 2021 at 8:14 PM (UTC)",
  },
  refreshTableDetails: () => {},
  refreshTableStats: () => {},
  refreshIndexStats: () => {},
  resetIndexUsageStats: () => {},
};

const name = randomName();

const withData: DatabaseTablePageProps = {
  databaseName: randomName(),
  name: name,
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
    loaded: false,
    stats: [
      {
        totalReads: 0,
        lastUsedTime: moment("2021-01-11T11:29:00Z"),
        lastUsedString: "Jan 11, 2021 at 11:29 AM",
        indexName: "primary",
      },
      {
        totalReads: 3,
        lastUsedTime: moment("2021-11-10T16:29:00Z"),
        lastUsedString: "Nov 10, 2021 at 4:29 PM",
        indexName: "primary",
      },
      {
        totalReads: 2,
        lastUsedTime: moment("2021-09-04T13:55:00Z"),
        lastUsedString: "Sep 04, 2021 at 12:55 PM",
        indexName: "secondary",
      },
    ],
    lastReset: "Oct 22, 2021 at 9:21 AM (UTC)",
  },
  refreshTableDetails: () => {},
  refreshTableStats: () => {},
  refreshIndexStats: () => {},
  resetIndexUsageStats: () => {},
};

storiesOf("Database Table Page", module)
  .addDecorator(withRouterProvider)
  .addDecorator(withBackground)
  .add("with data", () => <DatabaseTablePage {...withData} />)
  .add("with loading indicator", () => (
    <DatabaseTablePage {...withLoadingIndicator} />
  ));
