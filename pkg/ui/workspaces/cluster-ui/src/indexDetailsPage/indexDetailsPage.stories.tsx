// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import moment from "moment-timezone";
import React from "react";

import { withBackground, withRouterProvider } from "src/storybook/decorators";
import { randomName } from "src/storybook/fixtures";

import { IndexDetailsPage, IndexDetailsPageProps } from "./indexDetailsPage";

const withData: IndexDetailsPageProps = {
  databaseName: randomName(),
  tableName: randomName(),
  indexName: randomName(),
  isTenant: false,
  nodeRegions: {},
  timeScale: null,
  details: {
    databaseID: 1,
    loading: false,
    loaded: true,
    createStatement: `
      CREATE UNIQUE INDEX "primary" ON system.public.database_role_settings USING btree (database_id ASC, role_name ASC)
    `,
    tableID: "1",
    indexID: "1",
    totalReads: 0,
    lastRead: moment("2021-10-21T22:00:00Z"),
    lastReset: moment("2021-12-02T07:12:00Z"),
    indexRecommendations: [
      {
        type: "DROP_UNUSED",
        reason:
          "This index has not been used and can be removed for better write performance.",
      },
    ],
  },
  refreshIndexStats: () => {},
  resetIndexUsageStats: () => {},
  refreshNodes: () => {},
  refreshUserSQLRoles: () => {},
  onTimeScaleChange: () => {},
};

storiesOf("Index Details Page", module)
  .addDecorator(withRouterProvider)
  .addDecorator(withBackground)
  .add("with data", () => <IndexDetailsPage {...withData} />);
