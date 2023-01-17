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

import { withBackground, withRouterProvider } from "src/storybook/decorators";
import { randomName } from "src/storybook/fixtures";
import { IndexDetailsPage, IndexDetailsPageProps } from "./indexDetailsPage";
import moment from "moment";

const withData: IndexDetailsPageProps = {
  databaseName: randomName(),
  tableName: randomName(),
  indexName: randomName(),
  details: {
    loading: false,
    loaded: true,
    createStatement: `
      CREATE UNIQUE INDEX "primary" ON system.public.database_role_settings USING btree (database_id ASC, role_name ASC)
    `,
    totalReads: 0,
    lastRead: moment("2021-10-21T22:00:00Z"),
    lastReset: moment("2021-12-02T07:12:00Z"),
  },
  refreshIndexStats: () => {},
  resetIndexUsageStats: () => {},
  refreshNodes: () => {},
  refreshUserSQLRoles: () => {},
};

storiesOf("Index Details Page", module)
  .addDecorator(withRouterProvider)
  .addDecorator(withBackground)
  .add("with data", () => <IndexDetailsPage {...withData} />);
