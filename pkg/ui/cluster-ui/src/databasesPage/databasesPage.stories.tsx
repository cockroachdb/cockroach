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

const withLoadingIndicator: DatabasesPageProps = {
  loading: true,
  loaded: false,
  databases: [],
  refreshDatabases: () => {},
  refreshDatabaseDetails: () => {},
  refreshTableStats: () => {},
};

const withoutData: DatabasesPageProps = {
  loading: false,
  loaded: true,
  databases: [],
  refreshDatabases: () => {},
  refreshDatabaseDetails: () => {},
  refreshTableStats: () => {},
};

const withData: DatabasesPageProps = {
  loading: false,
  loaded: true,
  databases: _.map(Array(42), _item => {
    return {
      loading: false,
      loaded: true,
      name: randomName(),
      sizeInBytes: _.random(1000.0) * 1024 ** _.random(1, 2),
      tableCount: _.random(5, 100),
      rangeCount: _.random(50, 500),
      missingTables: [],
    };
  }),
  refreshDatabases: () => {},
  refreshDatabaseDetails: () => {},
  refreshTableStats: () => {},
};

storiesOf("Databases Page", module)
  .addDecorator(withRouterProvider)
  .addDecorator(withBackground)
  .add("with data", () => <DatabasesPage {...withData} />)
  .add("without data", () => <DatabasesPage {...withoutData} />)
  .add("with loading indicator", () => (
    <DatabasesPage {...withLoadingIndicator} />
  ));
