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
} from "./databaseDetailsPage";

const withLoadingIndicator: DatabaseDetailsPageProps = {
  loading: true,
  loaded: false,
  name: randomName(),
  tables: [],
  refreshDatabaseDetails: () => {},
  refreshTableDetails: () => {},
  refreshTableStats: () => {},
};

const withoutData: DatabaseDetailsPageProps = {
  loading: false,
  loaded: true,
  name: randomName(),
  tables: [],
  refreshDatabaseDetails: () => {},
  refreshTableDetails: () => {},
  refreshTableStats: () => {},
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
      },
      stats: {
        loading: false,
        loaded: true,
        replicationSizeInBytes: _.random(1000.0) * 1024 ** _.random(1, 2),
        rangeCount: _.random(50, 500),
      },
    };
  }),
  refreshDatabaseDetails: () => {},
  refreshTableDetails: () => {},
  refreshTableStats: () => {},
};

storiesOf("Database Details Page", module)
  .addDecorator(withRouterProvider)
  .addDecorator(withBackground)
  .add("with data", () => <DatabaseDetailsPage {...withData} />)
  .add("without data", () => <DatabaseDetailsPage {...withoutData} />)
  .add("with loading indicator", () => (
    <DatabaseDetailsPage {...withLoadingIndicator} />
  ));
