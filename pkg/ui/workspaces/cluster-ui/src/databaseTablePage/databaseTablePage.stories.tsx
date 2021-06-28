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
  refreshTableDetails: () => {},
  refreshTableStats: () => {},
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
  stats: {
    loading: false,
    loaded: true,
    sizeInBytes: 44040192,
    rangeCount: 4200,
  },
  refreshTableDetails: () => {},
  refreshTableStats: () => {},
};

storiesOf("Database Table Page", module)
  .addDecorator(withRouterProvider)
  .addDecorator(withBackground)
  .add("with data", () => <DatabaseTablePage {...withData} />)
  .add("with loading indicator", () => (
    <DatabaseTablePage {...withLoadingIndicator} />
  ));
