// Copyright 2020 The Cockroach Authors.
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

import { withRouterProvider } from ".storybook/decorators";
import {makeStatementsColumns, StatementsSortedTable} from "./statementsTable";
import statementsPagePropsFixture from "./statementsPage.fixture";

const { statements } = statementsPagePropsFixture

storiesOf("StatementsTable", module)
  .addDecorator(withRouterProvider)
  .add("with data", () => (
    <StatementsSortedTable
      columns={makeStatementsColumns(statements, "App")}
      data={statements}
      sortSetting={{
        sortKey: 3,
        ascending: true,
      }}
    />
  ));
