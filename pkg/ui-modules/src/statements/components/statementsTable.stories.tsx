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
import {makeStatementsColumns, StatementsSortedTable} from "./statementsTable";
import statementsPagePropsFixture from "src/views/statements/statementsPage.fixture";
import {withRouterProvider} from ".storybook/decorators";

const { statements } = statementsPagePropsFixture;

storiesOf("StatementsSortedTable", module)
  .addDecorator(withRouterProvider)
  .add("with data", () => (
    <StatementsSortedTable
      className="statements-table"
      data={statements}
      columns={
        makeStatementsColumns(
          statements,
          "(internal)",
        )
      }
      sortSetting={{
        ascending: false,
        sortKey: 3,
      }}
      pagination={{
        pageSize: 20,
        current: 1,
      }}
    />
  ))
  .add("empty table", () => (
    <StatementsSortedTable />
  ));
