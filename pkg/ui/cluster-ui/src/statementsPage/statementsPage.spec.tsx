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
import { assert } from "chai";
import { ReactWrapper, mount } from "enzyme";
import { MemoryRouter } from "react-router-dom";

import {
  StatementsPage,
  StatementsPageProps,
  StatementsPageState,
} from "src/statementsPage";
import statementsPagePropsFixture from "./statementsPage.fixture";

describe("StatementsPage", () => {
  describe("Statements table", () => {
    it("sorts data by Execution Count DESC as default option", () => {
      const rootWrapper = mount(
        <MemoryRouter>
          <StatementsPage {...statementsPagePropsFixture} />
        </MemoryRouter>,
      );

      const statementsPageWrapper: ReactWrapper<
        StatementsPageProps,
        StatementsPageState,
        React.Component<any, any>
      > = rootWrapper.find(StatementsPage).first();
      const statementsPageInstance = statementsPageWrapper.instance();

      assert.equal(
        statementsPageInstance.state.sortSetting.columnTitle,
        "executionCount",
      );
      assert.equal(statementsPageInstance.state.sortSetting.ascending, false);
    });
  });
});
