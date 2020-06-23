// Copyright 2018 The Cockroach Authors.
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
import { ReactWrapper } from "enzyme";

import { connectedMount } from "../test-utils";
import StatementsPageConnected, {
  StatementsPage,
  StatementsPageProps,
  StatementsPageState,
} from "./statementsPage";

describe("StatementsPage", () => {
  describe("Statements table", () => {
    it("sorts data by Execution Count DESC as default option", () => {
      const rootWrapper = connectedMount(() => <StatementsPageConnected />);

      const statementsPageWrapper: ReactWrapper<StatementsPageProps, StatementsPageState> = rootWrapper.find(StatementsPage).first();
      const state = statementsPageWrapper.state();

      assert.equal(state.sortSetting.sortKey, 3);
      assert.equal(state.sortSetting.ascending, false);
    });
  });
});
