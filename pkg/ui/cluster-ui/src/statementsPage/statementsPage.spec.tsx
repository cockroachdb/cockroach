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

      assert.equal(statementsPageInstance.state.sortSetting.sortKey, 3);
      assert.equal(statementsPageInstance.state.sortSetting.ascending, false);
    });
  });
});
