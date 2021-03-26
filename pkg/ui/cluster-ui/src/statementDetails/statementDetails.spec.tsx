import React from "react";
import { mount } from "enzyme";
import { assert } from "chai";
import { createSandbox } from "sinon";
import { MemoryRouter as Router } from "react-router-dom";
import { StatementDetails, StatementDetailsProps } from "./statementDetails";
import { DiagnosticsView } from "./diagnostics/diagnosticsView";
import { getStatementDetailsPropsFixture } from "./statementDetails.fixture";
import { Loading } from "../loading";

const sandbox = createSandbox();

describe("StatementDetails page", () => {
  let statementDetailsProps: StatementDetailsProps;

  beforeEach(() => {
    sandbox.reset();
    statementDetailsProps = getStatementDetailsPropsFixture();
  });

  it("shows loading indicator when data is not ready yet", () => {
    statementDetailsProps.statement = null;
    statementDetailsProps.statementsError = null;

    const wrapper = mount(
      <Router>
        <StatementDetails {...statementDetailsProps} />
      </Router>,
    );
    assert.isTrue(wrapper.find(Loading).prop("loading"));
    assert.isFalse(
      wrapper
        .find(StatementDetails)
        .find("div.ant-tabs-tab")
        .exists(),
    );
  });

  it("shows error alert when `lastError` is not null", () => {
    statementDetailsProps.statementsError = new Error("Something went wrong");

    const wrapper = mount(
      <Router>
        <StatementDetails {...statementDetailsProps} />
      </Router>,
    );
    assert.isNotNull(wrapper.find(Loading).prop("error"));
    assert.isFalse(
      wrapper
        .find(StatementDetails)
        .find("div.ant-tabs-tab")
        .exists(),
    );
  });

  it("calls onTabChanged prop when selected tab is changed", () => {
    const onTabChangeSpy = sandbox.spy();
    const wrapper = mount(
      <Router>
        <StatementDetails
          {...statementDetailsProps}
          onTabChanged={onTabChangeSpy}
        />
      </Router>,
    );

    wrapper
      .find(StatementDetails)
      .find("div.ant-tabs-tab")
      .last()
      .simulate("click");

    onTabChangeSpy.calledWith("execution-stats");
  });

  describe("Diagnostics tab", () => {
    beforeEach(() => {
      statementDetailsProps.history.location.search = new URLSearchParams([
        ["tab", "diagnostics"],
      ]).toString();
    });

    it("calls createStatementDiagnosticsReport callback on Activate button click", () => {
      const onDiagnosticsActivateClickSpy = sandbox.spy();
      const wrapper = mount(
        <Router>
          <StatementDetails
            {...statementDetailsProps}
            createStatementDiagnosticsReport={onDiagnosticsActivateClickSpy}
          />
        </Router>,
      );

      wrapper
        .find(DiagnosticsView)
        .findWhere(n => n.prop("children") === "Activate Diagnostics")
        .first()
        .simulate("click");

      onDiagnosticsActivateClickSpy.calledOnceWith(
        statementDetailsProps.statement.statement,
      );
    });
  });
});
