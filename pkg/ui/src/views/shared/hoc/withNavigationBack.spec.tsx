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
import { assert } from "chai";
import { mount } from "enzyme";
import { createMemoryHistory, History, Location } from "history";
import { spy } from "sinon";
import { compose } from "redux";
import { withRouter } from "react-router-dom";
import { Router } from "react-router";

import "src/enzymeInit";
import { LocationState, withNavigationBack, WithNavigationBackProps } from "./withNavigationBack";

const fallbackPath = "/fallbackPath";

const TestComponent: React.FC<WithNavigationBackProps> = props => (
  <div onClick={props.navigateBack}/>
);

describe("withNavigationBack", () => {
  let history: History<LocationState>;

  beforeEach(() => {
    history = createMemoryHistory();
  });

  it("navigates to specified location in `from` state prop", () => {
    const fromLocation: Location<LocationState> = {
      ...history.location,
      pathname: "/statements/(internal)",
      search: "?q=delete",
    };
    history.location.state = {
      from: fromLocation,
    };
    const pushSpy = spy(history, "push");

    const Component = compose(
      withRouter,
      withNavigationBack(fallbackPath),
    )(TestComponent);

    const wrapper = mount(
      <Router history={history}>
        <Component />
      </Router>,
    );
    wrapper.simulate("click");
    assert.ok(wrapper);
    assert.isTrue(pushSpy.calledOnceWith(fromLocation));
  });

  it("navigates back to fallback path if `from` state is not provided", () => {
    const pushSpy: any = spy(history, "push");
    const Component = compose(
      withRouter,
      withNavigationBack(fallbackPath),
    )(TestComponent);

    const wrapper = mount(
      <Router history={history}>
        <Component />
      </Router>,
    );
    wrapper.simulate("click");
    assert.ok(wrapper);
    assert.isTrue(pushSpy.calledOnceWith(fallbackPath));
  });

  it("calls browser back action if both `from` state and fallback path are not provided", () => {
    const goBackSpy = spy(history, "goBack");
    const pushSpy = spy(history, "push");

    const Component = compose(
      withRouter,
      withNavigationBack(),
    )(TestComponent);

    const wrapper = mount(
      <Router history={history}>
        <Component />
      </Router>,
    );
    wrapper.simulate("click");
    assert.ok(wrapper);
    assert.isTrue(goBackSpy.calledOnce);
    assert.isTrue(pushSpy.notCalled);
  });
});
