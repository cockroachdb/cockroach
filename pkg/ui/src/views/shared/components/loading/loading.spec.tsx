// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import _ from "lodash";
import React from "react";
import { assert } from "chai";
import {mount, ReactWrapper} from "enzyme";

import "src/enzymeInit";
import Loading from "src/views/shared/components/loading";

const LOADING_CLASS_NAME = "loading-class-name";
const RENDER_CLASS_NAME = "render-class-name";
const ERROR_CLASS_NAME = "loading-error";
const ALL_CLASS_NAMES = [LOADING_CLASS_NAME, ERROR_CLASS_NAME, RENDER_CLASS_NAME];

interface MakeLoadingProps {
  loading: boolean;
  error?: Error | Error[] | null;
  renderClassName?: string;
}

interface AssertExpectedProps {
  onlyVisibleClass: string;
  errorCount?: number;
}

describe("<Loading>", () => {
  describe("renders correctly for various states.", () => {
    it("should render content.", () => {
      const wrapper = makeLoadingComponent({
        loading: false, error: null,
        renderClassName: "my-rendered-content",
      });
      assertExpectedState(wrapper, {
        onlyVisibleClass: "my-rendered-content",
      });
    });

    it("renders loader.", () => {
      const wrapper = makeLoadingComponent({
        loading: true, error: null,
      });
      assertExpectedState(wrapper, {
        onlyVisibleClass: LOADING_CLASS_NAME,
      });
    });

    it("renders error.", () => {
      const wrapper = makeLoadingComponent({
        loading: false,
        error: Error("some error message"),
      });
      assertExpectedState(wrapper, {
        onlyVisibleClass: ERROR_CLASS_NAME,
        errorCount: 1,
      });
    });

    it("renders error even if loading is true.", () => {
      const wrapper = makeLoadingComponent({
        loading: true,
        error: Error("some error message"),
      });
      assertExpectedState(wrapper, {
        onlyVisibleClass: ERROR_CLASS_NAME,
        errorCount: 1,
      });
    });

    it("can render list of errors.", () => {
      const wrapper = makeLoadingComponent({
        loading: false,
        error: [
          Error("error1"),
          Error("error2"),
          Error("error3"),
        ],
      });
      assertExpectedState(wrapper, {
        onlyVisibleClass: ERROR_CLASS_NAME,
        errorCount: 3,
      });
    });

    it("should ignore null errors.", () => {
      const wrapper = makeLoadingComponent({
        loading: false,
        error: [
          null,
          Error("error1"),
          Error("error2"),
          null,
          Error("error3"),
          null,
        ],
      });
      assertExpectedState(wrapper, {
        onlyVisibleClass: ERROR_CLASS_NAME,
        errorCount: 3,
      });
    });

    it("list of one error renders same as single error.", () => {
      const wrapper = makeLoadingComponent({
        loading: false,
        error: [
          null,
          null,
          Error("error3"),
          null,
        ],
      });
      assertExpectedState(wrapper, {
        onlyVisibleClass: ERROR_CLASS_NAME,
        errorCount: 1,
      });
    });

    it("renders content because there are no errors.", () => {
      const wrapper = makeLoadingComponent({
        loading: false,
        error: [
          null,
          null,
          null,
        ],
        renderClassName: "no-errors-so-should-render-me",
      });
      assertExpectedState(wrapper, {
        onlyVisibleClass: "no-errors-so-should-render-me",
      });
    });
  });
});

function assertExpectedState(
  wrapper: ReactWrapper,
  props: AssertExpectedProps,
) {
  // Assert that onlyVisibleClass is rendered, and that all classes are not.
  _.map(ALL_CLASS_NAMES, (className) => {
    const expectedVisibility = props.onlyVisibleClass === className;
    const expectedLength = expectedVisibility ? 1 : 0;
    const element = "div." + className;
    assert.lengthOf(
      wrapper.find(element),
      expectedLength, "expected " + element +
      (expectedVisibility ? " to be visible" : " to not be rendered"));
  });

  if (props.errorCount) {
    // Single errors are rendered inside a <pre></pre>;
    // Multiple errors are listed in <ul><li> list.
    const expectedSelector = props.errorCount === 1 ? "pre" : "li";
    assert.lengthOf(
      wrapper.find("div." + ERROR_CLASS_NAME).find(expectedSelector),
      props.errorCount,
      "number of errors was not " + props.errorCount,
    );
  }
}

function makeLoadingComponent(
  props: MakeLoadingProps,
) {
  return mount(<Loading
    loading={props.loading}
    error={props.error}
    className={LOADING_CLASS_NAME}
    render={() => (
      <div className={props.renderClassName || RENDER_CLASS_NAME}>Hello, world!</div>
    )}
    />);
}
