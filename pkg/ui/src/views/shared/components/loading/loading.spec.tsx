// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import React from "react";
import { assert } from "chai";

import "src/enzymeInit";
import Loading from "src/views/shared/components/loading";
import { ReactWrapper, mount } from "enzyme";

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

const makeLoadingComponent = (props: MakeLoadingProps) => mount(
  <Loading
    loading={props.loading}
    error={props.error}
    className={LOADING_CLASS_NAME}
    render={() => (<div className={props.renderClassName || RENDER_CLASS_NAME}>Hello, world!</div>)}
  />,
);

describe("<Loading>", () => {

  describe("when error is null", () => {
    describe("when loading=false", () => {
      it("renders content.", () => {
        const wrapper = makeLoadingComponent({
          loading: false, error: null,
          renderClassName: "my-rendered-content",
        });
        assertExpectedState(wrapper, {
          onlyVisibleClass: "my-rendered-content",
        });
      });
    });

    describe("when loading=true", () => {
      it("renders loading spinner.", () => {
        const wrapper = makeLoadingComponent({
          loading: true, error: null,
        });
        assertExpectedState(wrapper, {
          onlyVisibleClass: LOADING_CLASS_NAME,
        });
      });
    });
  });

  describe("when error is a single error", () => {
    describe("when loading=false", () => {
      it("renders error, regardless of loading value.", () => {
        const wrapper = makeLoadingComponent({
          loading: false,
          error: Error("some error message"),
        });
        assertExpectedState(wrapper, {
          onlyVisibleClass: ERROR_CLASS_NAME,
          errorCount: 1,
        });
      });
    });

    describe("when loading=true", () => {
      it("renders error, regardless of loading value.", () => {
        const wrapper = makeLoadingComponent({
          loading: true,
          error: Error("some error message"),
        });
        assertExpectedState(wrapper, {
          onlyVisibleClass: ERROR_CLASS_NAME,
          errorCount: 1,
        });
      });
    });
  });

  describe("when error is a list of errors", () => {
    describe("when no errors are null", () => {
      it("renders all errors in list", () => {
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
    });

    describe("when some errors are null", () => {
      it("ignores null list values, rending only valid errors.", () => {
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
    });

    describe("when all errors are null", () => {
      it("renders content, since there are no errors.", () => {
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
    assert.lengthOf(
      wrapper.find("div." + ERROR_CLASS_NAME).find("li"),
      props.errorCount,
    );
  }
}
