// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Spinner, InlineAlert } from "@cockroachlabs/ui-components";
import { assert } from "chai";
import { mount } from "enzyme";
import React from "react";

import { Loading } from "./loading";

const SomeComponent = () => <div>Hello, world!</div>;
const SomeCustomErrorComponent = () => <div>Custom Error</div>;

describe("<Loading>", () => {
  describe("when error is null", () => {
    describe("when loading=false", () => {
      it("renders content.", () => {
        const wrapper = mount(
          <Loading
            loading={false}
            page={"Test"}
            error={null}
            render={() => <SomeComponent />}
          />,
        );
        assert.isTrue(wrapper.find(SomeComponent).exists());
      });
    });

    describe("when loading=true", () => {
      it("renders loading spinner.", () => {
        const wrapper = mount(
          <Loading
            loading={true}
            page={"Test"}
            error={null}
            render={() => <SomeComponent />}
          />,
        );
        assert.isFalse(wrapper.find(SomeComponent).exists());
        assert.isTrue(wrapper.find(Spinner).exists());
      });
    });
  });

  describe("when error is a single error", () => {
    describe("when loading=false", () => {
      it("renders error, regardless of loading value.", () => {
        const wrapper = mount(
          <Loading
            loading={false}
            page={"Test"}
            error={Error("some error message")}
            render={() => <SomeComponent />}
          />,
        );
        assert.isFalse(wrapper.find(SomeComponent).exists());
        assert.isFalse(wrapper.find(Spinner).exists());
        assert.isTrue(wrapper.find(InlineAlert).exists());
      });
    });

    describe("when loading=true", () => {
      it("renders error, regardless of loading value.", () => {
        const wrapper = mount(
          <Loading
            loading={true}
            page={"Test"}
            error={Error("some error message")}
            render={() => <SomeComponent />}
          />,
        );
        assert.isFalse(wrapper.find(SomeComponent).exists());
        assert.isFalse(wrapper.find(Spinner).exists());
        assert.isFalse(wrapper.find(SomeCustomErrorComponent).exists());
        assert.isTrue(wrapper.find(InlineAlert).exists());
      });

      it("render custom error when provided", () => {
        const wrapper = mount(
          <Loading
            loading={true}
            page={"Test"}
            error={Error("some error message")}
            render={() => <SomeComponent />}
            renderError={() => <SomeCustomErrorComponent />}
          />,
        );
        assert.isFalse(wrapper.find(SomeComponent).exists());
        assert.isFalse(wrapper.find(Spinner).exists());
        assert.isTrue(wrapper.find(SomeCustomErrorComponent).exists());
      });
    });
  });

  describe("when error is a list of errors", () => {
    describe("when no errors are null", () => {
      it("renders all errors in list", () => {
        const errors = [Error("error1"), Error("error2"), Error("error3")];
        const wrapper = mount(
          <Loading
            loading={false}
            page={"Test"}
            error={errors}
            render={() => <SomeComponent />}
          />,
        );
        assert.isFalse(wrapper.find(SomeComponent).exists());
        assert.isFalse(wrapper.find(Spinner).exists());
        assert.isTrue(wrapper.find(InlineAlert).exists());
        errors.forEach(e =>
          assert.isTrue(wrapper.find(InlineAlert).text().includes(e.message)),
        );
      });
    });

    describe("when some errors are null", () => {
      it("ignores null list values, rending only valid errors.", () => {
        const errors = [
          null,
          Error("error1"),
          Error("error2"),
          null,
          Error("error3"),
          null,
        ];
        const wrapper = mount(
          <Loading
            loading={false}
            page={"Test"}
            error={errors}
            render={() => <SomeComponent />}
          />,
        );
        assert.isFalse(wrapper.find(SomeComponent).exists());
        assert.isFalse(wrapper.find(Spinner).exists());
        assert.isTrue(wrapper.find(InlineAlert).exists());
        errors
          .filter(e => !!e)
          .forEach(e =>
            assert.isTrue(wrapper.find(InlineAlert).text().includes(e.message)),
          );
      });
    });

    describe("when all errors are null", () => {
      it("renders content, since there are no errors.", () => {
        const wrapper = mount(
          <Loading
            loading={false}
            page={"Test"}
            error={[null, null, null]}
            render={() => <SomeComponent />}
          />,
        );
        assert.isTrue(wrapper.find(SomeComponent).exists());
        assert.isFalse(wrapper.find(Spinner).exists());
        assert.isFalse(wrapper.find(InlineAlert).exists());
      });
    });
  });
});
