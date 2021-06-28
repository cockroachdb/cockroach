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
import { mount } from "enzyme";
import { Spinner, InlineAlert } from "@cockroachlabs/ui-components";
import { Loading } from "./loading";

const SomeComponent = () => <div>Hello, world!</div>;

describe("<Loading>", () => {
  describe("when error is null", () => {
    describe("when loading=false", () => {
      it("renders content.", () => {
        const wrapper = mount(
          <Loading
            loading={false}
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
            error={Error("some error message")}
            render={() => <SomeComponent />}
          />,
        );
        assert.isFalse(wrapper.find(SomeComponent).exists());
        assert.isFalse(wrapper.find(Spinner).exists());
        assert.isTrue(wrapper.find(InlineAlert).exists());
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
            error={errors}
            render={() => <SomeComponent />}
          />,
        );
        assert.isFalse(wrapper.find(SomeComponent).exists());
        assert.isFalse(wrapper.find(Spinner).exists());
        assert.isTrue(wrapper.find(InlineAlert).exists());
        errors.forEach(e =>
          assert.isTrue(
            wrapper
              .find(InlineAlert)
              .text()
              .includes(e.message),
          ),
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
            assert.isTrue(
              wrapper
                .find(InlineAlert)
                .text()
                .includes(e.message),
            ),
          );
      });
    });

    describe("when all errors are null", () => {
      it("renders content, since there are no errors.", () => {
        const wrapper = mount(
          <Loading
            loading={false}
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
