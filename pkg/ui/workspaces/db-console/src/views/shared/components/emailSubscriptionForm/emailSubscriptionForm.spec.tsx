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
import { mount, ReactWrapper } from "enzyme";

import { EmailSubscriptionForm } from "./index";

describe("EmailSubscriptionForm", () => {
  let wrapper: ReactWrapper;
  const onSubmitHandler = jest.fn();

  beforeEach(() => {
    onSubmitHandler.mockReset();
    wrapper = mount(<EmailSubscriptionForm onSubmit={onSubmitHandler} />);
  });

  describe("when correct email", () => {
    it("provides entered email on submit callback", () => {
      const emailAddress = "foo@bar.com";
      const inputComponent = wrapper.find("input.crl-input__text").first();
      inputComponent.simulate("change", { target: { value: emailAddress } });
      const buttonComponent = wrapper.find(`button`).first();
      buttonComponent.simulate("click");

      expect(onSubmitHandler).toHaveBeenCalledWith(emailAddress);
    });
  });

  describe("when invalid email", () => {
    beforeEach(() => {
      const emailAddress = "foo";
      const inputComponent = wrapper.find("input.crl-input__text").first();
      inputComponent.simulate("change", { target: { value: emailAddress } });
      inputComponent.simulate("blur");
    });

    it("doesn't call onSubmit callback", () => {
      const buttonComponent = wrapper.find(`button`).first();
      buttonComponent.simulate("click");
      expect(onSubmitHandler).not.toHaveBeenCalled();
    });

    it("submit button is disabled", () => {
      const buttonComponent = wrapper.find(`button[disabled]`).first();
      expect(buttonComponent.exists()).toBe(true);
    });

    it("validation message is shown", () => {
      const validationMessageWrapper = wrapper
        .find(".crl-input__text--error-message")
        .first();
      expect(validationMessageWrapper.exists()).toBe(true);
      expect(validationMessageWrapper.text()).toEqual("Invalid email address.");
    });
  });
});
