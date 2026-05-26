// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render, screen, fireEvent } from "@testing-library/react";
import React from "react";

import { EmailSubscriptionForm } from "./index";

describe("EmailSubscriptionForm", () => {
  const onSubmitHandler = jest.fn();

  beforeEach(() => {
    onSubmitHandler.mockReset();
  });

  describe("when correct email", () => {
    it("provides entered email on submit callback", () => {
      render(<EmailSubscriptionForm onSubmit={onSubmitHandler} />);
      const emailAddress = "foo@bar.com";
      fireEvent.change(screen.getByPlaceholderText("Enter your email"), {
        target: { value: emailAddress },
      });
      fireEvent.click(screen.getByRole("button", { name: "Sign up" }));

      expect(onSubmitHandler).toHaveBeenCalledWith(emailAddress);
    });
  });

  describe("when invalid email", () => {
    beforeEach(() => {
      render(<EmailSubscriptionForm onSubmit={onSubmitHandler} />);
      const input = screen.getByPlaceholderText("Enter your email");
      fireEvent.change(input, { target: { value: "foo" } });
      fireEvent.blur(input);
    });

    it("doesn't call onSubmit callback", () => {
      fireEvent.click(screen.getByRole("button", { name: "Sign up" }));
      expect(onSubmitHandler).not.toHaveBeenCalled();
    });

    it("submit button is disabled", () => {
      const button = screen.getByRole("button", {
        name: "Sign up",
      }) as HTMLButtonElement;
      expect(button.disabled).toBe(true);
    });

    it("validation message is shown", () => {
      const errorMessage = screen.getByText("Invalid email address.");
      expect(errorMessage).toBeDefined();
    });
  });
});
