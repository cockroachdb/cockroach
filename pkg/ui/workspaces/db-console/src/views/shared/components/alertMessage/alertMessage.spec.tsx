// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render, screen } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import { AlertLevel } from "src/redux/alerts";

import { AlertMessage } from "./alertMessage";

describe("AlertMessage", () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  const defaultProps = {
    title: "Test title",
    level: AlertLevel.WARNING,
    autoClose: false,
    dismiss: jest.fn(),
  };

  const renderAlertMessage = (overrides: Record<string, unknown> = {}) =>
    render(
      <MemoryRouter>
        <AlertMessage {...defaultProps} {...overrides} />
      </MemoryRouter>,
    );

  describe("auto-close timer", () => {
    it("calls dismiss after autoCloseTimeout when autoClose is true", () => {
      const dismiss = jest.fn();
      renderAlertMessage({ autoClose: true, autoCloseTimeout: 3000, dismiss });

      expect(dismiss).not.toHaveBeenCalled();
      jest.advanceTimersByTime(3000);
      expect(dismiss).toHaveBeenCalledTimes(1);
    });

    it("uses default 6000ms timeout when autoCloseTimeout is not provided", () => {
      const dismiss = jest.fn();
      renderAlertMessage({ autoClose: true, dismiss });

      jest.advanceTimersByTime(5999);
      expect(dismiss).not.toHaveBeenCalled();
      jest.advanceTimersByTime(1);
      expect(dismiss).toHaveBeenCalledTimes(1);
    });

    it("does not set timer when autoClose is false", () => {
      const dismiss = jest.fn();
      renderAlertMessage({ autoClose: false, dismiss });

      jest.advanceTimersByTime(10000);
      expect(dismiss).not.toHaveBeenCalled();
    });

    it("clears timer on unmount before it fires", () => {
      const dismiss = jest.fn();
      const { unmount } = renderAlertMessage({
        autoClose: true,
        autoCloseTimeout: 5000,
        dismiss,
      });

      jest.advanceTimersByTime(2000);
      unmount();
      jest.advanceTimersByTime(5000);
      expect(dismiss).not.toHaveBeenCalled();
    });
  });

  describe("description link", () => {
    it("renders text as a Link when link prop is provided", () => {
      renderAlertMessage({ text: "Click here", link: "/some/path" });
      const linkEl = screen.getByText("Click here");
      expect(linkEl.tagName).toBe("A");
      expect(linkEl.getAttribute("href")).toBe("/some/path");
    });

    it("renders plain text when link prop is absent", () => {
      renderAlertMessage({ text: "Plain text" });
      const textEl = screen.getByText("Plain text");
      expect(textEl.tagName).not.toBe("A");
    });
  });
});
