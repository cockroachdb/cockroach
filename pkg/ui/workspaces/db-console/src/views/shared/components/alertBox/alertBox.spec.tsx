// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";

import { AlertLevel } from "src/redux/alerts";

import { AlertBox, AlertBoxProps } from "./index";

describe("AlertBox", () => {
  const defaultProps: AlertBoxProps = {
    title: "Test Alert",
    level: AlertLevel.WARNING,
    dismiss: jest.fn(),
  };

  const renderAlertBox = (overrides: Partial<AlertBoxProps> = {}) =>
    render(<AlertBox {...defaultProps} {...overrides} />);

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("renders title and text", () => {
    renderAlertBox({ text: "Some details" });
    expect(screen.getByText("Test Alert")).toBeDefined();
    expect(screen.getByText("Some details")).toBeDefined();
  });

  it("calls dismiss when close button is clicked", async () => {
    const dismiss = jest.fn();
    renderAlertBox({ dismiss });
    await userEvent.click(screen.getByText("✕"));
    expect(dismiss).toHaveBeenCalledTimes(1);
  });

  describe("Learn More link", () => {
    it("renders when link prop is provided", () => {
      renderAlertBox({ link: "https://example.com" });
      const learnMore = screen.getByText("Learn More");
      expect(learnMore).toBeDefined();
      expect(learnMore.getAttribute("href")).toBe("https://example.com");
    });

    it("does not render when link prop is absent", () => {
      renderAlertBox();
      expect(screen.queryByText("Learn More")).toBeNull();
    });
  });

  describe("CSS class by alert level", () => {
    const levels: { level: AlertLevel; className: string }[] = [
      { level: AlertLevel.CRITICAL, className: "alert-box--critical" },
      { level: AlertLevel.WARNING, className: "alert-box--warning" },
      { level: AlertLevel.INFORMATION, className: "alert-box--information" },
      { level: AlertLevel.NOTIFICATION, className: "alert-box--notification" },
      { level: AlertLevel.SUCCESS, className: "alert-box--success" },
    ];

    test.each(levels)(
      "applies $className for AlertLevel.$level",
      ({ level, className }) => {
        const { container } = renderAlertBox({ level });
        const root = container.querySelector(".alert-box");
        expect(root?.classList.contains(className)).toBe(true);
      },
    );
  });
});
