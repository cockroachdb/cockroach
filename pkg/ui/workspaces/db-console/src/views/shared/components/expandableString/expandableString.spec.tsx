// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render, screen, fireEvent } from "@testing-library/react";
import React from "react";

import { ExpandableString } from "./index";

describe("ExpandableString", () => {
  const shortText = "a]short string";
  // 52 chars — at the boundary (truncateLength + 2 = 52), still neverCollapse.
  const boundaryText = "a".repeat(52);
  // 53 chars — exceeds the boundary, becomes collapsible.
  const longText = "a]".repeat(25) + "bcd";
  const customShort = "custom summary";

  describe("when text is short enough to never collapse", () => {
    it("renders the full text without expand control", () => {
      const { container } = render(<ExpandableString long={shortText} />);
      expect(screen.getByText(shortText)).toBeDefined();
      // No clickable expandable wrapper should exist.
      expect(container.querySelector(".expandable")).toBeNull();
    });

    it("renders full text at exactly the boundary length (52 chars)", () => {
      const { container } = render(<ExpandableString long={boundaryText} />);
      expect(screen.getByText(boundaryText)).toBeDefined();
      expect(container.querySelector(".expandable")).toBeNull();
    });
  });

  describe("when text exceeds the collapse threshold", () => {
    it("renders truncated text with ellipsis", () => {
      const { container } = render(<ExpandableString long={longText} />);
      // Should show the expandable wrapper.
      expect(container.querySelector(".expandable")).not.toBeNull();
      // The truncated text is the first 50 chars trimmed.
      const truncated = longText.substr(0, 50).trim();
      expect(container.textContent).toContain(truncated);
      // Should show ellipsis character (…).
      expect(container.textContent).toContain("\u2026");
      // Full text should NOT be visible.
      expect(container.querySelector(".expandable__text--expanded")).toBeNull();
    });

    it("expands to show full text on click", () => {
      const { container } = render(<ExpandableString long={longText} />);
      fireEvent.click(container.querySelector(".expandable"));
      // After clicking, the expanded class should appear.
      expect(
        container.querySelector(".expandable__text--expanded"),
      ).not.toBeNull();
      expect(container.textContent).toContain(longText);
    });

    it("collapses back to truncated text on second click", () => {
      const { container } = render(<ExpandableString long={longText} />);
      const wrapper = container.querySelector(".expandable");
      // Expand.
      fireEvent.click(wrapper);
      expect(
        container.querySelector(".expandable__text--expanded"),
      ).not.toBeNull();
      // Collapse.
      fireEvent.click(wrapper);
      expect(container.querySelector(".expandable__text--expanded")).toBeNull();
      const truncated = longText.substr(0, 50).trim();
      expect(container.textContent).toContain(truncated);
    });
  });

  describe("when a custom short prop is provided", () => {
    it("renders the custom short text instead of auto-truncating", () => {
      const { container } = render(
        <ExpandableString long={longText} short={customShort} />,
      );
      expect(container.textContent).toContain(customShort);
      // Should still be expandable.
      expect(container.querySelector(".expandable")).not.toBeNull();
    });

    it("always shows expand control even for short long text", () => {
      // With an explicit short prop, even short long text is collapsible.
      const { container } = render(
        <ExpandableString long={shortText} short={customShort} />,
      );
      expect(container.querySelector(".expandable")).not.toBeNull();
      expect(container.textContent).toContain(customShort);
    });

    it("expands to show the full long text on click", () => {
      const { container } = render(
        <ExpandableString long={longText} short={customShort} />,
      );
      fireEvent.click(container.querySelector(".expandable"));
      expect(container.textContent).toContain(longText);
    });
  });
});
