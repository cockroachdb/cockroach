// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import "@testing-library/jest-dom";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import React from "react";

import { Dropdown, DropdownOption } from "./dropdown";

const defaultItems: DropdownOption[] = [
  { value: "option1", name: "Option 1" },
  { value: "option2", name: "Option 2" },
  { value: "option3", name: "Option 3" },
];

describe("Dropdown", () => {
  it("renders the toggle button with children text", () => {
    render(
      <Dropdown items={defaultItems} onChange={jest.fn()}>
        Select Option
      </Dropdown>,
    );

    expect(screen.getByText("Select Option")).toBeInTheDocument();
  });

  it("menu is initially closed", () => {
    const { container } = render(
      <Dropdown items={defaultItems} onChange={jest.fn()}>
        Select
      </Dropdown>,
    );

    // Menu should not have the open class initially
    const menu = container.querySelector(".crl-dropdown__menu");
    expect(menu).not.toHaveClass("crl-dropdown__menu--open");
  });

  it("opens menu when toggle button is clicked", async () => {
    const { container } = render(
      <Dropdown items={defaultItems} onChange={jest.fn()}>
        Select
      </Dropdown>,
    );

    const toggleButton = screen.getByText("Select");
    fireEvent.click(toggleButton);

    const menu = container.querySelector(".crl-dropdown__menu");
    await waitFor(() => {
      expect(menu).toHaveClass("crl-dropdown__menu--open");
    });

    // All items should be present
    expect(screen.getByText("Option 1")).toBeInTheDocument();
    expect(screen.getByText("Option 2")).toBeInTheDocument();
    expect(screen.getByText("Option 3")).toBeInTheDocument();
  });

  it("calls onChange with selected value when item is clicked", async () => {
    const onChange = jest.fn();
    const { container } = render(
      <Dropdown items={defaultItems} onChange={onChange}>
        Select
      </Dropdown>,
    );

    // Open menu
    fireEvent.click(screen.getByText("Select"));

    const menu = container.querySelector(".crl-dropdown__menu");
    await waitFor(() => {
      expect(menu).toHaveClass("crl-dropdown__menu--open");
    });

    // Click on an item
    fireEvent.click(screen.getByText("Option 2"));

    expect(onChange).toHaveBeenCalledWith("option2");
  });

  it("closes menu after selecting an item", async () => {
    const { container } = render(
      <Dropdown items={defaultItems} onChange={jest.fn()}>
        Select
      </Dropdown>,
    );

    // Open menu
    fireEvent.click(screen.getByText("Select"));

    const menu = container.querySelector(".crl-dropdown__menu");
    await waitFor(() => {
      expect(menu).toHaveClass("crl-dropdown__menu--open");
    });

    // Click on an item
    fireEvent.click(screen.getByText("Option 1"));

    // Menu should close (lose the open class)
    await waitFor(() => {
      expect(menu).not.toHaveClass("crl-dropdown__menu--open");
    });
  });

  it("renders custom toggle button when provided", () => {
    render(
      <Dropdown
        items={defaultItems}
        onChange={jest.fn()}
        customToggleButton={<button>Custom Button</button>}
      >
        Default Text
      </Dropdown>,
    );

    expect(screen.getByText("Custom Button")).toBeInTheDocument();
    expect(screen.queryByText("Default Text")).not.toBeInTheDocument();
  });

  it("renders disabled items with correct styling", async () => {
    const itemsWithDisabled: DropdownOption[] = [
      { value: "enabled", name: "Enabled Option" },
      { value: "disabled", name: "Disabled Option", disabled: true },
    ];

    const { container } = render(
      <Dropdown items={itemsWithDisabled} onChange={jest.fn()}>
        Select
      </Dropdown>,
    );

    // Open menu
    fireEvent.click(screen.getByText("Select"));

    const menu = container.querySelector(".crl-dropdown__menu");
    await waitFor(() => {
      expect(menu).toHaveClass("crl-dropdown__menu--open");
    });

    // Check that disabled item has the disabled class
    const disabledItem = container.querySelector(
      ".crl-dropdown__item--disabled",
    );
    expect(disabledItem).toBeInTheDocument();
    expect(disabledItem).toHaveTextContent("Disabled Option");
  });

  it("supports generic value types", async () => {
    interface CustomValue {
      id: number;
      label: string;
    }

    const items: DropdownOption<CustomValue>[] = [
      { value: { id: 1, label: "first" }, name: "First" },
      { value: { id: 2, label: "second" }, name: "Second" },
    ];

    const onChange = jest.fn();
    const { container } = render(
      <Dropdown<CustomValue> items={items} onChange={onChange}>
        Select
      </Dropdown>,
    );

    fireEvent.click(screen.getByText("Select"));

    const menu = container.querySelector(".crl-dropdown__menu");
    await waitFor(() => {
      expect(menu).toHaveClass("crl-dropdown__menu--open");
    });

    fireEvent.click(screen.getByText("First"));

    expect(onChange).toHaveBeenCalledWith({ id: 1, label: "first" });
  });

  it("applies menuPosition class correctly", () => {
    const { container } = render(
      <Dropdown items={defaultItems} onChange={jest.fn()} menuPosition="right">
        Select
      </Dropdown>,
    );

    const menu = container.querySelector(".crl-dropdown__menu--align-right");
    expect(menu).toBeInTheDocument();
  });
});
