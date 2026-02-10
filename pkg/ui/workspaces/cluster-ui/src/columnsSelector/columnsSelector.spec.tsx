// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import "@testing-library/jest-dom";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import React from "react";

import ColumnsSelector, { SelectOption } from "./columnsSelector";

const defaultOptions: SelectOption[] = [
  { label: "Column A", value: "colA", isSelected: true },
  { label: "Column B", value: "colB", isSelected: true },
  { label: "Column C", value: "colC", isSelected: false },
];

describe("ColumnsSelector", () => {
  it("renders the Columns button", () => {
    render(
      <ColumnsSelector options={defaultOptions} onSubmitColumns={jest.fn()} />,
    );

    expect(screen.getByText("Columns")).toBeInTheDocument();
  });

  it("dropdown is initially hidden", () => {
    const { container } = render(
      <ColumnsSelector options={defaultOptions} onSubmitColumns={jest.fn()} />,
    );

    // The dropdown area should have the "hide" class
    const dropdownArea = container.querySelector(".hide");
    expect(dropdownArea).toBeInTheDocument();
  });

  it("opens dropdown when Columns button is clicked", async () => {
    const { container } = render(
      <ColumnsSelector options={defaultOptions} onSubmitColumns={jest.fn()} />,
    );

    fireEvent.click(screen.getByText("Columns"));

    // After clicking, dropdown should have "dropdown-area" class (visible)
    await waitFor(() => {
      expect(container.querySelector(".dropdown-area")).toBeInTheDocument();
    });

    expect(screen.getByText("Hide/show columns")).toBeInTheDocument();
  });

  it("displays all column options plus All option", async () => {
    render(
      <ColumnsSelector options={defaultOptions} onSubmitColumns={jest.fn()} />,
    );

    fireEvent.click(screen.getByText("Columns"));

    await waitFor(() => {
      expect(screen.getByText("All")).toBeInTheDocument();
      expect(screen.getByText("Column A")).toBeInTheDocument();
      expect(screen.getByText("Column B")).toBeInTheDocument();
      expect(screen.getByText("Column C")).toBeInTheDocument();
    });
  });

  it("displays Apply button", async () => {
    render(
      <ColumnsSelector options={defaultOptions} onSubmitColumns={jest.fn()} />,
    );

    fireEvent.click(screen.getByText("Columns"));

    await waitFor(() => {
      expect(screen.getByText("Apply")).toBeInTheDocument();
    });
  });

  it("calls onSubmitColumns with selected values when Apply is clicked", async () => {
    const onSubmitColumns = jest.fn();
    render(
      <ColumnsSelector
        options={defaultOptions}
        onSubmitColumns={onSubmitColumns}
      />,
    );

    fireEvent.click(screen.getByText("Columns"));

    await waitFor(() => {
      expect(screen.getByText("Apply")).toBeVisible();
    });

    fireEvent.click(screen.getByText("Apply"));

    // Should be called with initially selected columns (colA and colB)
    expect(onSubmitColumns).toHaveBeenCalledWith(["colA", "colB"]);
  });

  it("closes dropdown after Apply is clicked", async () => {
    const { container } = render(
      <ColumnsSelector options={defaultOptions} onSubmitColumns={jest.fn()} />,
    );

    fireEvent.click(screen.getByText("Columns"));

    // After clicking, dropdown should have "dropdown-area" class (visible)
    await waitFor(() => {
      expect(container.querySelector(".dropdown-area")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText("Apply"));

    // After applying, dropdown should have "hide" class
    await waitFor(() => {
      expect(container.querySelector(".hide")).toBeInTheDocument();
    });
  });

  it("renders checkboxes for each option", async () => {
    const { container } = render(
      <ColumnsSelector options={defaultOptions} onSubmitColumns={jest.fn()} />,
    );

    fireEvent.click(screen.getByText("Columns"));

    await waitFor(() => {
      // 4 checkboxes: All + 3 columns
      const checkboxes = container.querySelectorAll('input[type="checkbox"]');
      expect(checkboxes).toHaveLength(4);
    });
  });

  it("initially selected options have checked checkboxes", async () => {
    const { container } = render(
      <ColumnsSelector options={defaultOptions} onSubmitColumns={jest.fn()} />,
    );

    fireEvent.click(screen.getByText("Columns"));

    await waitFor(() => {
      const checkboxes = container.querySelectorAll('input[type="checkbox"]');
      // All option is not selected (not all columns are selected)
      expect(checkboxes[0]).not.toBeChecked();
      // Column A is selected
      expect(checkboxes[1]).toBeChecked();
      // Column B is selected
      expect(checkboxes[2]).toBeChecked();
      // Column C is not selected
      expect(checkboxes[3]).not.toBeChecked();
    });
  });

  it("All option is checked when all columns are selected", async () => {
    const allSelectedOptions: SelectOption[] = [
      { label: "Column A", value: "colA", isSelected: true },
      { label: "Column B", value: "colB", isSelected: true },
    ];

    const { container } = render(
      <ColumnsSelector
        options={allSelectedOptions}
        onSubmitColumns={jest.fn()}
      />,
    );

    fireEvent.click(screen.getByText("Columns"));

    await waitFor(() => {
      const checkboxes = container.querySelectorAll('input[type="checkbox"]');
      // All option should be checked when all columns are selected
      expect(checkboxes[0]).toBeChecked();
    });
  });

  it("renders with small size", () => {
    render(
      <ColumnsSelector
        options={defaultOptions}
        onSubmitColumns={jest.fn()}
        size="small"
      />,
    );

    // The button should still render
    expect(screen.getByText("Columns")).toBeInTheDocument();
  });
});
