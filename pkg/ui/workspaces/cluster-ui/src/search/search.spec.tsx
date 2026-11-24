// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import "@testing-library/jest-dom";
import { render, screen, fireEvent } from "@testing-library/react";
import React from "react";

import { Search } from "./search";

describe("Search", () => {
  it("renders with default placeholder", () => {
    render(<Search />);
    expect(
      screen.getByPlaceholderText("Search Statements"),
    ).toBeInTheDocument();
  });

  it("renders with custom placeholder", () => {
    render(<Search placeholder="Search Jobs" />);
    expect(screen.getByPlaceholderText("Search Jobs")).toBeInTheDocument();
  });

  it("renders with default value", () => {
    render(<Search defaultValue="initial query" />);
    expect(screen.getByDisplayValue("initial query")).toBeInTheDocument();
  });

  it("calls onChange when typing", () => {
    const onChange = jest.fn();
    render(<Search onChange={onChange} />);

    const input = screen.getByPlaceholderText("Search Statements");
    fireEvent.change(input, { target: { value: "test query" } });

    expect(onChange).toHaveBeenCalledWith("test query");
  });

  it("calls onSubmit when pressing Enter", () => {
    const onSubmit = jest.fn();
    render(<Search onSubmit={onSubmit} />);

    const input = screen.getByPlaceholderText("Search Statements");
    fireEvent.change(input, { target: { value: "search term" } });
    fireEvent.keyDown(input, { key: "Enter", code: "Enter" });

    expect(onSubmit).toHaveBeenCalledWith("search term");
  });

  it("shows Enter button when there is text and not yet submitted", () => {
    render(<Search />);

    const input = screen.getByPlaceholderText("Search Statements");
    fireEvent.change(input, { target: { value: "some text" } });

    expect(screen.getByText("Enter")).toBeInTheDocument();
  });

  it("calls onSubmit when clicking Enter button", () => {
    const onSubmit = jest.fn();
    render(<Search onSubmit={onSubmit} />);

    const input = screen.getByPlaceholderText("Search Statements");
    fireEvent.change(input, { target: { value: "click submit" } });

    const enterButton = screen.getByText("Enter");
    fireEvent.click(enterButton);

    expect(onSubmit).toHaveBeenCalledWith("click submit");
  });

  it("shows clear button after submission and calls onClear when clicked", () => {
    const onClear = jest.fn();
    render(<Search onClear={onClear} />);

    const input = screen.getByPlaceholderText("Search Statements");
    fireEvent.change(input, { target: { value: "submitted query" } });
    fireEvent.keyDown(input, { key: "Enter", code: "Enter" });

    // After submission, Enter button should be replaced with clear button
    expect(screen.queryByText("Enter")).not.toBeInTheDocument();

    // Find and click the clear button (it's a button with the cancel icon)
    const clearButton = screen.getByRole("button");
    fireEvent.click(clearButton);

    expect(onClear).toHaveBeenCalled();
  });

  it("clears the input value when clear button is clicked", () => {
    render(<Search />);

    const input = screen.getByPlaceholderText("Search Statements");
    fireEvent.change(input, { target: { value: "to be cleared" } });
    fireEvent.keyDown(input, { key: "Enter", code: "Enter" });

    const clearButton = screen.getByRole("button");
    fireEvent.click(clearButton);

    expect(input).toHaveValue("");
  });

  it("hides suffix when suffix prop is false", () => {
    render(<Search suffix={false} />);

    const input = screen.getByPlaceholderText("Search Statements");
    fireEvent.change(input, { target: { value: "some text" } });

    expect(screen.queryByText("Enter")).not.toBeInTheDocument();
  });
});
