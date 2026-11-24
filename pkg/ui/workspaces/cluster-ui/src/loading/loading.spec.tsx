// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import React from "react";

import { Loading } from "./loading";

const SomeComponent = () => <div>Hello, world!</div>;
const SomeCustomErrorComponent = () => <div>Custom Error</div>;

describe("<Loading>", () => {
  describe("when error is null", () => {
    describe("when loading=false", () => {
      it("renders content.", () => {
        render(
          <Loading
            loading={false}
            page={"Test"}
            error={null}
            render={() => <SomeComponent />}
          />,
        );
        expect(screen.getByText("Hello, world!")).toBeInTheDocument();
      });
    });

    describe("when loading=true", () => {
      it("renders loading spinner.", () => {
        render(
          <Loading
            loading={true}
            page={"Test"}
            error={null}
            render={() => <SomeComponent />}
          />,
        );
        expect(screen.queryByText("Hello, world!")).not.toBeInTheDocument();
        expect(screen.getByTestId("loading-spinner")).toBeInTheDocument();
      });
    });
  });

  describe("when error is a single error", () => {
    describe("when loading=false", () => {
      it("renders error, regardless of loading value.", () => {
        render(
          <Loading
            loading={false}
            page={"Test"}
            error={Error("some error message")}
            render={() => <SomeComponent />}
          />,
        );
        expect(screen.queryByText("Hello, world!")).not.toBeInTheDocument();
        expect(screen.queryByTestId("loading-spinner")).not.toBeInTheDocument();
        expect(screen.getByText("some error message")).toBeInTheDocument();
      });
    });

    describe("when loading=true", () => {
      it("renders error, regardless of loading value.", () => {
        render(
          <Loading
            loading={true}
            page={"Test"}
            error={Error("some error message")}
            render={() => <SomeComponent />}
          />,
        );
        expect(screen.queryByText("Hello, world!")).not.toBeInTheDocument();
        expect(screen.queryByTestId("loading-spinner")).not.toBeInTheDocument();
        expect(screen.queryByText("Custom Error")).not.toBeInTheDocument();
        expect(screen.getByText("some error message")).toBeInTheDocument();
      });

      it("render custom error when provided", () => {
        render(
          <Loading
            loading={true}
            page={"Test"}
            error={Error("some error message")}
            render={() => <SomeComponent />}
            renderError={() => <SomeCustomErrorComponent />}
          />,
        );
        expect(screen.queryByText("Hello, world!")).not.toBeInTheDocument();
        expect(screen.queryByTestId("loading-spinner")).not.toBeInTheDocument();
        expect(screen.getByText("Custom Error")).toBeInTheDocument();
      });
    });
  });

  describe("when error is a list of errors", () => {
    describe("when no errors are null", () => {
      it("renders all errors in list", () => {
        const errors = [Error("error1"), Error("error2"), Error("error3")];
        const { container } = render(
          <Loading
            loading={false}
            page={"Test"}
            error={errors}
            render={() => <SomeComponent />}
          />,
        );
        expect(screen.queryByText("Hello, world!")).not.toBeInTheDocument();
        expect(screen.queryByTestId("loading-spinner")).not.toBeInTheDocument();
        errors.forEach(e => expect(container.textContent).toContain(e.message));
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
        const { container } = render(
          <Loading
            loading={false}
            page={"Test"}
            error={errors}
            render={() => <SomeComponent />}
          />,
        );
        expect(screen.queryByText("Hello, world!")).not.toBeInTheDocument();
        expect(screen.queryByTestId("loading-spinner")).not.toBeInTheDocument();
        errors
          .filter(e => !!e)
          .forEach(e => expect(container.textContent).toContain(e.message));
      });
    });

    describe("when all errors are null", () => {
      it("renders content, since there are no errors.", () => {
        render(
          <Loading
            loading={false}
            page={"Test"}
            error={[null, null, null]}
            render={() => <SomeComponent />}
          />,
        );
        expect(screen.getByText("Hello, world!")).toBeInTheDocument();
        expect(screen.queryByTestId("loading-spinner")).not.toBeInTheDocument();
      });
    });
  });
});
