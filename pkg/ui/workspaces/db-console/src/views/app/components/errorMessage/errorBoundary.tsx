// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { ErrorInfo } from "react";
import Helmet from "react-helmet";

import "./errorMessage.styl";
import SleepyMoonImg from "assets/sleepy-moon.svg";

interface ErrorBoundaryProps {
  onCatch?: (error: Error, errorInfo: ErrorInfo) => void;
}

interface ErrorBoundaryState {
  hasError: boolean;
  error: Error | undefined;
}

// ErrorBoundary with image and text message.
export default class ErrorBoundary extends React.Component<
  ErrorBoundaryProps,
  ErrorBoundaryState
> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = {
      hasError: false,
      error: undefined,
    };
  }

  static getDerivedStateFromError(error: Error) {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo) {
    // Console.error for developer visibility as well as production logging.
    // eslint-disable-next-line no-console
    console.error("[ErrorBoundary::componentDidCatch] error = ", error);
    // eslint-disable-next-line no-console
    console.error("[ErrorBoundary::componentDidCatch] errorInfo = ", errorInfo);
    // eslint-disable-next-line no-console
    console.log("children = ", this.props.children);
    this.props.onCatch && this.props.onCatch(error, errorInfo);
  }

  render() {
    if (!this.state.hasError) {
      return this.props.children;
    }
    return (
      <main className="error-message-page">
        <Helmet title="Error" />
        <div className="error-message-page__content">
          <img className="error-message-page__img" src={SleepyMoonImg} />
          <div className="error-message-page__body">
            <div className="error-message-page__message">
              Something went wrong.
            </div>
            <p>
              There is a problem loading the component of this page. Try
              refreshing the page.
            </p>
          </div>
        </div>
      </main>
    );
  }
}
