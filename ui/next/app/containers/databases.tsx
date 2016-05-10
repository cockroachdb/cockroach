/// <reference path="../../typings/main.d.ts" />
import * as React from "react";

/**
 * DatabasesTitle renders the main content of the databases page.
 */
export class DatabasesMain extends React.Component<{}, {}> {
  render() {
    return <div className="section">
      <h1>Databases Page</h1>
    </div>;
  }
}

/**
 * DatabasesTitle renders the header of the databases page.
 */
export class DatabasesTitle extends React.Component<{}, {}> {
  render() {
    return <h2>Databases</h2>;
  }
}
