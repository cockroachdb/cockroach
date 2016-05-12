/// <reference path="../../typings/main.d.ts" />
import * as React from "react";

/**
 * Renders the main content of the databases page.
 */
export default class extends React.Component<{}, {}> {
  static title() {
    return <h2>Databases</h2>;
  }

  render() {
    return <div className="section">
      <h1>Databases Page</h1>
    </div>;
  }
}
