/// <reference path="../../typings/main.d.ts" />
import * as React from "react";

/**
 * Renders the main content of the help us page.
 */
export default class extends React.Component<{}, {}> {
  static title() {
    return <h2>Help Cockroach Labs</h2>;
  }

  render() {
    return <div className="section">
      <h1>HelpUs Page</h1>
    </div>;
  }
}
