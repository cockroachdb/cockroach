/// <reference path="../../typings/main.d.ts" />
import * as React from "react";

/**
 * HelpUsTitle renders the main content of the help us page.
 */
export class HelpUsMain extends React.Component<{}, {}> {
  render() {
    return <div className="section">
      <h1>HelpUs Page</h1>
    </div>;
  }
}

/**
 * HelpUsTitle renders the header of the help us page.
 */
export class HelpUsTitle extends React.Component<{}, {}> {
  render() {
    return <h2>HelpUs</h2>;
  }
}
