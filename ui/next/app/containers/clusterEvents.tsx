/// <reference path="../../typings/main.d.ts" />
import * as React from "react";

/**
 * Renders the content of the Cluster events tab.
 */
export default class extends React.Component<{}, {}> {
  static title() {
    return <h2>Cluster</h2>;
  }

  render() {
    return <div className="section">
      <h1>Cluster Events</h1>
    </div>;
  }
}
