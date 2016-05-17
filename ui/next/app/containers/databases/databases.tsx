/// <reference path="../../../typings/main.d.ts" />
import * as React from "react";

/**
 * This is the outer component for the databases page.
 */
export default class extends React.Component<{}, {}> {
  static title() {
    return <h2>Databases</h2>;
  }

  render() {
    // TODO: The first div seems superfluous, remove after switch to ui/next.
    return <div className="section">
      { this.props.children }
    </div>;
  }
}
