import * as React from "react";

/**
 * This is the outer component for the databases page.
 */
export default class extends React.Component<{}, {}> {
  static title() {
    return <h2>Databases</h2>;
  }

  render() {
    return <div className="section">
      { this.props.children }
    </div>;
  }
}
