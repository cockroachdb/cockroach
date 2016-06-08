import * as React from "react";
import Events from "./events";

/**
 * Renders the content of the Cluster events tab.
 */
export default class extends React.Component<{}, {}> {
  static title() {
    return <h2>Cluster</h2>;
  }

  render() {
    return <div className="section table">
      <Events />
    </div>;
  }
}
