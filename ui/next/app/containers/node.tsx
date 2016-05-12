/// <reference path="../../typings/main.d.ts" />
import * as React from "react";

import { Link, RouteComponentProps } from "react-router";

/**
 * Renders teh main content of the single node page.
 */
export default class extends React.Component<{}, {}> {
  static title(routes: RouteComponentProps<any, any>) {
    return <h2>
      <Link to="/nodes">Nodes</Link>: Node { routes.params.node_id }
    </h2>;
  }

  render() {
    return <div className="section">
      <h1>Single Node Page</h1>
    </div>;
  }
}
