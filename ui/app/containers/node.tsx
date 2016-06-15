import * as React from "react";
import { Link, RouteComponentProps } from "react-router";

import { IndexListLink, ListLink } from "../components/listLink.tsx";
import TimeScaleSelector from "./timescale";

/**
 * Renders teh main content of the single node page.
 */
export default class extends React.Component<RouteComponentProps<any, any>, {}> {
  static title(routes: RouteComponentProps<any, any>) {
    return <h2>
      <Link to="/nodes">Nodes</Link>: Node { routes.params.node_id }
    </h2>;
  }

  render() {
    // Determine whether or not the time scale options should be displayed.
    let child = React.Children.only(this.props.children);
    let displayTimescale = (child as any).type.displayTimeScale === true;

    let baseRoute = `/nodes/${this.props.params.node_id}`;

    // TODO: The first div seems superfluous, remove after switch to ui/next.
    return <div>
      <div className="nav-container">
        <ul className="nav">
          <IndexListLink to={baseRoute}>Overview</IndexListLink>
          <ListLink to={baseRoute + "/graphs"}>Graphs</ListLink>
          <ListLink to={baseRoute + "/logs"}>Logs</ListLink>
          { displayTimescale ? <TimeScaleSelector/> : null }
        </ul>
      </div>
      { this.props.children }
    </div>;
  }
}
