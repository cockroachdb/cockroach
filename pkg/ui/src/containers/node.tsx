import * as React from "react";
import { Link, RouterState } from "react-router";

import { nodeIDAttr } from "./../util/constants";

import { IndexListLink, ListLink } from "../components/listLink";
import TimeScaleSelector from "./timescale";

/**
 * Renders the main content of the single node page.
 */
export default class extends React.Component<RouterState, {}> {
  static title(routes: RouterState) {
    return <h2>
      <Link to="/nodes">Nodes</Link>: Node { routes.params[nodeIDAttr] }
    </h2>;
  }

  render() {
    // Determine whether or not the time scale options should be displayed.
    let child = React.Children.only(this.props.children);
    let displayTimescale = (child as any).type.displayTimeScale === true;

    let baseRoute = `/nodes/${this.props.params[nodeIDAttr]}`;

    // TODO(mrtracy): this outer div is used to spare the children
    // `nav-container's styling. Should those styles apply only to `nav`?
    return <div>
      <div className="nav-container">
        <ul className="nav">
          <IndexListLink to={baseRoute}>Overview</IndexListLink>
          <ListLink to={baseRoute + "/logs"}>Logs</ListLink>
          { displayTimescale ? <TimeScaleSelector/> : null }
        </ul>
      </div>
      { this.props.children }
    </div>;
  }
}
