import _ from "lodash";
import * as React from "react";
import { IInjectedProps } from "react-router";
import { connect } from "react-redux";

import { refreshNodes } from "../redux/apiReducers";
import Selector, { SelectorOption } from "../components/selector";
import { AdminUIState } from "../redux/state";

import TimeScaleSelector from "./timescale";
import { dashQueryString, nodeQueryString } from "../util/constants";

interface ClusterOverviewOwnProps {
  nodes: Proto2TypeScript.cockroach.server.status.NodeStatus[];
  refreshNodes: typeof refreshNodes;
}

class ClusterOverviewState {
  nodeOptions: SelectorOption[] = [];
}

type ClusterOverviewProps = ClusterOverviewOwnProps & IInjectedProps;

let dashboards = [
  { value: "node.activity", label: "Activity" },
  { value: "node.queries", label: "SQL Queries" },
  { value: "node.resources", label: "System Resources" },
  { value: "node.internals", label: "Advanced Internals" },
];

/**
 * Renders the layout of the nodes page.
 */
class ClusterOverview extends React.Component<ClusterOverviewProps, ClusterOverviewState> {
  state = new ClusterOverviewState();

  static title() {
    return <h2>Nodes</h2>;
  }

  componenetWillMount() {
    this.props.refreshNodes();
  }

  componentWillReceiveProps(props: ClusterOverviewProps) {
    let base = [{ value: "", label: "All nodes"}];
    if (props.nodes) {
      this.setState({
        nodeOptions: base.concat(_.map(props.nodes, (n) => { return { value: n.desc.node_id.toString(), label: n.desc.address.address_field }; })),
      });
    }
  }

  render() {
    // Determine whether or not the time scale options should be displayed.
    let child = React.Children.only(this.props.children);
    let displayTimescale = (child as any).type.displayTimeScale === true;

    // TODO(mrtracy): this outer div is used to spare the children
    // `nav-container's styling. Should those styles apply only to `nav`?
    return <div>
      <div className="nav-container">
        <ul className="nav">
          <li className="title">Nodes:</li>
          <li className="selector" style={{ width: 250 }}>
            <Selector urlKey={nodeQueryString} options={this.state.nodeOptions} /></li>
          <li className="title">Dashboard:</li>
          <li className="selector" style={{ width: 200 }}>
            <Selector urlKey={dashQueryString} options={dashboards} />
          </li>
          <li className="title">View:</li>
          {/* <li>Last 6 Hours</li> */}
          {displayTimescale ? <TimeScaleSelector /> : null }
        </ul>
      </div>
      { this.props.children }
    </div>;
  }
}

export default connect(
  (state: AdminUIState, ownProps: IInjectedProps) => {
    return {
      nodes: state.cachedData.nodes.data,
    };
  },
  {
    refreshNodes,
  }
)(ClusterOverview);
