import _ from "lodash";
import * as React from "react";
import { IRouter, IInjectedProps } from "react-router";
import { connect } from "react-redux";
import { PageConfig, PageConfigItem } from "../components/pageconfig";

import { refreshNodes } from "../redux/apiReducers";
import Selector, { SelectorOption } from "../components/selector";
import { AdminUIState } from "../redux/state";

import {
  nodeIDAttr, dashboardNameAttr,
} from "../util/constants";

import TimeScaleSelector from "./timescale";

interface ClusterOverviewOwnProps {
  nodes: Proto2TypeScript.cockroach.server.status.NodeStatus[];
  refreshNodes: typeof refreshNodes;
}

type ClusterOverviewProps = ClusterOverviewOwnProps & IInjectedProps;

interface ClusterOverviewState {
  nodeOptions: { value: string, label: string }[];
}

let dashboards = [
  { value: "activity", label: "Activity" },
  { value: "queries", label: "SQL Queries" },
  { value: "resources", label: "System Resources" },
  { value: "internals", label: "Advanced Internals" },
];

/**
 * Renders the layout of the nodes page.
 */
class ClusterOverview extends React.Component<ClusterOverviewProps, ClusterOverviewState> {
  // Magic to add react router to the context.
  // See https://github.com/ReactTraining/react-router/issues/975
  static contextTypes = {
    router: React.PropTypes.object.isRequired,
  };
  context: { router?: IRouter & IInjectedProps; };

  state: ClusterOverviewState = {
    nodeOptions: [{ value: "", label: "Cluster"}],
  };

  static title() {
    return "Cluster Overview";
  }

  componentWillMount() {
    this.props.refreshNodes();
  }

  componentWillReceiveProps(props: ClusterOverviewProps) {
    let base = [{ value: "", label: "Cluster"}];
    if (props.nodes) {
      this.setState({
        nodeOptions: base.concat(_.map(props.nodes, (n) => {
          return {
            value: n.desc.node_id.toString(),
            label: n.desc.address.address_field,
          };
        })),
      });
    }
  }

  setClusterPath(nodeID: string, dashboardName: string) {
    if (!_.isString(nodeID) || nodeID === "") {
      this.context.router.push(`/cluster/all/${dashboardName}`);
    } else {
      this.context.router.push(`/cluster/node/${nodeID}/${dashboardName}`);
    }
  }

  nodeChange = (selected: SelectorOption) => {
    this.setClusterPath(selected.value, this.props.params[dashboardNameAttr]);
  }

  dashChange = (selected: SelectorOption) => {
    this.setClusterPath(this.props.params[nodeIDAttr], selected.value);
  }

  render() {
    // Determine whether or not the time scale options should be displayed.
    let child = React.Children.only(this.props.children);
    let displayTimescale = (child as any).type.displayTimeScale === true;

    let dashboard = this.props.params[dashboardNameAttr];
    let node = this.props.params[nodeIDAttr] || "";

    return <div>
      <PageConfig>
        <PageConfigItem>
          <Selector title="Graph" options={this.state.nodeOptions}
                    selected={node} onChange={this.nodeChange} />
        </PageConfigItem>
        <PageConfigItem>
          <Selector title="Dashboard" options={dashboards}
                    selected={dashboard} onChange={this.dashChange} />
        </PageConfigItem>
        <PageConfigItem>
            {displayTimescale ? <TimeScaleSelector /> : null }
        </PageConfigItem>
      </PageConfig>
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
