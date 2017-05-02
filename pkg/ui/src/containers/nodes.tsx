import _ from "lodash";
import * as React from "react";
import { InjectedRouter, RouterState } from "react-router";
import { connect } from "react-redux";

import * as protos from "../js/protos";

import Dropdown, { DropdownOption } from "../components/dropdown";
import { PageConfig, PageConfigItem } from "../components/pageconfig";

import { refreshNodes } from "../redux/apiReducers";
import { AdminUIState } from "../redux/state";

import {
  nodeIDAttr, dashboardNameAttr,
} from "../util/constants";

import TimeScaleDropdown from "./timescale";

interface ClusterOverviewOwnProps {
  nodes: protos.cockroach.server.status.NodeStatus[];
  refreshNodes: typeof refreshNodes;
}

type ClusterOverviewProps = ClusterOverviewOwnProps & RouterState;

interface ClusterOverviewState {
  nodeOptions: { value: string, label: string }[];
}

let dashboards = [
  { value: "overview", label: "Overview" },
  { value: "runtime", label: "Runtime" },
  { value: "sql", label: "SQL" },
  { value: "storage", label: "Storage" },
  { value: "replication", label: "Replication" },
  { value: "distributed", label: "Distributed" },
  { value: "queues", label: "Queues" },
  { value: "requests", label: "Slow Requests" },
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
  context: { router?: InjectedRouter & RouterState; };

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

  nodeChange = (selected: DropdownOption) => {
    this.setClusterPath(selected.value, this.props.params[dashboardNameAttr]);
  }

  dashChange = (selected: DropdownOption) => {
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
          <Dropdown title="Graph" options={this.state.nodeOptions}
                    selected={node} onChange={this.nodeChange} />
        </PageConfigItem>
        <PageConfigItem>
          <Dropdown title="Dashboard" options={dashboards}
                    selected={dashboard} onChange={this.dashChange} />
        </PageConfigItem>
        <PageConfigItem>
            {displayTimescale ? <TimeScaleDropdown /> : null }
        </PageConfigItem>
      </PageConfig>
      { this.props.children }
    </div>;
  }
}

export default connect(
  (state: AdminUIState, _ownProps: RouterState) => {
    return {
      nodes: state.cachedData.nodes.data,
    };
  },
  {
    refreshNodes,
  },
)(ClusterOverview);
