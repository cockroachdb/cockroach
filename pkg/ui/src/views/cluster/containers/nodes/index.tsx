import _ from "lodash";
import React from "react";
import PropTypes from "prop-types";
import { InjectedRouter, RouterState } from "react-router";
import { connect } from "react-redux";

import * as protos from "src/js/protos";

import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import { PageConfig, PageConfigItem } from "src/views/shared/components/pageconfig";

import { refreshNodes } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";

import {
  nodeIDAttr, dashboardNameAttr,
} from "src/util/constants";

import TimeScaleDropdown from "src/views/cluster/containers/timescale";

interface ClusterOverviewOwnProps {
  nodes: protos.cockroach.server.status.NodeStatus[];
  refreshNodes: typeof refreshNodes;
}

type ClusterOverviewProps = ClusterOverviewOwnProps & RouterState;

interface ClusterOverviewState {
  nodeOptions: { value: string, label: string }[];
}

const dashboards = [
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
  // TODO(mrtracy): Switch this, and the other uses of contextTypes, to use the
  // 'withRouter' HoC after upgrading to react-router 4.x.
  static contextTypes = {
    router: PropTypes.object.isRequired,
  };
  context: { router: InjectedRouter & RouterState; };

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
    const base = [{ value: "", label: "Cluster"}];
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
    const child = React.Children.only(this.props.children);
    const displayTimescale = (child as any).type.displayTimeScale === true;

    const dashboard = this.props.params[dashboardNameAttr];
    const node = this.props.params[nodeIDAttr] || "";

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
