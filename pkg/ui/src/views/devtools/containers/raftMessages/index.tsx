import _ from "lodash";
import PropTypes from "prop-types";
import React from "react";
import { connect } from "react-redux";
import { InjectedRouter, RouterState } from "react-router";
import { createSelector } from "reselect";

import { refreshNodes, refreshLiveness } from "src/redux/apiReducers";
import { hoverStateSelector, HoverState, hoverOn, hoverOff } from "src/redux/hover";
import { nodesSummarySelector, NodesSummary } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { nodeIDAttr } from "src/util/constants";
import {
  GraphDashboardProps, storeIDsForNode,
} from "src/views/cluster/containers/nodeGraphs/dashboards/dashboardUtils";
import TimeScaleDropdown from "src/views/cluster/containers/timescale";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import { PageConfig, PageConfigItem } from "src/views/shared/components/pageconfig";
import { MetricsDataProvider } from "src/views/shared/containers/metricDataProvider";

import messagesDashboard from "./messages";

// The properties required by a NodeGraphs component.
interface NodeGraphsOwnProps {
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
  hoverOn: typeof hoverOn;
  hoverOff: typeof hoverOff;
  nodesQueryValid: boolean;
  livenessQueryValid: boolean;
  nodesSummary: NodesSummary;
  hoverState: HoverState;
}

type NodeGraphsProps = NodeGraphsOwnProps & RouterState;

/**
 * NodeGraphs renders the main content of the cluster graphs page.
 */
class NodeGraphs extends React.Component<NodeGraphsProps, {}> {
  // Magic to add react router to the context.
  // See https://github.com/ReactTraining/react-router/issues/975
  // TODO(mrtracy): Switch this, and the other uses of contextTypes, to use the
  // 'withRouter' HoC after upgrading to react-router 4.x.
  static contextTypes = {
    router: PropTypes.object.isRequired,
  };
  context: { router: InjectedRouter & RouterState; };

  /**
   * Selector to compute node dropdown options from the current node summary
   * collection.
   */
  private nodeDropdownOptions = createSelector(
    (summary: NodesSummary) => summary.nodeStatuses,
    (summary: NodesSummary) => summary.nodeDisplayNameByID,
    (nodeStatuses, nodeDisplayNameByID): DropdownOption[] => {
      const base = [{value: "", label: "Cluster"}];
      return base.concat(_.map(nodeStatuses, (ns) => {
        return {
          value: ns.desc.node_id.toString(),
          label: nodeDisplayNameByID[ns.desc.node_id],
        };
      }));
    },
  );

  static title() {
    return "Raft Messages";
  }

  refresh(props = this.props) {
    if (!props.nodesQueryValid) {
      props.refreshNodes();
    }
    if (!props.livenessQueryValid) {
      props.refreshLiveness();
    }
  }

  setClusterPath(nodeID: string) {
    if (!_.isString(nodeID) || nodeID === "") {
      this.context.router.push("/raft/messages/all/");
    } else {
      this.context.router.push(`/raft/messages/node/${nodeID}`);
    }
  }

  nodeChange = (selected: DropdownOption) => {
    this.setClusterPath(selected.value);
  }

  componentWillMount() {
    this.refresh();
  }

  componentWillReceiveProps(props: NodeGraphsProps) {
    this.refresh(props);
  }

  render() {
    const { params, nodesSummary, hoverState, hoverOn, hoverOff } = this.props;

    const selectedNode = params[nodeIDAttr] || "";
    const nodeSources = (selectedNode !== "") ? [selectedNode] : null;

    // When "all" is the selected source, some graphs display a line for every
    // node in the cluster using the nodeIDs collection. However, if a specific
    // node is already selected, these per-node graphs should only display data
    // only for the selected node.
    const nodeIDs = nodeSources ? nodeSources : nodesSummary.nodeIDs;

    // If a single node is selected, we need to restrict the set of stores
    // queried for per-store metrics (only stores that belong to that node will
    // be queried).
    const storeSources = nodeSources ? storeIDsForNode(nodesSummary, nodeSources[0]) : null;

    // tooltipSelection is a string used in tooltips to reference the currently
    // selected nodes. This is a prepositional phrase, currently either "across
    // all nodes" or "on node X".
    const tooltipSelection = (nodeSources && nodeSources.length === 1)
                              ? `on node ${nodeSources[0]}`
                              : "across all nodes";

    const dashboardProps: GraphDashboardProps = {
      nodeIDs,
      nodesSummary,
      nodeSources,
      storeSources,
      tooltipSelection,
    };

    // Generate graphs for the current dashboard, wrapping each one in a
    // MetricsDataProvider with a unique key.
    const graphs = messagesDashboard(dashboardProps);
    const graphComponents = _.map(graphs, (graph, idx) => {
      const key = `nodes.raftMessages.${idx}`;
      return (
        <div key={key}>
          <MetricsDataProvider id={key}>
            { React.cloneElement(graph, { hoverOn, hoverOff, hoverState }) }
          </MetricsDataProvider>
        </div>
      );
    });

    return (
      <div>
        <PageConfig>
          <PageConfigItem>
            <Dropdown
              title="Graph"
              options={this.nodeDropdownOptions(this.props.nodesSummary)}
              selected={selectedNode}
              onChange={this.nodeChange}
            />
          </PageConfigItem>
          <PageConfigItem>
            <TimeScaleDropdown />
          </PageConfigItem>
        </PageConfig>
        <div className="section l-columns">
          <div className="chart-group l-columns__left">
            { graphComponents }
          </div>
        </div>
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    nodesSummary: nodesSummarySelector(state),
    nodesQueryValid: state.cachedData.nodes.valid,
    livenessQueryValid: state.cachedData.nodes.valid,
    hoverState: hoverStateSelector(state),
  };
}
const actions = {
  refreshNodes,
  refreshLiveness,
  hoverOn,
  hoverOff,
};
export default connect(mapStateToProps, actions)(NodeGraphs);
