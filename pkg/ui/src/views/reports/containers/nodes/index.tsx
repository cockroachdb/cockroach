import _ from "lodash";
import React from "react";
import { connect } from "react-redux";
import moment from "moment";
import { RouterState } from "react-router";

import { refreshLiveness, refreshNodes } from "src/redux/apiReducers";
import { NodesSummary, nodesSummarySelector } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { LongToMoment } from "src/util/convert";
import { getFilters, localityToString, NodeFilterList } from "src/views/reports/components/nodeFilterList";

interface NodesOwnProps {
  nodesSummary: NodesSummary;
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
}

interface Detail {
  [name: string]: string[];
}

interface Title {
  readonly variable: string;
  readonly display: string;
  readonly equality: boolean; // When true, displays a warning when all values don't match.
}

const dateFormat = "Y-MM-DD HH:mm:ss";
const detailTimeFormat = "Y/MM/DD HH:mm:ss";

const displayList: Title[] = [
  { variable: "nodeID", display: "Node ID", equality: false },
  { variable: "address", display: "Address", equality: false },
  { variable: "locality", display: "Locality", equality: false },
  { variable: "attributes", display: "Attributes", equality: false },
  { variable: "environment", display: "Environment", equality: false },
  { variable: "arguments", display: "Arguments", equality: false },
  { variable: "tag", display: "Tag", equality: true },
  { variable: "revision", display: "Revision", equality: true },
  { variable: "time", display: "Time", equality: true },
  { variable: "type", display: "Type", equality: true },
  { variable: "platform", display: "Platform", equality: true },
  { variable: "goVersion", display: "Go Version", equality: true },
  { variable: "cgo", display: "CGO", equality: true },
  { variable: "distribution", display: "Distribution", equality: true },
  { variable: "startedAt", display: "Started at", equality: false },
  { variable: "updatedAt", display: "Updated at", equality: false },
];

type NodesProps = NodesOwnProps & RouterState;

const loading = (
  <div className="section">
    <h1>Loading cluster status...</h1>
  </div>
);

/**
 * Renders the Nodes Diagnostics Report page.
 */
class Nodes extends React.Component<NodesProps, {}> {
  refresh(props = this.props) {
    props.refreshLiveness();
    props.refreshNodes();
  }

  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.refresh();
  }

  componentWillReceiveProps(nextProps: NodesProps) {
    if (this.props.location !== nextProps.location) {
      this.refresh(nextProps);
    }
  }

  renderResultsCell(title: Title, nodeDetail: Detail, key: number) {
    return (
      <td key={key} className="nodes-table__cell" title={
        _.join(nodeDetail[title.variable], "\n")
      }>
        <ul className="nodes-entries-list">
          {
            _.map(nodeDetail[title.variable], (value, k) => (
              <li key={k}>
                {value}
              </li>
            ))
          }
        </ul>
      </td>
    );
  }

  renderResultsRow(title: Title, nodeDetails: Detail[], key: number) {
    let headerClassName: string = "nodes-table__cell nodes-table__cell--header";
    if (title.equality && _.chain(nodeDetails)
      .map(detail => _.join(detail[title.variable], " "))
      .uniq()
      .value()
      .length > 1) {
      headerClassName += " nodes-table__cell--header-warning";
    }
    return (
      <tr key={key} className="nodes-table__row">
        <th className={headerClassName}>
          {title.display}
        </th>
        {
          _.map(nodeDetails, (detail, key2) => (
            this.renderResultsCell(title, detail, key2)
          ))
        }
      </tr>
    );
  }

  render() {
    const { nodesSummary } = this.props;
    if (_.isEmpty(nodesSummary.nodeIDs)) {
      return loading;
    }

    const filters = getFilters(this.props.location);

    let nodeIDsContext = _.chain(nodesSummary.nodeIDs)
      .map(nodeID => Number.parseInt(nodeID, 10));
    if (!_.isNil(filters.nodeIDs) && filters.nodeIDs.size > 0) {
      nodeIDsContext = nodeIDsContext.filter(nodeID => filters.nodeIDs.has(nodeID));
    }
    if (!_.isNil(filters.localityRegex)) {
      nodeIDsContext = nodeIDsContext.filter(nodeID => (
        filters.localityRegex.test(localityToString(nodesSummary.nodeStatusByID[nodeID].desc.locality))
      ));
    }

    const nodeDetails: Detail[] = nodeIDsContext
      .map(nodeID => nodesSummary.nodeStatusByID[nodeID])
      .map(status => {
        return {
          nodeID: [`n${status.desc.node_id}`],
          address: [status.desc.address.address_field],
          locality: [localityToString(status.desc.locality)],
          attributes: status.desc.attrs.attrs,
          environment: status.env,
          arguments: status.args,
          tag: [status.build_info.tag],
          revision: [status.build_info.revision],
          time: [moment(status.build_info.time, detailTimeFormat).format(dateFormat)],
          type: [status.build_info.type],
          platform: [status.build_info.platform],
          goVersion: [status.build_info.go_version],
          cgo: [status.build_info.cgo_compiler],
          distribution: [status.build_info.distribution],
          startedAt: [LongToMoment(status.started_at).format(dateFormat)],
          updatedAt: [LongToMoment(status.updated_at).format(dateFormat)],
        };
      })
      .sortBy(identity => identity.nodeID)
      .sortBy(identity => identity.locality)
      .value();

    if (_.isEmpty(nodeDetails)) {
      return (
        <div>
          <h1>Node Diagnostics</h1>
          <NodeFilterList nodeIDs={filters.nodeIDs} localityRegex={filters.localityRegex} />
          <h2>No nodes match the filters</h2>
        </div>
      );
    }

    return (
      <div>
        <h1>Node Diagnostics</h1>
        <NodeFilterList nodeIDs={filters.nodeIDs} localityRegex={filters.localityRegex} />
        <h2>Nodes</h2>
        <table className="nodes-table">
          <tbody>
            {
              _.map(displayList, (title, key) => (
                this.renderResultsRow(title, nodeDetails, key)
              ))
            }
          </tbody>
        </table>
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    nodesSummary: nodesSummarySelector(state),
  };
}

const actions = {
  refreshNodes,
  refreshLiveness,
};

export default connect(mapStateToProps, actions)(Nodes);
