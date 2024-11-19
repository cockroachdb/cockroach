// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { util } from "@cockroachlabs/cluster-ui";
import classNames from "classnames";
import flow from "lodash/flow";
import get from "lodash/get";
import has from "lodash/has";
import isEmpty from "lodash/isEmpty";
import isEqual from "lodash/isEqual";
import isNil from "lodash/isNil";
import join from "lodash/join";
import map from "lodash/map";
import orderBy from "lodash/orderBy";
import uniq from "lodash/uniq";
import Long from "long";
import moment from "moment-timezone";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { withRouter, RouteComponentProps } from "react-router-dom";

import { InlineAlert } from "src/components";
import * as protos from "src/js/protos";
import { refreshLiveness, refreshNodes } from "src/redux/apiReducers";
import {
  LivenessStatus,
  nodeIDsStringifiedSelector,
  selectNodesLastError,
  nodeStatusByIDSelector,
  livenessStatusByNodeIDSelector,
} from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { FixLong } from "src/util/fixLong";
import {
  getFilters,
  localityToString,
  NodeFilterList,
} from "src/views/reports/components/nodeFilterList";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import {
  PageConfig,
  PageConfigItem,
} from "src/views/shared/components/pageconfig";

import { BackToAdvanceDebug } from "../util";

interface NodesOwnProps {
  nodeIds: ReturnType<typeof nodeIDsStringifiedSelector.resultFunc>;
  nodeLastError: ReturnType<typeof selectNodesLastError.resultFunc>;
  nodeStatusByID: ReturnType<typeof nodeStatusByIDSelector.resultFunc>;
  livenessStatusByNodeID: ReturnType<
    typeof livenessStatusByNodeIDSelector.resultFunc
  >;
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
}

interface NodesTableRowParams {
  title: string;
  extract: (
    ns: protos.cockroach.server.status.statuspb.INodeStatus,
  ) => React.ReactNode;
  equality?: (
    ns: protos.cockroach.server.status.statuspb.INodeStatus,
  ) => string;
  cellTitle?: (
    ns: protos.cockroach.server.status.statuspb.INodeStatus,
  ) => string;
}

type NodesProps = NodesOwnProps & RouteComponentProps;

const dateFormat = "Y-MM-DD HH:mm:ss";
const detailTimeFormat = "Y/MM/DD HH:mm:ss";

const loading = (
  <div className="section">
    <h1 className="base-heading">Node Diagnostics</h1>
    <h2 className="base-heading">Loading cluster status...</h2>
  </div>
);

function NodeTableCell(props: { value: React.ReactNode; title: string }) {
  return (
    <td className="nodes-table__cell" title={props.title}>
      {props.value}
    </td>
  );
}

// Functions starting with "print" return a single string representation which
// can be used for title, the main content or even equality comparisons.
function printNodeID(
  status: protos.cockroach.server.status.statuspb.INodeStatus,
) {
  return `n${status.desc.node_id}`;
}

function printSingleValue(value: string) {
  return function (
    status: protos.cockroach.server.status.statuspb.INodeStatus,
  ) {
    return get(status, value, null);
  };
}

function printSingleValueWithFunction(
  value: string,
  fn: (item: any) => string,
) {
  return function (
    status: protos.cockroach.server.status.statuspb.INodeStatus,
  ) {
    return fn(get(status, value, null));
  };
}

function printMultiValue(value: string) {
  return function (
    status: protos.cockroach.server.status.statuspb.INodeStatus,
  ) {
    return join(get(status, value, []), "\n");
  };
}

function printDateValue(value: string, inputDateFormat: string) {
  return function (
    status: protos.cockroach.server.status.statuspb.INodeStatus,
  ) {
    if (!has(status, value)) {
      return null;
    }
    return moment(get(status, value), inputDateFormat).format(dateFormat);
  };
}

function printTimestampValue(value: string) {
  return function (
    status: protos.cockroach.server.status.statuspb.INodeStatus,
  ) {
    if (!has(status, value)) {
      return null;
    }
    return util
      .LongToMoment(FixLong(get(status, value) as Long))
      .format(dateFormat);
  };
}

// Functions starting with "title" are used exclusively to print the cell
// titles. They always return a single string.
function titleDateValue(value: string, inputDateFormat: string) {
  return function (
    status: protos.cockroach.server.status.statuspb.INodeStatus,
  ) {
    if (!has(status, value)) {
      return null;
    }
    const raw = get(status, value);
    return `${moment(raw, inputDateFormat).format(dateFormat)}\n${raw}`;
  };
}

function titleTimestampValue(value: string) {
  return function (
    status: protos.cockroach.server.status.statuspb.INodeStatus,
  ) {
    if (!has(status, value)) {
      return null;
    }
    const raw = FixLong(get(status, value) as Long);
    return `${util.LongToMoment(raw).format(dateFormat)}\n${raw.toString()}`;
  };
}

// Functions starting with "extract" are used exclusively for for extracting
// the main content of a cell.
function extractMultiValue(value: string) {
  return function (
    status: protos.cockroach.server.status.statuspb.INodeStatus,
  ) {
    const items = map(get(status, value, []), item => item.toString());
    return (
      <ul className="nodes-entries-list">
        {map(items, (item, key) => (
          <li key={key} className="nodes-entries-list--item">
            {item}
          </li>
        ))}
      </ul>
    );
  };
}

function extractCertificateLink(
  status: protos.cockroach.server.status.statuspb.INodeStatus,
) {
  const nodeID = status.desc.node_id;
  return (
    <a className="debug-link" href={`#/reports/certificates/${nodeID}`}>
      n{nodeID} Certificates
    </a>
  );
}

const nodesTableRows: NodesTableRowParams[] = [
  {
    title: "Node ID",
    extract: printNodeID,
  },
  {
    title: "Address",
    extract: printSingleValue("desc.address.address_field"),
    cellTitle: printSingleValue("desc.address.address_field"),
  },
  {
    title: "Locality",
    extract: printSingleValueWithFunction("desc.locality", localityToString),
    cellTitle: printSingleValueWithFunction("desc.locality", localityToString),
  },
  {
    title: "Certificates",
    extract: extractCertificateLink,
  },
  {
    title: "Attributes",
    extract: extractMultiValue("desc.attrs.attrs"),
    cellTitle: printMultiValue("desc.attrs.attrs"),
  },
  {
    title: "Environment",
    extract: extractMultiValue("env"),
    cellTitle: printMultiValue("env"),
  },
  {
    title: "Arguments",
    extract: extractMultiValue("args"),
    cellTitle: printMultiValue("args"),
  },
  {
    title: "Tag",
    extract: printSingleValue("build_info.tag"),
    cellTitle: printSingleValue("build_info.tag"),
    equality: printSingleValue("build_info.tag"),
  },
  {
    title: "Revision",
    extract: printSingleValue("build_info.revision"),
    cellTitle: printSingleValue("build_info.revision"),
    equality: printSingleValue("build_info.revision"),
  },
  {
    title: "Time",
    extract: printDateValue("build_info.time", detailTimeFormat),
    cellTitle: titleDateValue("build_info.time", detailTimeFormat),
    equality: printDateValue("build_info.time", detailTimeFormat),
  },
  {
    title: "Type",
    extract: printSingleValue("build_info.type"),
    cellTitle: printSingleValue("build_info.type"),
    equality: printSingleValue("build_info.type"),
  },
  {
    title: "Platform",
    extract: printSingleValue("build_info.platform"),
    cellTitle: printSingleValue("build_info.platform"),
    equality: printSingleValue("build_info.platform"),
  },
  {
    title: "Go Version",
    extract: printSingleValue("build_info.go_version"),
    cellTitle: printSingleValue("build_info.go_version"),
    equality: printSingleValue("build_info.go_version"),
  },
  {
    title: "CGO",
    extract: printSingleValue("build_info.cgo_compiler"),
    cellTitle: printSingleValue("build_info.cgo_compiler"),
    equality: printSingleValue("build_info.cgo_compiler"),
  },
  {
    title: "Distribution",
    extract: printSingleValue("build_info.distribution"),
    cellTitle: printSingleValue("build_info.distribution"),
    equality: printSingleValue("build_info.distribution"),
  },
  {
    title: "Started at",
    extract: printTimestampValue("started_at"),
    cellTitle: titleTimestampValue("started_at"),
  },
  {
    title: "Updated at",
    extract: printTimestampValue("updated_at"),
    cellTitle: titleTimestampValue("updated_at"),
  },
];

type LocalNodeState = { selectFilter: number | null };
/**
 * Renders the Nodes Diagnostics Report page.
 */
export class Nodes extends React.Component<NodesProps, LocalNodeState> {
  constructor(props: NodesProps) {
    super(props);

    this.state = {
      selectFilter: isEmpty(getFilters(this.props.location))
        ? LivenessStatus.NODE_STATUS_LIVE
        : null,
    };
  }
  refresh(props = this.props) {
    props.refreshLiveness();
    props.refreshNodes();
  }

  componentDidMount() {
    // Refresh nodes status query when mounting.
    this.refresh();
  }

  componentDidUpdate(prevProps: NodesProps) {
    if (!isEqual(this.props.location, prevProps.location)) {
      this.refresh(this.props);
    }
  }

  renderNodesTableRow(
    orderedNodeIDs: string[],
    key: number,
    title: string,
    extract: (
      ns: protos.cockroach.server.status.statuspb.INodeStatus,
    ) => React.ReactNode,
    equality?: (
      ns: protos.cockroach.server.status.statuspb.INodeStatus,
    ) => string,
    cellTitle?: (
      ns: protos.cockroach.server.status.statuspb.INodeStatus,
    ) => string,
  ) {
    const inconsistent =
      !isNil(equality) &&
      flow(
        (nodeIds: string[]) =>
          map(nodeIds, nodeID => this.props.nodeStatusByID[nodeID]),
        statuses => map(statuses, status => equality(status)),
        uniq,
      )(orderedNodeIDs).length > 1;
    const headerClassName = classNames(
      "nodes-table__cell",
      "nodes-table__cell--header",
      { "nodes-table__cell--header-warning": inconsistent },
    );

    return (
      <tr className="nodes-table__row" key={key}>
        <th className={headerClassName}>{title}</th>
        {map(orderedNodeIDs, nodeID => {
          const status = this.props.nodeStatusByID[nodeID];
          return (
            <NodeTableCell
              key={nodeID}
              value={extract(status)}
              title={isNil(cellTitle) ? null : cellTitle(status)}
            />
          );
        })}
      </tr>
    );
  }

  requiresAdmin() {
    return (
      this.props.nodeLastError?.message ===
      "this operation requires admin privilege"
    );
  }

  render() {
    const { nodeStatusByID, livenessStatusByNodeID, nodeIds } = this.props;
    if (this.requiresAdmin()) {
      return (
        <InlineAlert title="" message="This page requires admin privileges." />
      );
    }

    if (isEmpty(nodeIds)) {
      return loading;
    }

    const filters = getFilters(this.props.location);

    let nodeIDsContext = nodeIds.map((nodeID: string) =>
      Number.parseInt(nodeID, 10),
    );
    if (!isNil(filters.nodeIDs) && filters.nodeIDs.size > 0) {
      nodeIDsContext = nodeIDsContext.filter(nodeID =>
        filters.nodeIDs.has(nodeID),
      );
    }
    if (!isNil(filters.localityRegex)) {
      nodeIDsContext = nodeIDsContext.filter(nodeID =>
        filters.localityRegex.test(
          localityToString(nodeStatusByID[nodeID.toString()].desc.locality),
        ),
      );
    }
    if (this.state.selectFilter !== null) {
      nodeIDsContext = nodeIDsContext.filter(nodeID => {
        // For this context, if the user chooses active nodes,
        // only include nodes with a liveness status of DEAD, LIVE
        // or UNAVAILABLE (suspect).
        if (this.state.selectFilter === LivenessStatus.NODE_STATUS_LIVE) {
          return [
            LivenessStatus.NODE_STATUS_DEAD,
            LivenessStatus.NODE_STATUS_LIVE,
            LivenessStatus.NODE_STATUS_UNAVAILABLE,
          ].includes(livenessStatusByNodeID[nodeID]);
        }
        return (
          livenessStatusByNodeID[nodeID] ===
          LivenessStatus.NODE_STATUS_DECOMMISSIONED
        );
      });
    }

    // Sort the node IDs and then convert them back to string for lookups.
    const orderedNodeIDs = orderBy(nodeIDsContext, nodeID => nodeID).map(
      nodeID => nodeID.toString(),
    );

    const dropdownOptions: DropdownOption[] = [
      {
        value: LivenessStatus.NODE_STATUS_LIVE.toString(),
        label: "Active Nodes",
      },
      {
        value: LivenessStatus.NODE_STATUS_DECOMMISSIONED.toString(),
        label: "Decomissioned Nodes",
      },
      { value: "", label: "All Nodes" },
    ];

    return (
      <section className="section">
        <Helmet title="Node Diagnostics | Debug" />
        <BackToAdvanceDebug history={this.props.history} />
        <h1 className="base-heading">Node Diagnostics</h1>
        <NodeFilterList
          nodeIDs={filters.nodeIDs}
          localityRegex={filters.localityRegex}
        />
        {!isEmpty(orderedNodeIDs) && <h2 className="base-heading">Nodes</h2>}
        {isEmpty(filters) && (
          <PageConfig>
            <PageConfigItem>
              <Dropdown
                title="Node Selection"
                options={dropdownOptions}
                selected={
                  this.state.selectFilter === null
                    ? ""
                    : this.state.selectFilter.toString()
                }
                onChange={selected =>
                  this.setState({
                    selectFilter:
                      selected.value === "" ? null : parseInt(selected.value),
                  })
                }
              />
            </PageConfigItem>
          </PageConfig>
        )}
        {isEmpty(orderedNodeIDs) ? (
          <section className="section">
            <NodeFilterList
              nodeIDs={filters.nodeIDs}
              localityRegex={filters.localityRegex}
            />
            <h2 className="base-heading">No nodes match the filters</h2>
          </section>
        ) : (
          <table className="nodes-table">
            <tbody>
              {map(nodesTableRows, (row, key) => {
                return this.renderNodesTableRow(
                  orderedNodeIDs,
                  key,
                  row.title,
                  row.extract,
                  row.equality,
                  row.cellTitle,
                );
              })}
            </tbody>
          </table>
        )}
      </section>
    );
  }
}

const mapStateToProps = (state: AdminUIState) => ({
  nodeIds: nodeIDsStringifiedSelector(state),
  nodeLastError: selectNodesLastError(state),
  nodeStatusByID: nodeStatusByIDSelector(state),
  livenessStatusByNodeID: livenessStatusByNodeIDSelector(state),
});

const mapDispatchToProps = {
  refreshNodes,
  refreshLiveness,
};

export default withRouter(connect(mapStateToProps, mapDispatchToProps)(Nodes));
