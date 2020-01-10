// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import classNames from "classnames";
import { deviation as d3Deviation, mean as d3Mean } from "d3";
import _ from "lodash";
import moment from "moment";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouterState } from "react-router";
import { bindActionCreators, Dispatch } from "redux";
import { createSelector } from "reselect";
import { refreshLiveness, refreshNodes } from "src/redux/apiReducers";
import { LivenessStatus, NodesSummary, nodesSummarySelector, selectLivenessRequestStatus, selectNodeRequestStatus } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { LongToMoment, NanoToMilli } from "src/util/convert";
import { FixLong } from "src/util/fixLong";
import { getFilters, localityToString, NodeFilterList, NodeFilterListProps } from "src/views/reports/components/nodeFilterList";
import Loading from "src/views/shared/components/loading";

interface NetworkOwnProps {
  nodesSummary: NodesSummary;
  nodeSummaryErrors: Error[];
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
}

interface Identity {
  nodeID: number;
  address: string;
  locality: string;
  updatedAt: moment.Moment;
}

interface NoConnection {
  from: Identity;
  to: Identity;
}

type NetworkProps = NetworkOwnProps & RouterState;

// staleTable is a table of all stale nodes.
function staleTable(staleIdentities: Identity[]) {
  if (_.isEmpty(staleIdentities)) {
    return null;
  }

  return (
    <div>
      <h2 className="base-heading">Stale Nodes</h2>
      <table className="failure-table">
        <tbody>
          <tr className="failure-table__row failure-table__row--header">
            <td className="failure-table__cell failure-table__cell--header">
              Node
              </td>
            <td className="failure-table__cell failure-table__cell--header">
              Address
              </td>
            <td className="failure-table__cell failure-table__cell--header">
              Locality
              </td>
            <td className="failure-table__cell failure-table__cell--header">
              Last Updated
              </td>
          </tr>
          {
            _.map(staleIdentities, (staleIdentity) => (
              <tr className="failure-table__row" key={staleIdentity.nodeID}>
                <td className="failure-table__cell">
                  n{staleIdentity.nodeID}
                </td>
                <td className="failure-table__cell">
                  {staleIdentity.address}
                </td>
                <td className="failure-table__cell">
                  {staleIdentity.locality}
                </td>
                <td className="failure-table__cell">
                  {staleIdentity.updatedAt.toString()}
                </td>
              </tr>
            ))
          }
        </tbody>
      </table>
    </div>
  );
}

// noConnectionTable is a list of all good nodes that seem to be missing a connection.
function noConnectionTable(noConnections: NoConnection[]) {
  if (_.isEmpty(noConnections)) {
    return null;
  }

  return (
    <div>
      <h2 className="base-heading">No Connections</h2>
      <table className="failure-table">
        <tbody>
          <tr className="failure-table__row failure-table__row--header">
            <td className="failure-table__cell failure-table__cell--header">
              From Node
              </td>
            <td className="failure-table__cell failure-table__cell--header">
              From Address
              </td>
            <td className="failure-table__cell failure-table__cell--header">
              From Locality
              </td>
            <td className="failure-table__cell failure-table__cell--header">
              To Node
              </td>
            <td className="failure-table__cell failure-table__cell--header">
              To Address
              </td>
            <td className="failure-table__cell failure-table__cell--header">
              To Locality
              </td>
          </tr>
          {
            _.map(noConnections, (noConn) => (
              <tr className="failure-table__row" key={`${noConn.from.nodeID}-${noConn.to.nodeID}`}>
                <td className="failure-table__cell">
                  n{noConn.from.nodeID}
                </td>
                <td className="failure-table__cell">
                  {noConn.from.address}
                </td>
                <td className="failure-table__cell">
                  {noConn.from.locality}
                </td>
                <td className="failure-table__cell">
                  n{noConn.to.nodeID}
                </td>
                <td className="failure-table__cell">
                  {noConn.to.address}
                </td>
                <td className="failure-table__cell">
                  {noConn.to.locality}
                </td>
              </tr>
            ))
          }
        </tbody>
      </table>
    </div>
  );
}

// createHeaderCell creates and decorates a header cell.
function createHeaderCell(staleIDs: Set<number>, id: Identity, key: string) {
  const node = `n${id.nodeID.toString()}`;
  const title = _.join([node, id.address, id.locality], "\n");
  const className = classNames(
    "network-table__cell",
    "network-table__cell--header",
    { "network-table__cell--header-warning": staleIDs.has(id.nodeID) },
  );
  return <td key={key} className={className} title={title}>
    {node}
  </td>;
}

function contentAvailable(nodesSummary: NodesSummary) {
  return !_.isUndefined(nodesSummary) &&
    !_.isEmpty(nodesSummary.nodeStatuses) &&
    !_.isEmpty(nodesSummary.nodeStatusByID) &&
    !_.isEmpty(nodesSummary.nodeIDs);
}

/**
 * Renders the Network Diagnostics Report page.
 */
class Network extends React.Component<NetworkProps, {}> {
  refresh(props = this.props) {
    props.refreshLiveness();
    props.refreshNodes();
  }

  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.refresh();
  }

  componentWillReceiveProps(nextProps: NetworkProps) {
    if (this.props.location !== nextProps.location) {
      this.refresh(nextProps);
    }
  }

  renderLatencyTable(
    latencies: number[],
    staleIDs: Set<number>,
    nodesSummary: NodesSummary,
    displayIdentities: Identity[],
  ) {
    const mean = d3Mean(latencies);
    let stddev = d3Deviation(latencies);
    if (_.isUndefined(stddev)) {
      stddev = 0;
    }
    // If there is no stddev, we should not display a legend. So there is no
    // need to set these values.
    const stddevPlus1 = stddev > 0 ? mean + stddev : 0;
    const stddevPlus2 = stddev > 0 ? stddevPlus1 + stddev : 0;
    const stddevMinus1 = stddev > 0 ? _.max([mean - stddev, 0]) : 0;
    const stddevMinus2 = stddev > 0 ? _.max([stddevMinus1 - stddev, 0]) : 0;

    // getLatencyCell creates and decorates a cell based on it's latency.
    function getLatencyCell(nodeIDa: number, nodeIDb: number) {
      const key = `${nodeIDa}-${nodeIDb}`;
      if (nodeIDa === nodeIDb) {
        return <td key={key} className="network-table__cell network-table__cell--self">
          -
        </td>;
      }
      if (staleIDs.has(nodeIDa) || staleIDs.has(nodeIDb)) {
        return <td key={key} className="network-table__cell network-table__cell--no-connection">
          X
        </td>;
      }
      const a = nodesSummary.nodeStatusByID[nodeIDa].activity;
      if (_.isNil(a) || _.isNil(a[nodeIDb])) {
        return <td key={key} className="network-table__cell network-table__cell--no-connection">
          X
        </td>;
      }
      const nano = FixLong(a[nodeIDb].latency);
      if (nano.eq(0)) {
        return <td key={key} className="network-table__cell network-table__cell--stddev-even">
          ?
        </td>;
      }
      const latency = NanoToMilli(nano.toNumber());
      const className = classNames({
        "network-table__cell": true,
        "network-table__cell--stddev-minus-2": stddev > 0 && latency < stddevMinus2,
        "network-table__cell--stddev-minus-1": stddev > 0 && latency < stddevMinus1 && latency >= stddevMinus2,
        "network-table__cell--stddev-even": stddev > 0 && latency >= stddevMinus1 && latency <= stddevPlus1,
        "network-table__cell--stddev-plus-1": stddev > 0 && latency > stddevPlus1 && latency <= stddevPlus2,
        "network-table__cell--stddev-plus-2": stddev > 0 && latency > stddevPlus2,
      });

      const title = `n${nodeIDa} -> n${nodeIDb}\n${latency.toString()}ms`;
      return <td key={key} className={className} title={title}>
        {latency.toFixed(2)}ms
      </td>;
    }

    // latencyTable is the table and heat-map that's displayed for all nodes.
    const latencyTable = (
      <div key="latency-table">
        <h2 className="base-heading">Latencies</h2>
        <table className="network-table">
          <tbody>
            <tr className="network-table__row">
              <td className="network-table__cell network-table__cell--spacer" />
              {
                _.map(displayIdentities, (identity) => createHeaderCell(
                  staleIDs,
                  identity,
                  `0-${identity.nodeID}`,
                ))
              }
            </tr>
            {
              _.map(displayIdentities, (identityA) => (
                <tr key={identityA.nodeID} className="network-table__row">
                  {
                    createHeaderCell(staleIDs, identityA, `${identityA.nodeID}-0`)
                  }
                  {
                    _.map(displayIdentities, (identityB) => getLatencyCell(
                      identityA.nodeID,
                      identityB.nodeID,
                    ))
                  }
                </tr>
              ))
            }
          </tbody>
        </table>
      </div>
    );

    if (stddev === 0) {
      return latencyTable;
    }

    // legend is just a quick table showing the standard deviation values.
    const legend = (
      <div key="legend">
        <h2 className="base-heading">Legend</h2>
        <table className="network-table">
          <tbody>
            <tr className="network-table__row">
              <td className="network-table__cell network-table__cell--header">
                &lt; -2 stddev
              </td>
              <td className="network-table__cell network-table__cell--header">
                &lt; -1 stddev
              </td>
              <td className="network-table__cell network-table__cell--header">
                mean
              </td>
              <td className="network-table__cell network-table__cell--header">
                &gt; +1 stddev
              </td>
              <td className="network-table__cell network-table__cell--header">
                &gt; +2 stddev
              </td>
            </tr>
            <tr className="network-table__row">
              <td className="network-table__cell network-table__cell--stddev-minus-2">
                {stddevMinus2.toFixed(2)}ms
              </td>
              <td className="network-table__cell network-table__cell--stddev-minus-1">
                {stddevMinus1.toFixed(2)}ms
              </td>
              <td className="network-table__cell network-table__cell--stddev-even">
                {mean.toFixed(2)}ms
              </td>
              <td className="network-table__cell network-table__cell--stddev-plus-1">
                {stddevPlus1.toFixed(2)}ms
              </td>
              <td className="network-table__cell network-table__cell--stddev-plus-2">
                {stddevPlus2.toFixed(2)}ms
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    );

    return [
      latencyTable,
      legend,
    ];
  }

  renderContent(nodesSummary: NodesSummary, filters: NodeFilterListProps) {
    if (!contentAvailable(nodesSummary)) {
      return null;
    }

    // List of node identities.
    const identityByID: Map<number, Identity> = new Map();
    _.forEach(nodesSummary.nodeStatuses, status => {
      identityByID.set(status.desc.node_id, {
        nodeID: status.desc.node_id,
        address: status.desc.address.address_field,
        locality: localityToString(status.desc.locality),
        updatedAt: LongToMoment(status.updated_at),
      });
    });

    // Calculate the mean and sampled standard deviation.
    let healthyIDsContext = _.chain(nodesSummary.nodeIDs)
      .filter(nodeID => nodesSummary.livenessStatusByNodeID[nodeID] === LivenessStatus.LIVE)
      .filter(nodeID => !_.isNil(nodesSummary.nodeStatusByID[nodeID].activity))
      .map(nodeID => Number.parseInt(nodeID, 0));
    let staleIDsContext = _.chain(nodesSummary.nodeIDs)
      .filter(nodeID => nodesSummary.livenessStatusByNodeID[nodeID] === LivenessStatus.UNAVAILABLE)
      .map(nodeID => Number.parseInt(nodeID, 0));
    if (!_.isNil(filters.nodeIDs) && filters.nodeIDs.size > 0) {
      healthyIDsContext = healthyIDsContext.filter(nodeID => filters.nodeIDs.has(nodeID));
      staleIDsContext = staleIDsContext.filter(nodeID => filters.nodeIDs.has(nodeID));
    }
    if (!_.isNil(filters.localityRegex)) {
      healthyIDsContext = healthyIDsContext.filter(nodeID => (
        filters.localityRegex.test(localityToString(nodesSummary.nodeStatusByID[nodeID].desc.locality))
      ));
      staleIDsContext = staleIDsContext.filter(nodeID => (
        filters.localityRegex.test(localityToString(nodesSummary.nodeStatusByID[nodeID].desc.locality))
      ));
    }
    const healthyIDs = healthyIDsContext.value();
    const staleIDs = new Set(staleIDsContext.value());
    const displayIdentities: Identity[] = healthyIDsContext
      .union(staleIDsContext.value())
      .map(nodeID => identityByID.get(nodeID))
      .sortBy(identity => identity.nodeID)
      .sortBy(identity => identity.locality)
      .value();
    const staleIdentities = staleIDsContext
      .map(nodeID => identityByID.get(nodeID))
      .sortBy(identity => identity.nodeID)
      .sortBy(identity => identity.locality)
      .value();
    const latencies = _.flatMap(healthyIDs, nodeIDa => (
      _.chain(healthyIDs)
        .without(nodeIDa)
        .map(nodeIDb => nodesSummary.nodeStatusByID[nodeIDa].activity[nodeIDb])
        .filter(activity => !_.isNil(activity) && !_.isNil(activity.latency))
        .map(activity => NanoToMilli(FixLong(activity.latency).toNumber()))
        .filter(ms => _.isFinite(ms) && ms > 0)
        .value()
    ));

    const noConnections: NoConnection[] = _.flatMap(healthyIDs, nodeIDa => (
      _.chain(nodesSummary.nodeStatusByID[nodeIDa].activity)
        .keys()
        .map(nodeIDb => Number.parseInt(nodeIDb, 10))
        .difference(healthyIDs)
        .map(nodeIDb => ({
          from: identityByID.get(nodeIDa),
          to: identityByID.get(nodeIDb),
        }))
        .sortBy(noConnection => noConnection.to.nodeID)
        .sortBy(noConnection => noConnection.to.locality)
        .sortBy(noConnection => noConnection.from.nodeID)
        .sortBy(noConnection => noConnection.from.locality)
        .value()
    ));

    let content: JSX.Element | JSX.Element[];
    if (_.isEmpty(healthyIDs)) {
      content = <h2 className="base-heading">No healthy nodes match the filters</h2>;
    } else if (latencies.length < 1) {
      content = <h2 className="base-heading">Cannot show latency chart without two healthy nodes.</h2>;
    } else {
      content = this.renderLatencyTable(latencies, staleIDs, nodesSummary, displayIdentities);
    }
    return [
      content,
      staleTable(staleIdentities),
      noConnectionTable(noConnections),
    ];
  }

  render() {
    const { nodesSummary } = this.props;
    const filters = getFilters(this.props.location);
    return (
      <div className="section">
        <Helmet>
          <title>Network Diagnostics | Debug</title>
        </Helmet>
        <h1 className="base-heading">Network Diagnostics</h1>
        <Loading
          loading={!contentAvailable(nodesSummary)}
          error={this.props.nodeSummaryErrors}
          className="loading-image loading-image__spinner-left loading-image__spinner-left__padded"
          render={() => (
            <div>
              <NodeFilterList nodeIDs={filters.nodeIDs} localityRegex={filters.localityRegex} />
              {this.renderContent(nodesSummary, filters)}
            </div>
          )}
        />
      </div>
    );
  }
}

const nodeSummaryErrors = createSelector(
  selectNodeRequestStatus,
  selectLivenessRequestStatus,
  (nodes, liveness) => [nodes.lastError, liveness.lastError],
);

const mapStateToProps = (state: AdminUIState) => ({ // RootState contains declaration for whole state
  nodesSummary: nodesSummarySelector(state),
  nodeSummaryErrors: nodeSummaryErrors(state),
});

const mapDispatchToProps = (dispatch: Dispatch<AdminUIState>) =>
  bindActionCreators(
    {
      refreshNodes,
      refreshLiveness,
    },
    dispatch,
  );

export default connect(mapStateToProps, mapDispatchToProps)(Network);
