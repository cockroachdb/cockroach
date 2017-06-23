import React from "react";
import { connect } from "react-redux";
import _ from "lodash";

import * as protos from "src/js/protos";
import { AdminUIState } from "src/redux/state";
import { refreshNodes } from "src/redux/apiReducers";
import { RouterState } from "react-router";
import { LongToMoment } from "src/util/convert";
import { FilterList } from "src/views/reports/components/filterList";
import moment from "moment";
import Long from "long";

const staleStoreCutoff = moment.duration(1, "minute");
const deadStoreCutoff = moment.duration(1, "day");

interface NetworkOwnProps {
  nodes: protos.cockroach.server.status.NodeStatus$Properties[];
  refreshNodes: typeof refreshNodes;
}

type NetworkProps = NetworkOwnProps & RouterState;

function getNodeIDs(input: string) {
  const ids: Map<number, {}> = new Map();
  if (!_.isEmpty(input)) {
    _.forEach(_.split(input, ","), nodeIDString => {
      const nodeID = parseInt(nodeIDString, 10);
      if (nodeID) {
        ids.set(nodeID, {});
      }
    });
  }
  return ids;
}

function nsToMs(ns: Long) {
  return ns.div(1000).toNumber() / 1000;
}

function localityToString(locality: protos.cockroach.roachpb.Locality$Properties) {
  return _.join(_.map(locality.tiers, (tier) => tier.key + "=" + tier.value), ",");
}

/**
 * Renders the Network Diagnostics Report page.
 */
class Network extends React.Component<NetworkProps, {}> {
  refresh(props = this.props) {
    props.refreshNodes(new protos.cockroach.server.serverpb.NodesRequest({
      node_ids: (!_.isEmpty(props.location.query.node_ids)) ? props.location.query.node_ids : "",
      locality: (!_.isEmpty(props.location.query.locality)) ? props.location.query.locality : "",
    }));
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

  render() {
    const { nodes } = this.props;
    if (_.isEmpty(nodes)) {
      return (
        <div className="section">
          <h1>Loading cluster status...</h1>
        </div>
      );
    }

    const requestedNodeIDs = getNodeIDs(this.props.location.query.node_ids);
    let locality: RegExp = null;
    if (!_.isEmpty(this.props.location.query.locality)) {
      try {
        locality = new RegExp(this.props.location.query.locality);
      } catch (e) {
        // ignore there error
        locality = null;
      }
    }
    const lastUpdatedAt = _.maxBy(nodes, _ => "updated_at").updated_at;
    // Moments are not immutable.
    const staleBefore = LongToMoment(lastUpdatedAt).subtract(staleStoreCutoff);
    const deadBefore = LongToMoment(lastUpdatedAt).subtract(deadStoreCutoff);

    interface Identity {
      nodeID: number;
      address: string;
      locality: string;
      lastUpdatedAt?: moment.Moment;
    }
    // List of node identities.
    let ids: Identity[] = [];
    // Map of latencies by starting node id.
    const latenciesByNodeID: Map<number, { [k: string]: Long }> = new Map();
    // List of stale node identities and a map of their node ids.
    let staleIDs: Identity[] = [];
    const staleNodeIDs: Map<number, {}> = new Map();

    // Iterate through all nodes statues, filter them, look for good/stale/dead
    // nodes.
    _.forEach(nodes, (n) => {
      if (requestedNodeIDs.size > 0 && !requestedNodeIDs.has(n.desc.node_id)) {
        return;
      }
      const localityString = localityToString(n.desc.locality);
      if (!_.isNil(locality) && !locality.test(localityString)) {
        return;
      }
      const updatedAt = LongToMoment(n.updated_at);
      if (updatedAt.isBefore(deadBefore)) {
        // Dead nodes will be outright removed.
        return;
      }
      // Stale nodes should still be displayed, but their latencies will be
      // all set to X.
      ids.push({
        nodeID: n.desc.node_id,
        address: n.desc.address.address_field,
        locality: localityString,
      });
      if (updatedAt.isBefore(staleBefore)) {
        staleIDs.push({
          nodeID: n.desc.node_id,
          address: n.desc.address.address_field,
          locality: localityString,
          lastUpdatedAt: updatedAt,
        });
        staleNodeIDs.set(n.desc.node_id, {});
        return;
      }
      latenciesByNodeID.set(n.desc.node_id, n.latencies);
    });

    // Calculate the mean and sampled standard deviation.
    let latencyCount = 0;
    let latencySum = 0;
    let latencySquareSum = 0;
    _.forEach(ids, (id) => {
      _.forEach(latenciesByNodeID.get(id.nodeID), (latency, otherNodeIDString) => {
        const otherNodeID = Number(otherNodeIDString);
        // Convert from ns to ms.
        const ms = nsToMs(latency);
        // Don't include self latencies or latencies to/from stale nodes.
        if (id.nodeID !== otherNodeID &&
          !staleNodeIDs.has(id.nodeID) && !staleNodeIDs.has(otherNodeID)) {
          latencyCount = latencyCount + 1;
          latencySum = latencySum + ms;
          latencySquareSum = latencySquareSum + (ms * ms);
        }
      });
    });

    const mean = (latencyCount > 0) ? (latencySum / latencyCount) : 0;
    const stddev = (latencyCount > 1) ?
      Math.sqrt(latencySquareSum / (latencyCount - 1) - (mean * mean)) : 0;
    const stddevPlus1 = mean + stddev;
    const stddevPlus2 = stddevPlus1 + stddev;
    const stddevMinus1 = mean - stddev;
    const stddevMinus2 = stddevMinus1 - stddev;

    // Find all missing latencies between good nodes.
    interface NoConnection {
      from: Identity;
      to: Identity;
    }
    let noConnections: NoConnection[] = [];
    _.forEach(nodes, (nA) => {
      const nodeIDa = nA.desc.node_id;
      if (latenciesByNodeID.has(nodeIDa)) {
        const latencies = latenciesByNodeID.get(nodeIDa);
        _.forEach(nodes, (nB) => {
          const nodeIDb = nB.desc.node_id;
          if (nodeIDa !== nodeIDb && latenciesByNodeID.has(nodeIDb) && _.isNil(latencies[nodeIDb])) {
            noConnections.push({
              from: {
                nodeID: nA.desc.node_id,
                address: nA.desc.address.address_field,
                locality: localityToString(nA.desc.locality),
              },
              to: {
                nodeID: nB.desc.node_id,
                address: nB.desc.address.address_field,
                locality: localityToString(nB.desc.locality),
              },
            });
          }
        });
      }
    });

    ids = _.sortBy(ids, ["locality", "nodeID"]);
    staleIDs = _.sortBy(staleIDs, ["locality", "nodeID"]);
    noConnections = _.sortBy(noConnections, ["from.locality", "from.nodeID", "to.locality", "to.nodeID"]);

    // getLatencyCell creates and decorates a cell based on it's latency.
    function getLatencyCell(nodeIDa: number, nodeIDb: number) {
      if (nodeIDa === nodeIDb) {
        return <td className="network-table__cell network-table__cell--self" key={nodeIDb}>
          -
        </td>;
      }
      if (staleNodeIDs.has(nodeIDa) || staleNodeIDs.has(nodeIDb)) {
        return <td className="network-table__cell network-table__cell--no-connection" key={nodeIDb}>
          X
        </td>;
      }
      const a = latenciesByNodeID.get(nodeIDa);
      if (_.isNil(a[nodeIDb])) {
        return <td className="network-table__cell network-table__cell--no-connection" key={nodeIDb}>
          X
        </td>;
      }
      const latency = nsToMs(a[nodeIDb]);
      let heat: string;
      if (latency > stddevPlus2) {
        heat = "stddev-plus-2";
      } else if (latency > stddevPlus1) {
        heat = "stddev-plus-1";
      } else if (latency < stddevMinus2) {
        heat = "stddev-minus-2";
      } else if (latency < stddevMinus1) {
        heat = "stddev-minus-1";
      } else {
        heat = "stddev-even";
      }
      const className = `network-table__cell network-table__cell--${heat}`;
      const title = `n${nodeIDa} -> n${nodeIDb}\n${latency.toString()}ms`;
      return <td className={className} title={title} key={nodeIDb}>
        {latency.toFixed(2)}ms
      </td>;
    }

    // createHeaderCell creates and decorates a header cell.
    function createHeaderCell(id: Identity, key: number, left: boolean = false) {
      const node = `n${id.nodeID.toString()}`;
      const title = _.join([node, id.address, id.locality], "\n");
      let className = "network-table__cell network-table__cell--header";
      if (left) {
        className = className + " network-table__cell--header-left";
      }
      if (staleNodeIDs.has(id.nodeID)) {
        className = className + " network-table__cell--header-warning";
      }
      return <td key={key} className={className} title={title}>
        {node}
      </td>;
    }

    // staleTable is a table of all stale nodes.
    const staleTable = (staleNodeIDs.size === 0) ? null :
      <div>
        <h2>Stale Nodes</h2>
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
              _.map(staleIDs, (stale) => (
                <tr className="failure-table__row" key={stale.nodeID}>
                  <td className="failure-table__cell">
                    n{stale.nodeID}
                  </td>
                  <td className="failure-table__cell">
                    {stale.address}
                  </td>
                  <td className="failure-table__cell">
                    {stale.locality}
                  </td>
                  <td className="failure-table__cell">
                    {stale.lastUpdatedAt.toString()}
                  </td>
                </tr>
              ))
            }
          </tbody>
        </table>
      </div>;

    // latencyTable is the table and heat-map that's displayed for all nodes.
    const latencyTable = (_.isEmpty(ids)) ?
      <div>
        <h2>No nodes match the filters</h2>
      </div> :
      <div>
        <h2>Latencies</h2>
        <table className="network-table">
          <tbody>
            <tr key="0" className="network-table__row">
              <td key="0" className="network-table__cell network-table__cell--spacer" />
              {
                _.map(ids, (id) => createHeaderCell(id, id.nodeID))
              }
            </tr>
            {
              _.map(ids, (ida) => (
                <tr key={ida.nodeID} className="network-table__row">
                  {
                    createHeaderCell(ida, 0, true)
                  }
                  {
                    _.map(ids, (idb) => getLatencyCell(ida.nodeID, idb.nodeID))
                  }
                </tr>
              ))
            }
          </tbody>
        </table>
      </div>;

    // legend is just a quick table showing the standard deviation values.
    const legend = (_.isEmpty(ids)) ? null :
      <div>
        <h2>Legend</h2>
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
      </div>;

    // noConnectionTable is a list of all good nodes that seem to be missing
    // a connection.
    const noConnectionTable = (_.isEmpty(noConnections)) ? null :
      <div>
        <h2>No Connections</h2>
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
              _.map(noConnections, (noConn, k) => (
                <tr className="failure-table__row" key={k}>
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
      </div>;

    return (
      <div>
        <h1>Network Diagnostics</h1>
        <FilterList nodeIDs={requestedNodeIDs} localityRegex={locality} />
        {latencyTable}
        {legend}
        {staleTable}
        {noConnectionTable}
      </div>
    );
  }
}

export default connect(
  (state: AdminUIState) => {
    return {
      nodes: state.cachedData.nodes.data,
    };
  },
  {
    refreshNodes,
  },
)(Network);
