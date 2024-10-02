// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { util, Loading } from "@cockroachlabs/cluster-ui";
import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { deviation as d3Deviation, mean as d3Mean } from "d3";
import capitalize from "lodash/capitalize";
import filter from "lodash/filter";
import flatMap from "lodash/flatMap";
import flow from "lodash/flow";
import isEmpty from "lodash/isEmpty";
import isUndefined from "lodash/isUndefined";
import map from "lodash/map";
import max from "lodash/max";
import sortBy from "lodash/sortBy";
import union from "lodash/union";
import values from "lodash/values";
import moment from "moment-timezone";
import { common } from "protobufjs";
import React, { Fragment } from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { withRouter, RouteComponentProps } from "react-router-dom";
import { createSelector } from "reselect";

import { InlineAlert } from "src/components";
import {
  CachedDataReducerState,
  refreshConnectivity,
  refreshLiveness,
  refreshNodes,
} from "src/redux/apiReducers";
import { connectivitySelector } from "src/redux/connectivity";
import {
  NodesSummary,
  nodesSummarySelector,
  selectLivenessRequestStatus,
  selectNodeRequestStatus,
} from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { trackFilter, trackCollapseNodes } from "src/util/analytics";
import { getDataFromServer } from "src/util/dataFromServer";
import { getMatchParamByName } from "src/util/query";
import {
  getFilters,
  localityToString,
  NodeFilterList,
  NodeFilterListProps,
} from "src/views/reports/components/nodeFilterList";

import { Latency } from "./latency";
import { Legend } from "./legend";
import Sort from "./sort";
import "./network.styl";

import NodeLivenessStatus = protos.cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;
import IConnectivity = cockroach.server.serverpb.NetworkConnectivityResponse.IConnectivity;
import IPeer = cockroach.server.serverpb.NetworkConnectivityResponse.IPeer;
import IDuration = common.IDuration;

interface NetworkOwnProps {
  nodesSummary: NodesSummary;
  nodeSummaryErrors: Error[];
  connectivity: CachedDataReducerState<cockroach.server.serverpb.NetworkConnectivityResponse>;
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
  refreshConnectivity: typeof refreshConnectivity;
}

export type Identity = {
  nodeID: number;
  address: string;
  locality?: string;
  updatedAt: moment.Moment;
  livenessStatus: protos.cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;
  connectivity: protos.cockroach.server.serverpb.NetworkConnectivityResponse.IConnectivity;
};

export interface NoConnection {
  from: Identity;
  to: Identity;
}

type NetworkProps = NetworkOwnProps & RouteComponentProps;

export interface NetworkFilter {
  [key: string]: Array<string>;
}

export interface NetworkSort {
  id: string;
  filters: Array<{ name: string; address: string }>;
}

interface INetworkState {
  collapsed: boolean;
  filter: NetworkFilter | null;
}

function contentAvailable(nodesSummary: NodesSummary) {
  return (
    !isUndefined(nodesSummary) &&
    !isEmpty(nodesSummary.nodeStatuses) &&
    !isEmpty(nodesSummary.nodeStatusByID) &&
    !isEmpty(nodesSummary.nodeIDs)
  );
}

export const isHealthyLivenessStatus = (
  status: NodeLivenessStatus,
): boolean => {
  switch (status) {
    case cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus
      .NODE_STATUS_LIVE:
    case cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus
      .NODE_STATUS_DRAINING:
    case cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus
      .NODE_STATUS_DECOMMISSIONING:
    case cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus
      .NODE_STATUS_UNKNOWN:
    case cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus
      .NODE_STATUS_UNAVAILABLE:
      return true;
    case cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus
      .NODE_STATUS_DEAD:
    case cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus
      .NODE_STATUS_DECOMMISSIONED:
    default:
      return false;
  }
};

export function getValueFromString(
  key: string,
  params: string,
  fullString?: boolean,
) {
  if (!params) {
    return;
  }
  const result = params.match(new RegExp(key + "=([^,#]*)"));
  if (!result) {
    return;
  }
  return fullString ? result[0] : result[1];
}

/**
 * Renders the Network Report page.
 */
export class Network extends React.Component<NetworkProps, INetworkState> {
  state: INetworkState = {
    collapsed: false,
    filter: null,
  };

  refresh() {
    this.props.refreshLiveness();
    this.props.refreshNodes();
    this.props.refreshConnectivity();
  }

  componentDidMount() {
    // Refresh nodes status query when mounting.
    this.refresh();
  }

  componentDidUpdate(_prevProps: NetworkProps) {
    this.refresh();
  }

  onChangeCollapse = (collapsed: boolean) => {
    trackCollapseNodes(collapsed);
    this.setState({ collapsed });
  };

  onChangeFilter = (key: string, value: string) => {
    const { filter } = this.state;
    const newFilter = filter ? filter : {};
    const data = newFilter[key] || [];
    const values =
      data.indexOf(value) === -1
        ? [...data, value]
        : data.length === 1
          ? null
          : data.filter((m: string | number) => m !== value);
    trackFilter(capitalize(key), value);
    this.setState({
      filter: {
        ...newFilter,
        [key]: values,
      },
    });
  };

  deselectFilterByKey = (key: string) => {
    const { filter } = this.state;
    const newFilter = filter ? filter : {};
    trackFilter(capitalize(key), "deselect all");
    this.setState({
      filter: {
        ...newFilter,
        [key]: null,
      },
    });
  };

  filteredDisplayIdentities = (displayIdentities: Identity[]) => {
    const { filter } = this.state;
    let data: Identity[] = [];
    let selectedIndex = 0;
    if (
      !filter ||
      Object.keys(filter).length === 0 ||
      Object.keys(filter).every(x => filter[x] === null)
    ) {
      return displayIdentities;
    }
    displayIdentities.forEach(identities => {
      Object.keys(filter).forEach((key, index) => {
        const value = getValueFromString(
          key,
          key === "cluster"
            ? `cluster=${identities.nodeID.toString()}`
            : identities.locality,
        );
        if (
          (!data.length || selectedIndex === index) &&
          filter[key] &&
          filter[key].indexOf(value) !== -1
        ) {
          data.push(identities);
          selectedIndex = index;
        } else if (filter[key]) {
          data = data.filter(
            identity =>
              filter[key].indexOf(
                getValueFromString(
                  key,
                  key === "cluster"
                    ? `cluster=${identity.nodeID.toString()}`
                    : identity.locality,
                ),
              ) !== -1,
          );
        }
      });
    });
    return data;
  };

  renderLatencyTable(latencies: number[], displayIdentities: Identity[]) {
    const { match } = this.props;
    const nodeId = getMatchParamByName(match, "node_id");
    const { collapsed, filter } = this.state;
    const mean = d3Mean(latencies);
    const sortParams = this.getSortParams(displayIdentities);
    let stddev = d3Deviation(latencies);
    if (isUndefined(stddev)) {
      stddev = 0;
    }
    // If there is no stddev, we should not display a legend. So there is no
    // need to set these values.
    const stddevPlus1 = stddev > 0 ? mean + stddev : 0;
    const stddevPlus2 = stddev > 0 ? stddevPlus1 + stddev : 0;
    const stddevMinus1 = stddev > 0 ? max([mean - stddev, 0]) : 0;
    const stddevMinus2 = stddev > 0 ? max([stddevMinus1 - stddev, 0]) : 0;
    const latencyTable = (
      <Latency
        displayIdentities={this.filteredDisplayIdentities(displayIdentities)}
        multipleHeader={nodeId !== "cluster"}
        node_id={nodeId}
        collapsed={collapsed}
        std={{
          stddev,
          stddevMinus2,
          stddevMinus1,
          stddevPlus1,
          stddevPlus2,
        }}
      />
    );

    if (stddev === 0) {
      return latencyTable;
    }

    // legend is just a quick table showing the standard deviation values.
    return (
      <Fragment>
        <Sort
          onChangeCollapse={this.onChangeCollapse}
          collapsed={collapsed}
          sort={sortParams}
          filter={filter}
          onChangeFilter={this.onChangeFilter}
          deselectFilterByKey={this.deselectFilterByKey}
        />
        <div className="section">
          <Legend
            stddevMinus2={stddevMinus2}
            stddevMinus1={stddevMinus1}
            mean={mean}
            stddevPlus1={stddevPlus1}
            stddevPlus2={stddevPlus2}
          />
          {latencyTable}
        </div>
      </Fragment>
    );
  }

  // getSortParams builds a list of sorting and filtering options based on provided list of nodes.
  // It is possible to filter data by: nodes ID, region, or availability zone. For instance this
  // function returns can return NetworkSort instance { id: "region", filters: [{name: "us-west1", ...}, {name: "us-west2"}]}
  // that later used to populate Sort and Filter dropdowns with available options.
  getSortParams = (data: Identity[]): NetworkSort[] => {
    const sort: NetworkSort[] = [];
    const searchQuery = (params: string) => `cluster,${params}`;
    data
      .filter(d => !!d.locality) // filter out dead nodes that don't have locality props
      .forEach(values => {
        const localities = searchQuery(values.locality).split(",");
        localities.forEach((locality: string) => {
          if (locality === "") return;
          const value = locality.match(/^\w+/gi)
            ? locality.match(/^\w+/gi)[0]
            : null;
          if (sort.some(x => x.id === value)) return;
          const sortValue: NetworkSort = { id: value, filters: [] };
          data
            .filter(d => !!d.locality) // filter out dead nodes that don't have locality props
            .forEach(item => {
              const valueLocality = searchQuery(values.locality).split(",");
              const itemLocality = searchQuery(item.locality);
              valueLocality.forEach(val => {
                const address = item.address ?? "";
                const itemLocalitySplited = val.match(/^\w+/gi)
                  ? val.match(/^\w+/gi)[0]
                  : null;
                if (val === "cluster" && value === "cluster") {
                  sortValue.filters = [
                    ...sortValue.filters,
                    {
                      name: item.nodeID.toString(),
                      address: address,
                    },
                  ];
                } else if (
                  itemLocalitySplited === value &&
                  !sortValue.filters.some(
                    f => f.name === getValueFromString(value, itemLocality),
                  )
                ) {
                  sortValue.filters = [
                    ...sortValue.filters,
                    {
                      name: getValueFromString(value, itemLocality),
                      address: address,
                    },
                  ];
                }
              });
            });
          sort.push(sortValue);
        });
      });
    return sort;
  };

  getDisplayIdentities = (identityByID: Map<number, Identity>): Identity[] => {
    const { match } = this.props;
    const nodeId = getMatchParamByName(match, "node_id");
    const identityContent = sortBy(
      Array.from(identityByID.values()),
      identity => identity.nodeID,
    );
    const sort = this.getSortParams(identityContent);
    if (sort.some(x => x.id === nodeId)) {
      return sortBy(identityContent, identity =>
        getValueFromString(nodeId, identity.locality, true),
      );
    }
    return identityContent;
  };

  renderContent(
    nodesSummary: NodesSummary,
    filters: NodeFilterListProps,
    connections: protos.cockroach.server.serverpb.NetworkConnectivityResponse["connections"],
  ) {
    if (!contentAvailable(nodesSummary)) {
      return null;
    }
    // Following states can be observed:
    // 1. live connection
    // 2. partitioned connection

    // Combine Node Ids known by gossip client (from `connectivity`) and from
    // Nodes api to make sure we show all known nodes.
    const knownNodeIds = union(
      Object.keys(nodesSummary.livenessByNodeID),
      Object.keys(connections),
    );

    // List of node identities.
    const identityByID: Map<number, Identity> = new Map();

    knownNodeIds.forEach(nodeId => {
      const nodeIdInt = parseInt(nodeId);
      const status = nodesSummary.nodeStatusByID[nodeId];
      identityByID.set(nodeIdInt, {
        nodeID: nodeIdInt,
        address: status?.desc.address.address_field,
        locality: status && localityToString(status.desc.locality),
        updatedAt: status && util.LongToMoment(status.updated_at),
        livenessStatus:
          nodesSummary.livenessStatusByNodeID[nodeId] ||
          protos.cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus
            .NODE_STATUS_UNKNOWN,
        connectivity: connections[nodeId],
      });
    });

    // apply filters to exclude items that don't satisfy filter conditions.
    identityByID.forEach((identity, nodeId) => {
      if (
        (filters.nodeIDs?.size > 0 && !filters.nodeIDs?.has(nodeId)) ||
        (!!filters.localityRegex &&
          filters.localityRegex?.test(identity.locality)) ||
        !isHealthyLivenessStatus(identity.livenessStatus)
      ) {
        identityByID.delete(nodeId);
      }
    });

    const displayIdentities: Identity[] =
      this.getDisplayIdentities(identityByID);

    const latencies = flow([
      values,
      (vals: IConnectivity[]) => flatMap(vals, v => Object.values(v.peers)),
      (vals: IPeer[]) => flatMap(vals, v => v.latency),
      (vals: IDuration[]) =>
        filter(vals, v => v !== undefined && v.nanos !== undefined),
      (vals: IDuration[]) => map(vals, v => util.NanoToMilli(v.nanos)),
    ])(connections);

    if (isEmpty(identityByID)) {
      return <h2 className="base-heading">No nodes match the filters</h2>;
    }
    if (knownNodeIds.length < 2) {
      return (
        <h2 className="base-heading">
          Cannot show latency chart for cluster with less than 2 nodes.
        </h2>
      );
    }
    return this.renderLatencyTable(latencies, displayIdentities);
  }

  render() {
    const { nodesSummary, location, connectivity } = this.props;
    const filters = getFilters(location);
    const canShowPage =
      !getDataFromServer().FeatureFlags.disable_kv_level_advanced_debug;

    return (
      <Fragment>
        <Helmet title="Network" />
        <h3 className="base-heading">Network</h3>
        {!canShowPage && (
          <section className="section">
            <InlineAlert
              title="The network page is only available via the system interface."
              intent="warning"
            />
          </section>
        )}
        {canShowPage && (
          <Loading
            loading={
              !contentAvailable(nodesSummary) ||
              !connectivity?.data?.connections
            }
            page={"network"}
            error={this.props.nodeSummaryErrors}
            className="loading-image loading-image__spinner-left loading-image__spinner-left__padded"
            render={() => (
              <div>
                <NodeFilterList
                  nodeIDs={filters.nodeIDs}
                  localityRegex={filters.localityRegex}
                />
                {this.renderContent(
                  nodesSummary,
                  filters,
                  connectivity?.data?.connections,
                )}
              </div>
            )}
          />
        )}
      </Fragment>
    );
  }
}

const nodeSummaryErrors = createSelector(
  selectNodeRequestStatus,
  selectLivenessRequestStatus,
  (nodes, liveness) => [nodes.lastError, liveness.lastError],
);

const mapStateToProps = (state: AdminUIState) => ({
  nodesSummary: nodesSummarySelector(state),
  nodeSummaryErrors: nodeSummaryErrors(state),
  connectivity: connectivitySelector(state),
});

const mapDispatchToProps = {
  refreshNodes,
  refreshLiveness,
  refreshConnectivity,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(Network),
);
