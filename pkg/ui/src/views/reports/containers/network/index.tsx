// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { deviation as d3Deviation, mean as d3Mean } from "d3";
import _, { capitalize } from "lodash";
import moment from "moment";
import React, { Fragment } from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { createSelector } from "reselect";
import { withRouter, RouteComponentProps } from "react-router-dom";

import { refreshLiveness, refreshNodes } from "src/redux/apiReducers";
import {
  LivenessStatus,
  NodesSummary,
  nodesSummarySelector,
  selectLivenessRequestStatus,
  selectNodeRequestStatus,
} from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { LongToMoment, NanoToMilli } from "src/util/convert";
import { FixLong } from "src/util/fixLong";
import { trackFilter, trackCollapseNodes } from "src/util/analytics";
import {
  getFilters,
  localityToString,
  NodeFilterList,
  NodeFilterListProps,
} from "src/views/reports/components/nodeFilterList";
import { Loading } from "@cockroachlabs/cluster-ui";
import { Latency } from "./latency";
import { Legend } from "./legend";
import Sort from "./sort";
import { getMatchParamByName } from "src/util/query";
import "./network.styl";

interface NetworkOwnProps {
  nodesSummary: NodesSummary;
  nodeSummaryErrors: Error[];
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
}

export interface Identity {
  nodeID: number;
  address: string;
  locality?: string;
  updatedAt: moment.Moment;
}

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
    !_.isUndefined(nodesSummary) &&
    !_.isEmpty(nodesSummary.nodeStatuses) &&
    !_.isEmpty(nodesSummary.nodeStatusByID) &&
    !_.isEmpty(nodesSummary.nodeIDs)
  );
}

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
 * Renders the Network Diagnostics Report page.
 */
export class Network extends React.Component<NetworkProps, INetworkState> {
  state: INetworkState = {
    collapsed: false,
    filter: null,
  };

  refresh(props = this.props) {
    props.refreshLiveness();
    props.refreshNodes();
  }

  componentDidMount() {
    // Refresh nodes status query when mounting.
    this.refresh();
  }

  componentDidUpdate(prevProps: NetworkProps) {
    if (!_.isEqual(this.props.location, prevProps.location)) {
      this.refresh(this.props);
    }
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
      Object.keys(filter).every((x) => filter[x] === null)
    ) {
      return displayIdentities;
    }
    displayIdentities.forEach((identities) => {
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
            (identity) =>
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

  renderLatencyTable(
    latencies: number[],
    staleIDs: Set<number>,
    nodesSummary: NodesSummary,
    displayIdentities: Identity[],
    noConnections: NoConnection[],
  ) {
    const { match } = this.props;
    const nodeId = getMatchParamByName(match, "node_id");
    const { collapsed, filter } = this.state;
    const mean = d3Mean(latencies);
    const sortParams = this.getSortParams(displayIdentities);
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
    const latencyTable = (
      <Latency
        displayIdentities={this.filteredDisplayIdentities(displayIdentities)}
        staleIDs={staleIDs}
        multipleHeader={nodeId !== "cluster"}
        node_id={nodeId}
        collapsed={collapsed}
        nodesSummary={nodesSummary}
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
    return [
      <Sort
        onChangeCollapse={this.onChangeCollapse}
        collapsed={collapsed}
        sort={sortParams}
        filter={filter}
        onChangeFilter={this.onChangeFilter}
        deselectFilterByKey={this.deselectFilterByKey}
      />,
      <div className="section">
        <Legend
          stddevMinus2={stddevMinus2}
          stddevMinus1={stddevMinus1}
          mean={mean}
          stddevPlus1={stddevPlus1}
          stddevPlus2={stddevPlus2}
          noConnections={noConnections}
        />
        {latencyTable}
      </div>,
    ];
  }

  getSortParams = (data: Identity[]) => {
    const sort: NetworkSort[] = [];
    const searchQuery = (params: string) => `cluster,${params}`;
    data.forEach((values) => {
      const localities = searchQuery(values.locality).split(",");
      localities.forEach((locality: string) => {
        if (locality !== "") {
          const value = locality.match(/^\w+/gi)
            ? locality.match(/^\w+/gi)[0]
            : null;
          if (!sort.some((x) => x.id === value)) {
            const sortValue: NetworkSort = { id: value, filters: [] };
            data.forEach((item) => {
              const valueLocality = searchQuery(values.locality).split(",");
              const itemLocality = searchQuery(item.locality);
              valueLocality.forEach((val) => {
                const itemLocalitySplited = val.match(/^\w+/gi)
                  ? val.match(/^\w+/gi)[0]
                  : null;
                if (val === "cluster" && value === "cluster") {
                  sortValue.filters = [
                    ...sortValue.filters,
                    {
                      name: item.nodeID.toString(),
                      address: item.address,
                    },
                  ];
                } else if (
                  itemLocalitySplited === value &&
                  !sortValue.filters.reduce(
                    (accumulator, vendor) =>
                      accumulator ||
                      vendor.name === getValueFromString(value, itemLocality),
                    false,
                  )
                ) {
                  sortValue.filters = [
                    ...sortValue.filters,
                    {
                      name: getValueFromString(value, itemLocality),
                      address: item.address,
                    },
                  ];
                }
              });
            });
            sort.push(sortValue);
          }
        }
      });
    });
    return sort;
  };

  getDisplayIdentities = (
    healthyIDsContext: _.CollectionChain<number>,
    staleIDsContext: _.CollectionChain<number>,
    identityByID: Map<number, Identity>,
  ) => {
    const { match } = this.props;
    const nodeId = getMatchParamByName(match, "node_id");
    const identityContent = healthyIDsContext
      .union(staleIDsContext.value())
      .map((nodeID) => identityByID.get(nodeID))
      .sortBy((identity) => identity.nodeID);
    const sort = this.getSortParams(identityContent.value());
    if (sort.some((x) => x.id === nodeId)) {
      return identityContent
        .sortBy((identity) =>
          getValueFromString(nodeId, identity.locality, true),
        )
        .value();
    }
    return identityContent.value();
  };

  renderContent(nodesSummary: NodesSummary, filters: NodeFilterListProps) {
    if (!contentAvailable(nodesSummary)) {
      return null;
    }
    // List of node identities.
    const identityByID: Map<number, Identity> = new Map();
    _.forEach(nodesSummary.nodeStatuses, (status) => {
      identityByID.set(status.desc.node_id, {
        nodeID: status.desc.node_id,
        address: status.desc.address.address_field,
        locality: localityToString(status.desc.locality),
        updatedAt: LongToMoment(status.updated_at),
      });
    });

    // Calculate the mean and sampled standard deviation.
    let healthyIDsContext = _.chain(nodesSummary.nodeIDs)
      .filter(
        (nodeID) =>
          nodesSummary.livenessStatusByNodeID[nodeID] ===
          LivenessStatus.NODE_STATUS_LIVE,
      )
      .filter(
        (nodeID) => !_.isNil(nodesSummary.nodeStatusByID[nodeID].activity),
      )
      .map((nodeID) => Number.parseInt(nodeID, 0));
    let staleIDsContext = _.chain(nodesSummary.nodeIDs)
      .filter(
        (nodeID) =>
          nodesSummary.livenessStatusByNodeID[nodeID] ===
          LivenessStatus.NODE_STATUS_UNAVAILABLE,
      )
      .map((nodeID) => Number.parseInt(nodeID, 0));
    if (!_.isNil(filters.nodeIDs) && filters.nodeIDs.size > 0) {
      healthyIDsContext = healthyIDsContext.filter((nodeID) =>
        filters.nodeIDs.has(nodeID),
      );
      staleIDsContext = staleIDsContext.filter((nodeID) =>
        filters.nodeIDs.has(nodeID),
      );
    }
    if (!_.isNil(filters.localityRegex)) {
      healthyIDsContext = healthyIDsContext.filter((nodeID) =>
        filters.localityRegex.test(
          localityToString(nodesSummary.nodeStatusByID[nodeID].desc.locality),
        ),
      );
      staleIDsContext = staleIDsContext.filter((nodeID) =>
        filters.localityRegex.test(
          localityToString(nodesSummary.nodeStatusByID[nodeID].desc.locality),
        ),
      );
    }
    const healthyIDs = healthyIDsContext.value();
    const staleIDs = new Set(staleIDsContext.value());
    const displayIdentities: Identity[] = this.getDisplayIdentities(
      healthyIDsContext,
      staleIDsContext,
      identityByID,
    );
    const latencies = _.flatMap(healthyIDs, (nodeIDa) =>
      _.chain(healthyIDs)
        .without(nodeIDa)
        .map(
          (nodeIDb) => nodesSummary.nodeStatusByID[nodeIDa].activity[nodeIDb],
        )
        .filter((activity) => !_.isNil(activity) && !_.isNil(activity.latency))
        .map((activity) => NanoToMilli(FixLong(activity.latency).toNumber()))
        .filter((ms) => _.isFinite(ms) && ms > 0)
        .value(),
    );

    const noConnections: NoConnection[] = _.flatMap(healthyIDs, (nodeIDa) =>
      _.chain(nodesSummary.nodeStatusByID[nodeIDa].activity)
        .keys()
        .map((nodeIDb) => Number.parseInt(nodeIDb, 10))
        .difference(healthyIDs)
        .map((nodeIDb) => ({
          from: identityByID.get(nodeIDa),
          to: identityByID.get(nodeIDb),
        }))
        .sortBy((noConnection) => noConnection.to.nodeID)
        .sortBy((noConnection) => noConnection.to.locality)
        .sortBy((noConnection) => noConnection.from.nodeID)
        .sortBy((noConnection) => noConnection.from.locality)
        .value(),
    );

    let content: JSX.Element | JSX.Element[];
    if (_.isEmpty(healthyIDs)) {
      content = (
        <h2 className="base-heading">No healthy nodes match the filters</h2>
      );
    } else if (latencies.length < 1) {
      content = (
        <h2 className="base-heading">
          Cannot show latency chart without two healthy nodes.
        </h2>
      );
    } else {
      content = this.renderLatencyTable(
        latencies,
        staleIDs,
        nodesSummary,
        displayIdentities,
        noConnections,
      );
    }
    return [
      content,
      // staleTable(staleIdentities),
      // noConnectionTable(noConnections),
    ];
  }

  render() {
    const { nodesSummary, location } = this.props;
    const filters = getFilters(location);
    return (
      <Fragment>
        <Helmet title="Network Diagnostics | Debug" />
        <div className="section">
          <h1 className="base-heading">Network Diagnostics</h1>
        </div>
        <Loading
          loading={!contentAvailable(nodesSummary)}
          error={this.props.nodeSummaryErrors}
          className="loading-image loading-image__spinner-left loading-image__spinner-left__padded"
          render={() => (
            <div>
              <NodeFilterList
                nodeIDs={filters.nodeIDs}
                localityRegex={filters.localityRegex}
              />
              {this.renderContent(nodesSummary, filters)}
            </div>
          )}
        />
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
});

const mapDispatchToProps = {
  refreshNodes,
  refreshLiveness,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(Network),
);
