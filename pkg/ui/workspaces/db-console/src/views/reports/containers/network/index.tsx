// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _, { capitalize } from "lodash";
import moment from "moment";
import React, { Fragment, useEffect, useState } from "react";
import { Helmet } from "react-helmet";
import { useDispatch, useSelector } from "react-redux";
import { useParams, useLocation } from "react-router-dom";

import { refreshLiveness, refreshNodes } from "src/redux/apiReducers";
import { nodesSummarySelector } from "src/redux/nodes";
import { trackFilter, trackCollapseNodes } from "src/util/analytics";
import {
  getFilters,
  localityToString,
  NodeFilterList,
} from "src/views/reports/components/nodeFilterList";
import { Loading } from "@cockroachlabs/cluster-ui";
import { Latency } from "./latency";
import { Legend } from "./legend";
import Sort from "./sort";
import { getParam } from "src/util/query";
import "./network.styl";
import {
  nodesSummaryHealthyIDs,
  nodesSummaryLatencies,
  nodesSummaryStaleIDs,
  selectIdentityArray,
  //selectValidLatencies,
  selectNoConnectionNodes,
  selectNodeSummaryErrors,
} from "./selectors";
import { createStdDev, getMean } from "./utils";

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
export const Network = () => {
  const [state, setState] = useState<INetworkState>({
    collapsed: false,
    filter: null,
  });
  const dispatch = useDispatch();
  const params = useParams();
  const location = useLocation();
  const filters = getFilters(location);
  const nodesSummary = useSelector(nodesSummarySelector);
  const nodeSummaryErrors = useSelector(selectNodeSummaryErrors);
  const identityArray = useSelector(selectIdentityArray);
  const noConnections = useSelector(selectNoConnectionNodes);
  // replace with selectValidLatencies
  const validLatencies = useSelector(nodesSummaryLatencies);
  // replace with selectLiveNodeIDs
  const healthyIDs = useSelector(nodesSummaryHealthyIDs);
  // replace with selectNonLiveNodeIDs
  const staleIDs = useSelector(nodesSummaryStaleIDs);

  useEffect(() => {
    dispatch(refreshLiveness());
    dispatch(refreshNodes());
  }, [dispatch, location]);

  const contentAvailable = () => {
    return (
      !_.isUndefined(nodesSummary) &&
      !_.isEmpty(nodesSummary.nodeStatuses) &&
      !_.isEmpty(nodesSummary.nodeStatusByID) &&
      !_.isEmpty(nodesSummary.nodeIDs)
    );
  };

  const onChangeCollapse = (collapsed: boolean) => {
    trackCollapseNodes(collapsed);
    setState({ collapsed, ...state });
  };

  const onChangeFilter = (key: string, value: string) => {
    const { filter } = state;
    const newFilter = filter ? filter : {};
    const data = newFilter[key] || [];
    const values =
      data.indexOf(value) === -1
        ? [...data, value]
        : data.length === 1
        ? null
        : data.filter((m: string | number) => m !== value);
    trackFilter(capitalize(key), value);
    setState({
      filter: {
        ...newFilter,
        [key]: values,
      },
      ...state,
    });
  };

  const deselectFilterByKey = (key: string) => {
    const { filter } = state;
    const newFilter = filter ? filter : {};
    trackFilter(capitalize(key), "deselect all");
    setState({
      filter: {
        ...newFilter,
        [key]: null,
      },
      ...state,
    });
  };

  const filteredDisplayIdentities = (displayIdentities: Identity[]) => {
    const { filter } = state;
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

  const renderLatencyTable = (sortedIdentities: Identity[]) => {
    const { collapsed, filter } = state;
    const nodeId = getParam(params, "node_id");
    const sortParams = getSortParams(sortedIdentities);
    const stdDev = createStdDev(validLatencies);
    const latencyTable = (
      <Latency
        displayIdentities={filteredDisplayIdentities(sortedIdentities)}
        staleIDs={new Set(staleIDs.value())}
        multipleHeader={nodeId !== "cluster"}
        node_id={nodeId}
        collapsed={collapsed}
        nodesSummary={nodesSummary}
        std={stdDev}
      />
    );

    if (stdDev.stddev === 0) {
      return latencyTable;
    }

    // legend is just a quick table showing the standard deviation values.
    return [
      <Sort
        onChangeCollapse={onChangeCollapse}
        collapsed={collapsed}
        sort={sortParams}
        filter={filter}
        onChangeFilter={onChangeFilter}
        deselectFilterByKey={deselectFilterByKey}
      />,
      <div className="section">
        <Legend
          stddevMinus2={stdDev.stddevMinus2}
          stddevMinus1={stdDev.stddevMinus1}
          mean={getMean(validLatencies)}
          stddevPlus1={stdDev.stddevPlus1}
          stddevPlus2={stdDev.stddevPlus2}
          noConnections={noConnections}
        />
        {latencyTable}
      </div>,
    ];
  };

  const getSortParams = (data: Identity[]) => {
    const sort: NetworkSort[] = [];
    const searchQuery = (params: string) => `cluster,${params}`;
    data.forEach(values => {
      const localities = searchQuery(values.locality).split(",");
      localities.forEach((locality: string) => {
        if (locality !== "") {
          const value = locality.match(/^\w+/gi)
            ? locality.match(/^\w+/gi)[0]
            : null;
          if (!sort.some(x => x.id === value)) {
            const sortValue: NetworkSort = { id: value, filters: [] };
            data.forEach(item => {
              const valueLocality = searchQuery(values.locality).split(",");
              const itemLocality = searchQuery(item.locality);
              valueLocality.forEach(val => {
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

  // Applies user selected sort.
  const sortDisplayIdentities = () => {
    const nodeId = getParam(params, "node_id");
    const sortedIdentityArray = _.chain(identityArray).sortBy(
      identity => identity.nodeID,
    );
    const sort = getSortParams(sortedIdentityArray.value());
    if (sort.some(x => x.id === nodeId)) {
      return sortedIdentityArray
        .sortBy(identity => getValueFromString(nodeId, identity.locality, true))
        .value();
    }
    return sortedIdentityArray.value();
  };

  const renderContent = () => {
    if (!contentAvailable()) {
      return null;
    }

    let healthyIDsContext = { ...healthyIDs };
    let staleIDsContext = { ...staleIDs };
    if (!_.isNil(filters.nodeIDs) && filters.nodeIDs.size > 0) {
      healthyIDsContext = healthyIDsContext.filter(nodeID =>
        filters.nodeIDs.has(nodeID),
      );
      staleIDsContext = staleIDsContext.filter(nodeID =>
        filters.nodeIDs.has(nodeID),
      );
    }
    if (!_.isNil(filters.localityRegex)) {
      healthyIDsContext = healthyIDsContext.filter(nodeID =>
        filters.localityRegex.test(
          localityToString(nodesSummary.nodeStatusByID[nodeID].desc.locality),
        ),
      );
      staleIDsContext = staleIDsContext.filter(nodeID =>
        filters.localityRegex.test(
          localityToString(nodesSummary.nodeStatusByID[nodeID].desc.locality),
        ),
      );
    }
    const sortedIdentities: Identity[] = sortDisplayIdentities();

    let content: JSX.Element | JSX.Element[];
    if (_.isEmpty(healthyIDs)) {
      content = (
        <h2 className="base-heading">No healthy nodes match the filters</h2>
      );
    } else if (validLatencies.length < 1) {
      content = (
        <h2 className="base-heading">
          Cannot show latency chart without two healthy nodes.
        </h2>
      );
    } else {
      content = renderLatencyTable(sortedIdentities);
    }
    return [content];
  };

  return (
    <Fragment>
      <Helmet title="Network Diagnostics | Debug" />
      <h3 className="base-heading">Network Diagnostics</h3>
      <Loading
        loading={!contentAvailable()}
        page={"network"}
        error={nodeSummaryErrors}
        className="loading-image loading-image__spinner-left loading-image__spinner-left__padded"
        render={() => (
          <div>
            <NodeFilterList
              nodeIDs={filters.nodeIDs}
              localityRegex={filters.localityRegex}
            />
            {renderContent()}
          </div>
        )}
      />
    </Fragment>
  );
};

export default Network;
