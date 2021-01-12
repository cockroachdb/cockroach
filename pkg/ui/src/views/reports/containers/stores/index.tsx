// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";

import * as protos from "src/js/protos";
import { storesRequestKey, refreshStores } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { nodeIDAttr } from "src/util/constants";
import EncryptionStatus from "src/views/reports/containers/stores/encryption";
import { Loading } from "@cockroachlabs/cluster-ui";
import { getMatchParamByName } from "src/util/query";

interface StoresOwnProps {
  stores: protos.cockroach.server.serverpb.IStoreDetails[];
  loading: boolean;
  lastError: Error;
  refreshStores: typeof refreshStores;
}

type StoresProps = StoresOwnProps & RouteComponentProps;

function storesRequestFromProps(props: StoresProps) {
  const nodeId = getMatchParamByName(props.match, nodeIDAttr);
  return new protos.cockroach.server.serverpb.StoresRequest({
    node_id: nodeId,
  });
}

/**
 * Renders the Stores Report page.
 */
export class Stores extends React.Component<StoresProps, {}> {
  refresh(props = this.props) {
    props.refreshStores(storesRequestFromProps(props));
  }

  componentDidMount() {
    // Refresh nodes status query when mounting.
    this.refresh();
  }

  componentDidUpdate(prevProps: StoresProps) {
    if (!_.isEqual(this.props.location, prevProps.location)) {
      this.refresh(this.props);
    }
  }

  renderSimpleRow(header: string, value: string, title: string = "") {
    let realTitle = title;
    if (_.isEmpty(realTitle)) {
      realTitle = value;
    }
    return (
      <tr className="stores-table__row">
        <th className="stores-table__cell stores-table__cell--header">
          {header}
        </th>
        <td className="stores-table__cell" title={realTitle}>
          {value}
        </td>
      </tr>
    );
  }

  renderStore = (store: protos.cockroach.server.serverpb.IStoreDetails) => {
    return (
      <table key={store.store_id} className="stores-table">
        <tbody>
          {this.renderSimpleRow("Store ID", store.store_id.toString())}
          {new EncryptionStatus({ store: store }).getEncryptionRows()}
        </tbody>
      </table>
    );
  };

  renderContent = () => {
    const { stores, match } = this.props;

    const nodeID = getMatchParamByName(match, nodeIDAttr);
    if (_.isEmpty(stores)) {
      return (
        <h2 className="base-heading">No stores were found on node {nodeID}.</h2>
      );
    }

    return _.map(this.props.stores, this.renderStore);
  };

  render() {
    const nodeID = getMatchParamByName(this.props.match, nodeIDAttr);
    let header: string = null;
    if (_.isNaN(parseInt(nodeID, 10))) {
      header = "Local Node";
    } else {
      header = `Node ${nodeID}`;
    }

    return (
      <div className="section">
        <Helmet title="Stores | Debug" />
        <h1 className="base-heading">Stores</h1>
        <h2 className="base-heading">{header} stores</h2>
        <Loading
          loading={this.props.loading}
          error={this.props.lastError}
          render={this.renderContent}
        />
      </div>
    );
  }
}

function selectStoresState(state: AdminUIState, props: StoresProps) {
  const nodeIDKey = storesRequestKey(storesRequestFromProps(props));
  return state.cachedData.stores[nodeIDKey];
}

const selectStoresLoading = createSelector(
  selectStoresState,
  (stores) => _.isEmpty(stores) || _.isEmpty(stores.data),
);

const selectSortedStores = createSelector(
  selectStoresLoading,
  selectStoresState,
  (loading, stores) => {
    if (loading) {
      return null;
    }
    return _.sortBy(stores.data.stores, (store) => store.store_id);
  },
);

const selectStoresLastError = createSelector(
  selectStoresLoading,
  selectStoresState,
  (loading, stores) => {
    if (loading) {
      return null;
    }
    return stores.lastError;
  },
);

const mapStateToProps = (state: AdminUIState, props: StoresProps) => ({
  stores: selectSortedStores(state, props),
  loading: selectStoresLoading(state, props),
  lastError: selectStoresLastError(state, props),
});

const mapDispatchToProps = {
  refreshStores,
};

export default withRouter(connect(mapStateToProps, mapDispatchToProps)(Stores));
