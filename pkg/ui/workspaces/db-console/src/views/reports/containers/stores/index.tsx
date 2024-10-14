// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Loading } from "@cockroachlabs/cluster-ui";
import isEmpty from "lodash/isEmpty";
import isEqual from "lodash/isEqual";
import isNil from "lodash/isNil";
import map from "lodash/map";
import sortBy from "lodash/sortBy";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";

import * as protos from "src/js/protos";
import { storesRequestKey, refreshStores } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { nodeIDAttr } from "src/util/constants";
import { getMatchParamByName } from "src/util/query";
import EncryptionStatus from "src/views/reports/containers/stores/encryption";

import { BackToAdvanceDebug } from "../util";

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
    if (!isEqual(this.props.location, prevProps.location)) {
      this.refresh(this.props);
    }
  }

  renderSimpleRow(header: string, value: string, title = "") {
    let realTitle = title;
    if (isEmpty(realTitle)) {
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
    if (isEmpty(stores)) {
      return (
        <h2 className="base-heading">No stores were found on node {nodeID}.</h2>
      );
    }

    return (
      <>${React.Children.toArray(map(this.props.stores, this.renderStore))}</>
    );
  };

  render() {
    const nodeID = getMatchParamByName(this.props.match, nodeIDAttr);
    let header: string = null;
    if (isNaN(parseInt(nodeID, 10))) {
      header = "Local Node";
    } else {
      header = `Node ${nodeID}`;
    }

    return (
      <div className="section">
        <Helmet title="Stores | Debug" />
        <BackToAdvanceDebug history={this.props.history} />
        <h1 className="base-heading">Stores</h1>
        <h2 className="base-heading">{header} stores</h2>
        <Loading
          loading={this.props.loading}
          page={"containers stores"}
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

const selectStoresLoading = createSelector(selectStoresState, stores => {
  return isEmpty(stores) || (isEmpty(stores.data) && isNil(stores.lastError));
});

const selectSortedStores = createSelector(
  selectStoresLoading,
  selectStoresState,
  (loading, stores) => {
    if (loading) {
      return null;
    }
    return sortBy(stores.data?.stores, store => store.store_id);
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
