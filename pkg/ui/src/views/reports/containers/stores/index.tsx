import _ from "lodash";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouterState } from "react-router";
import { createSelector } from "reselect";

import * as protos from "src/js/protos";
import { storesRequestKey, refreshStores } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { nodeIDAttr } from "src/util/constants";
import EncryptionStatus from "src/views/reports/containers/stores/encryption";
import Loading from "src/views/shared/components/loading";

import spinner from "assets/spinner.gif";

interface StoresOwnProps {
  stores: protos.cockroach.server.serverpb.IStoreDetails[];
  loading: boolean;
  lastError: Error;
  refreshStores: typeof refreshStores;
}

type StoresProps = StoresOwnProps & RouterState;

function storesRequestFromProps(props: StoresProps) {
  return new protos.cockroach.server.serverpb.StoresRequest({
    node_id: props.params[nodeIDAttr],
  });
}

/**
 * Renders the Stores Report page.
 */
class Stores extends React.Component<StoresProps, {}> {
  refresh(props = this.props) {
    props.refreshStores(storesRequestFromProps(props));
  }

  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.refresh();
  }

  componentWillReceiveProps(nextProps: StoresProps) {
    if (this.props.location !== nextProps.location) {
      this.refresh(nextProps);
    }
  }

  renderSimpleRow(header: string, value: string, title: string = "") {
    let realTitle = title;
    if (_.isEmpty(realTitle)) {
      realTitle = value;
    }
    return (
      <tr className="stores-table__row">
        <th className="stores-table__cell stores-table__cell--header">{header}</th>
        <td className="stores-table__cell" title={realTitle}>{value}</td>
      </tr>
    );
  }

  renderStore = (store: protos.cockroach.server.serverpb.IStoreDetails) => {
    return (
      <table key={store.store_id} className="stores-table">
        <tbody>
          { this.renderSimpleRow("Store ID", store.store_id.toString()) }
          { new EncryptionStatus({store: store}).getEncryptionRows() }
        </tbody>
      </table>
    );
  }

  renderContent() {
    const nodeID = this.props.params[nodeIDAttr];
    if (!_.isNil(this.props.lastError)) {
      return (
        <h2>Error loading stores for node {nodeID}</h2>
      );
    }

    const { stores } = this.props;
    if (_.isEmpty(stores)) {
      return (
        <h2>No stores were found on node {nodeID}.</h2>
      );
    }

    return (
      <React.Fragment>
        { _.map(this.props.stores,  this.renderStore) }
      </React.Fragment>
    );
  }

  render() {
    const nodeID = this.props.params[nodeIDAttr];
    let header: string = null;
    if (_.isNaN(parseInt(nodeID, 10))) {
      header = "Local Node";
    } else {
      header = `Node ${nodeID}`;
    }

    return (
      <div className="section">
        <Helmet>
          <title>Stores | Debug</title>
        </Helmet>
        <h1>Stores</h1>
        <h2>{header} stores</h2>
        <Loading
          loading={this.props.loading}
          className="loading-image loading-image__spinner"
          image={spinner}
        >
          {this.renderContent()}
        </Loading>
      </div>
    );
  }
}

function selectStoresState(state: AdminUIState, props: StoresProps) {
  const nodeIDKey = storesRequestKey(storesRequestFromProps(props));
  return state.cachedData.stores[nodeIDKey] && state.cachedData.stores[nodeIDKey];
}

const selectStoresLoading = createSelector(
  selectStoresState,
  (stores) => _.isEmpty(stores.data),
);

const selectSortedStores = createSelector(
  selectStoresState,
  (stores) => (
    _.sortBy(stores.data.stores, (store) => store.store_id)
  ),
);

const selectStoresLastError = createSelector(
  selectStoresState,
  (stores) => (
   stores.lastError
  ),
);

function mapStateToProps(state: AdminUIState, props: StoresProps) {
  return {
    stores: selectSortedStores(state, props),
    loading: selectStoresLoading(state, props),
    lastError: selectStoresLastError(state, props),
  };
}

const actions = {
  refreshStores,
};

export default connect(mapStateToProps, actions)(Stores);
