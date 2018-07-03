import _ from "lodash";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouterState } from "react-router";

import * as protos from "src/js/protos";
import { storesRequestKey, refreshStores } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { nodeIDAttr } from "src/util/constants";
import EncryptionStatus from "src/views/reports/containers/stores/encryption";

import "./stores.styl";

interface StoresOwnProps {
  stores: protos.cockroach.server.serverpb.StoresResponse;
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

  renderStore(store: protos.cockroach.server.serverpb.IStoreDetails, key: number) {
    return (
      <table key={key} className="stores-table">
        <tbody>
          { this.renderSimpleRow("Store ID", store.store_id.toString()) }
          <EncryptionStatus store={store} />
        </tbody>
      </table>
    );
  }

  render() {
    const nodeID = this.props.params[nodeIDAttr];
    if (!_.isNil(this.props.lastError)) {
      return (
        <div className="section">
          <Helmet>
            <title>Stores | Debug</title>
          </Helmet>
          <h1>Stores</h1>
          <h2>Error loading stores for node {nodeID}</h2>
        </div>
      );
    }
    const { stores } = this.props;
    if (_.isEmpty(stores)) {
      return (
        <div className="section">
          <Helmet>
            <title>Stores | Debug</title>
          </Helmet>
          <h1>Stores</h1>
          <h2>Loading cluster status...</h2>
        </div>
      );
    }

    if (_.isEmpty(stores.stores)) {
      return (
        <div className="section">
          <Helmet>
            <title>Stores | Debug</title>
          </Helmet>
          <h1>Stores</h1>
          <h2>No stores were found on node {this.props.params[nodeIDAttr]}.</h2>
        </div>
      );
    }

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
        {
          _.map(stores.stores, (store, key) => (
            this.renderStore(store, key)
          ))
        }
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState, props: StoresProps) {
  const nodeIDKey = storesRequestKey(storesRequestFromProps(props));
  return {
    stores: state.cachedData.stores[nodeIDKey] && state.cachedData.stores[nodeIDKey].data,
    lastError: state.cachedData.stores[nodeIDKey] && state.cachedData.stores[nodeIDKey].lastError,
  };
}

const actions = {
  refreshStores,
};

export default connect(mapStateToProps, actions)(Stores);
