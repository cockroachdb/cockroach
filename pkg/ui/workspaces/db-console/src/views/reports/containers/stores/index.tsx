// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Loading, util } from "@cockroachlabs/cluster-ui";
import isEmpty from "lodash/isEmpty";
import map from "lodash/map";
import sortBy from "lodash/sortBy";
import React from "react";
import { Helmet } from "react-helmet";
import { RouteComponentProps, withRouter } from "react-router-dom";

import * as protos from "src/js/protos";
import { getStores } from "src/util/api";
import { nodeIDAttr } from "src/util/constants";
import { getMatchParamByName } from "src/util/query";
import EncryptionStatus from "src/views/reports/containers/stores/encryption";

import { BackToAdvanceDebug } from "../util";

function renderSimpleRow(header: string, value: string, title = "") {
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

function renderStore(store: protos.cockroach.server.serverpb.IStoreDetails) {
  return (
    <table key={store.store_id} className="stores-table">
      <tbody>
        {renderSimpleRow("Store ID", store.store_id.toString())}
        {new EncryptionStatus({ store: store }).getEncryptionRows()}
      </tbody>
    </table>
  );
}

/**
 * Renders the Stores Report page.
 */
export function Stores({
  match,
  history,
}: RouteComponentProps): React.ReactElement {
  const nodeID = getMatchParamByName(match, nodeIDAttr);

  const { data, error, isLoading } = util.useSwrWithClusterId(
    ["stores", nodeID],
    () =>
      getStores(
        new protos.cockroach.server.serverpb.StoresRequest({
          node_id: nodeID,
        }),
      ),
    { revalidateOnFocus: false },
  );

  const stores = data ? sortBy(data.stores, store => store.store_id) : null;

  const renderContent = () => {
    if (isEmpty(stores)) {
      return (
        <h2 className="base-heading">No stores were found on node {nodeID}.</h2>
      );
    }

    return <>{React.Children.toArray(map(stores, renderStore))}</>;
  };

  let header: string = null;
  if (isNaN(parseInt(nodeID, 10))) {
    header = "Local Node";
  } else {
    header = `Node ${nodeID}`;
  }

  return (
    <div className="section">
      <Helmet title="Stores | Debug" />
      <BackToAdvanceDebug history={history} />
      <h1 className="base-heading">Stores</h1>
      <h2 className="base-heading">{header} stores</h2>
      <Loading
        loading={isLoading}
        page={"containers stores"}
        error={error}
        render={renderContent}
      />
    </div>
  );
}

export default withRouter(Stores);
