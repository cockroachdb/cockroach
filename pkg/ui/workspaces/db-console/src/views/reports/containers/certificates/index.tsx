// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Loading, util } from "@cockroachlabs/cluster-ui";
import isEmpty from "lodash/isEmpty";
import isNaN from "lodash/isNaN";
import join from "lodash/join";
import map from "lodash/map";
import sortBy from "lodash/sortBy";
import React, { Fragment, useEffect } from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import * as protos from "src/js/protos";
import {
  certificatesRequestKey,
  refreshCertificates,
} from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { nodeIDAttr } from "src/util/constants";
import { getMatchParamByName } from "src/util/query";
import { BackToAdvanceDebug } from "src/views/reports/containers/util";

interface CertificatesOwnProps {
  certificates: protos.cockroach.server.serverpb.CertificatesResponse;
  lastError: Error;
  refreshCertificates: typeof refreshCertificates;
}

const dateFormat = "Y-MM-DD HH:mm:ss";

type CertificatesProps = CertificatesOwnProps & RouteComponentProps;

const emptyRow = (
  <tr className="certs-table__row">
    <th className="certs-table__cell certs-table__cell--header" />
    <td className="certs-table__cell" />
  </tr>
);

function certificatesRequestFromProps(props: CertificatesProps) {
  return new protos.cockroach.server.serverpb.CertificatesRequest({
    node_id: getMatchParamByName(props.match, nodeIDAttr),
  });
}

function renderSimpleRow(header: string, value: string, title = "") {
  let realTitle = title;
  if (isEmpty(realTitle)) {
    realTitle = value;
  }
  return (
    <tr className="certs-table__row">
      <th className="certs-table__cell certs-table__cell--header">{header}</th>
      <td className="certs-table__cell" title={realTitle}>
        {value}
      </td>
    </tr>
  );
}

function renderMultilineRow(header: string, values: string[]) {
  return (
    <tr className="certs-table__row">
      <th className="certs-table__cell certs-table__cell--header">{header}</th>
      <td className="certs-table__cell" title={join(values, "\n")}>
        <ul className="certs-entries-list">
          {sortBy(values).map((value, key) => (
            <li key={key}>{value}</li>
          ))}
        </ul>
      </td>
    </tr>
  );
}

function renderTimestampRow(header: string, value: Long) {
  const timestamp = util.LongToMoment(value).format(dateFormat);
  const title = value + "\n" + timestamp;
  return renderSimpleRow(header, timestamp, title);
}

function renderFields(
  fields: protos.cockroach.server.serverpb.CertificateDetails.IFields,
  id: number,
) {
  return [
    renderSimpleRow("Cert ID", id.toString()),
    renderSimpleRow("Issuer", fields.issuer),
    renderSimpleRow("Subject", fields.subject),
    renderTimestampRow("Valid From", fields.valid_from),
    renderTimestampRow("Valid Until", fields.valid_until),
    renderMultilineRow("Addresses", fields.addresses),
    renderSimpleRow("Signature Algorithm", fields.signature_algorithm),
    renderSimpleRow("Public Key", fields.public_key),
    renderMultilineRow("Key Usage", fields.key_usage),
    renderMultilineRow("Extended Key Usage", fields.extended_key_usage),
  ];
}

function renderCert(
  cert: protos.cockroach.server.serverpb.ICertificateDetails,
  key: number,
) {
  let certType: string;
  switch (cert.type) {
    case protos.cockroach.server.serverpb.CertificateDetails.CertificateType.CA:
      certType = "Certificate Authority";
      break;
    case protos.cockroach.server.serverpb.CertificateDetails.CertificateType
      .NODE:
      certType = "Node Certificate";
      break;
    case protos.cockroach.server.serverpb.CertificateDetails.CertificateType
      .CLIENT_CA:
      certType = "Client Certificate Authority";
      break;
    case protos.cockroach.server.serverpb.CertificateDetails.CertificateType
      .CLIENT:
      certType = "Client Certificate";
      break;
    case protos.cockroach.server.serverpb.CertificateDetails.CertificateType
      .UI_CA:
      certType = "UI Certificate Authority";
      break;
    case protos.cockroach.server.serverpb.CertificateDetails.CertificateType.UI:
      certType = "UI Certificate";
      break;
    default:
      certType = "Unknown";
  }
  return (
    <table key={key} className="certs-table">
      <tbody>
        {renderSimpleRow("Type", certType)}
        {map(cert.fields, (fields, id) => {
          const result = renderFields(fields, id);
          if (id > 0) {
            result.unshift(emptyRow);
          }
          return result;
        })}
      </tbody>
    </table>
  );
}

/**
 * Renders the Certificate Report page.
 */
export function Certificates({
  certificates,
  lastError,
  refreshCertificates: refreshCertificatesAction,
  match,
  history,
  location,
}: CertificatesProps): React.ReactElement {
  useEffect(() => {
    refreshCertificatesAction(
      new protos.cockroach.server.serverpb.CertificatesRequest({
        node_id: getMatchParamByName(match, nodeIDAttr),
      }),
    );
  }, [refreshCertificatesAction, match, location.pathname, location.search]);

  const renderContent = () => {
    const nodeId = getMatchParamByName(match, nodeIDAttr);

    if (isEmpty(certificates.certificates)) {
      return (
        <h2 className="base-heading">
          No certificates were found on node {nodeId}.
        </h2>
      );
    }

    let header: string = null;
    if (isNaN(parseInt(nodeId, 10))) {
      header = "Local Node";
    } else {
      header = `Node ${nodeId}`;
    }

    return (
      <Fragment>
        <h2 className="base-heading">{header} certificates</h2>
        {map(certificates.certificates, (cert, key) => renderCert(cert, key))}
      </Fragment>
    );
  };

  return (
    <div className="section">
      <Helmet title="Certificates | Debug" />
      <BackToAdvanceDebug history={history} />
      <h1 className="base-heading">Certificates</h1>

      <section className="section">
        <Loading
          loading={!certificates}
          page={"certificates"}
          error={lastError}
          render={renderContent}
        />
      </section>
    </div>
  );
}

const mapStateToProps = (state: AdminUIState, props: CertificatesProps) => {
  const nodeIDKey = certificatesRequestKey(certificatesRequestFromProps(props));
  return {
    certificates:
      state.cachedData.certificates[nodeIDKey] &&
      state.cachedData.certificates[nodeIDKey].data,
    lastError:
      state.cachedData.certificates[nodeIDKey] &&
      state.cachedData.certificates[nodeIDKey].lastError,
  };
};

const mapDispatchToProps = {
  refreshCertificates,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(Certificates),
);
