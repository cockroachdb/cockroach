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
import React, { Fragment } from "react";
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
import { LongToMoment } from "src/util/convert";
import { Loading } from "@cockroachlabs/cluster-ui";
import { getMatchParamByName } from "src/util/query";

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

/**
 * Renders the Certificate Report page.
 */
export class Certificates extends React.Component<CertificatesProps, {}> {
  refresh(props = this.props) {
    props.refreshCertificates(certificatesRequestFromProps(props));
  }

  componentDidMount() {
    // Refresh nodes status query when mounting.
    this.refresh();
  }

  componentDidUpdate(prevProps: CertificatesProps) {
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
      <tr className="certs-table__row">
        <th className="certs-table__cell certs-table__cell--header">
          {header}
        </th>
        <td className="certs-table__cell" title={realTitle}>
          {value}
        </td>
      </tr>
    );
  }

  renderMultilineRow(header: string, values: string[]) {
    return (
      <tr className="certs-table__row">
        <th className="certs-table__cell certs-table__cell--header">
          {header}
        </th>
        <td className="certs-table__cell" title={_.join(values, "\n")}>
          <ul className="certs-entries-list">
            {_.chain(values)
              .sort()
              .map((value, key) => <li key={key}>{value}</li>)
              .value()}
          </ul>
        </td>
      </tr>
    );
  }

  renderTimestampRow(header: string, value: Long) {
    const timestamp = LongToMoment(value).format(dateFormat);
    const title = value + "\n" + timestamp;
    return this.renderSimpleRow(header, timestamp, title);
  }

  renderFields(
    fields: protos.cockroach.server.serverpb.CertificateDetails.IFields,
    id: number,
  ) {
    return [
      this.renderSimpleRow("Cert ID", id.toString()),
      this.renderSimpleRow("Issuer", fields.issuer),
      this.renderSimpleRow("Subject", fields.subject),
      this.renderTimestampRow("Valid From", fields.valid_from),
      this.renderTimestampRow("Valid Until", fields.valid_until),
      this.renderMultilineRow("Addresses", fields.addresses),
      this.renderSimpleRow("Signature Algorithm", fields.signature_algorithm),
      this.renderSimpleRow("Public Key", fields.public_key),
      this.renderMultilineRow("Key Usage", fields.key_usage),
      this.renderMultilineRow("Extended Key Usage", fields.extended_key_usage),
    ];
  }

  renderCert(
    cert: protos.cockroach.server.serverpb.ICertificateDetails,
    key: number,
  ) {
    let certType: string;
    switch (cert.type) {
      case protos.cockroach.server.serverpb.CertificateDetails.CertificateType
        .CA:
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
      case protos.cockroach.server.serverpb.CertificateDetails.CertificateType
        .UI:
        certType = "UI Certificate";
        break;
      default:
        certType = "Unknown";
    }
    return (
      <table key={key} className="certs-table">
        <tbody>
          {this.renderSimpleRow("Type", certType)}
          {_.map(cert.fields, (fields, id) => {
            const result = this.renderFields(fields, id);
            if (id > 0) {
              result.unshift(emptyRow);
            }
            return result;
          })}
        </tbody>
      </table>
    );
  }

  renderContent = () => {
    const { certificates, match } = this.props;
    const nodeId = getMatchParamByName(match, nodeIDAttr);

    if (_.isEmpty(certificates.certificates)) {
      return (
        <h2 className="base-heading">
          No certificates were found on node {nodeId}.
        </h2>
      );
    }

    let header: string = null;
    if (_.isNaN(parseInt(nodeId, 10))) {
      header = "Local Node";
    } else {
      header = `Node ${nodeId}`;
    }

    return (
      <Fragment>
        <h2 className="base-heading">{header} certificates</h2>
        {_.map(certificates.certificates, (cert, key) =>
          this.renderCert(cert, key),
        )}
      </Fragment>
    );
  };

  render() {
    return (
      <div className="section">
        <Helmet title="Certificates | Debug" />
        <h1 className="base-heading">Certificates</h1>

        <section className="section">
          <Loading
            loading={!this.props.certificates}
            error={this.props.lastError}
            render={this.renderContent}
          />
        </section>
      </div>
    );
  }
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
