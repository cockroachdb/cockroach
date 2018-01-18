import _ from "lodash";
import React from "react";
import { connect } from "react-redux";
import { RouterState } from "react-router";

import * as protos from "src/js/protos";
import { certificatesRequestKey, refreshCertificates } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { nodeIDAttr } from "src/util/constants";
import { LongToMoment } from "src/util/convert";

interface CertificatesOwnProps {
  certificates: protos.cockroach.server.serverpb.CertificatesResponse;
  lastError: Error;
  refreshCertificates: typeof refreshCertificates;
}

const dateFormat = "Y-MM-DD HH:mm:ss";

type CertificatesProps = CertificatesOwnProps & RouterState;

const emptyRow = (
  <tr className="certs-table__row">
    <th className="certs-table__cell certs-table__cell--header" />
    <td className="certs-table__cell" />
  </tr>
);

function certificatesRequestFromProps(props: CertificatesProps) {
  return new protos.cockroach.server.serverpb.CertificatesRequest({
    node_id: props.params[nodeIDAttr],
  });
}

/**
 * Renders the Certificate Report page.
 */
class Certificates extends React.Component<CertificatesProps, {}> {
  refresh(props = this.props) {
    props.refreshCertificates(certificatesRequestFromProps(props));
  }

  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.refresh();
  }

  componentWillReceiveProps(nextProps: CertificatesProps) {
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
      <tr className="certs-table__row">
        <th className="certs-table__cell certs-table__cell--header">{header}</th>
        <td className="certs-table__cell" title={realTitle}>{value}</td>
      </tr>
    );
  }

  renderMultilineRow(header: string, values: string[]) {
    return (
      <tr className="certs-table__row">
        <th className="certs-table__cell certs-table__cell--header">{header}</th>
        <td className="certs-table__cell" title={_.join(values, "\n")}>
          <ul className="certs-entries-list">
            {
              _.chain(values)
                .sort()
                .map((value, key) => (
                  <li key={key}>
                    {value}
                  </li>
                ))
                .value()
            }
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

  renderFields(fields: protos.cockroach.server.serverpb.CertificateDetails.Fields$Properties, id: number) {
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

  renderCert(cert: protos.cockroach.server.serverpb.CertificateDetails$Properties, key: number) {
    let certType: string;
    switch (cert.type) {
      case protos.cockroach.server.serverpb.CertificateDetails.CertificateType.CA:
        certType = "Certificate Authority";
        break;
      case protos.cockroach.server.serverpb.CertificateDetails.CertificateType.NODE:
        certType = "Node";
        break;
      default:
        certType = "Unknown";
    }
    return (
      <table key={key} className="certs-table">
        <tbody>
          {this.renderSimpleRow("Type", certType)}
          {
            _.map(cert.fields, (fields, id) => {
              const result = this.renderFields(fields, id);
              if (id > 0) {
                result.unshift(emptyRow);
              }
              return result;
            })
          }
        </tbody>
      </table>
    );
  }

  render() {
    const nodeID = this.props.params[nodeIDAttr];
    if (!_.isNil(this.props.lastError)) {
      return (
        <div className="section">
          <h1>Certificates</h1>
          <h2>Error loading certificates for node {nodeID}</h2>
        </div>
      );
    }
    const { certificates } = this.props;
    if (_.isEmpty(certificates)) {
      return (
        <div className="section">
          <h1>Certificates</h1>
          <h2>Loading cluster status...</h2>
        </div>
      );
    }

    if (_.isEmpty(certificates.certificates)) {
      return (
        <div className="section">
          <h1>Certificates</h1>
          <h2>No certificates were found on node {this.props.params[nodeIDAttr]}.</h2>
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
        <h1>Certificates</h1>
        <h2>{header} certificates</h2>
        {
          _.map(certificates.certificates, (cert, key) => (
            this.renderCert(cert, key)
          ))
        }
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState, props: CertificatesProps) {
  const nodeIDKey = certificatesRequestKey(certificatesRequestFromProps(props));
  return {
    certificates: state.cachedData.certificates[nodeIDKey] && state.cachedData.certificates[nodeIDKey].data,
    lastError: state.cachedData.certificates[nodeIDKey] && state.cachedData.certificates[nodeIDKey].lastError,
  };
}

const actions = {
  refreshCertificates,
};

export default connect(mapStateToProps, actions)(Certificates);
