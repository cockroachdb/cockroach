import _ from "lodash";
import React from "react";
import { connect } from "react-redux";
import { RouterState } from "react-router";

import * as protos from "src/js/protos";
import { refreshCertificates } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { nodeIDAttr } from "src/util/constants";
import { LongToMoment } from "src/util/convert";

interface CertificatesOwnProps {
  certificates: protos.cockroach.server.serverpb.CertificatesResponse;
  refreshCertificates: typeof refreshCertificates;
}

const dateFormat = "Y-MM-DD HH:mm:ss";

type CertificatesProps = CertificatesOwnProps & RouterState;

const loading = (
    <div className="section">
    <h1>Loading cluster status...</h1>
  </div>
);

const emptyRow = (
  <tr className="certs-table__row">
    <th className="certs-table__cell certs-table__cell--header"/>
    <td className="certs-table__cell"/>
  </tr>
);

/**
 * Renders the Nodes Diagnostics Report page.
 */
class Certificates extends React.Component<CertificatesProps, {}> {
  refresh(props = this.props) {
    props.refreshCertificates(new protos.cockroach.server.serverpb.ProblemRangesRequest({
      node_id: this.props.params[nodeIDAttr],
    }));
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

  renderSimpleRow(header: string, value: string) {
    return (
      <tr className="certs-table__row">
         <th className="certs-table__cell certs-table__cell--header">{header}</th>
         <td className="certs-table__cell" title={value}>{value}</td>
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
    return (
      <tr className="certs-table__row">
         <th className="certs-table__cell certs-table__cell--header">{header}</th>
         <td className="certs-table__cell" title={title}>{timestamp}</td>
      </tr>
    );
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
    const { certificates } = this.props;
    if (_.isEmpty(certificates)) {
      return loading;
    }

    if (_.isEmpty(certificates.certificates)) {
      return (
        <div>
          <h1>No certificates were found on node {this.props.params[nodeIDAttr]}.</h1>
        </div>
      );
    }

    const nodeID = this.props.params[nodeIDAttr];
    let header: JSX.Element = null;
    if (_.isNaN(parseInt(nodeID, 10))) {
      header = (
      <h1>
        Local Node certificates
      </h1>
      );
    } else {
      header = (
      <h1>
        Node {nodeID} certificates
      </h1>
      );
    }

    return (
      <div>
        {header}
        {
          _.map(certificates.certificates, (cert, key) => (
            this.renderCert(cert, key)
          ))
        }
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    certificates: state.cachedData.certificates.data,
  };
}

const actions = {
  refreshCertificates,
};

export default connect(mapStateToProps, actions)(Certificates);
