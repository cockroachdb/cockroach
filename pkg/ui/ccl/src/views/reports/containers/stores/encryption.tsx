import React from "react";

import * as protos from "src/js/protos";

interface EncryptionStatusProps {
  store: protos.cockroach.server.serverpb.StoreDetails$Properties;
}

export default class EncryptionStatus extends React.Component<EncryptionStatusProps, {}> {

  renderSimpleRow(header: string, value: string) {
    return (
      <tr className="stores-table__row">
        <th className="stores-table__cell stores-table__cell--header">{header}</th>
        <td className="stores-table__cell" title={value}>{value}</td>
      </tr>
    );
  }

  render() {
    const { store } = this.props;
    return this.renderSimpleRow("Encryption Status", store.encryption_status.toString());
  }
}
