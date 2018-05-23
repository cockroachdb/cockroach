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
        <td className="stores-table__cell" title={value}><pre>{value}</pre></td>
      </tr>
    );
  }

  render(): React.ReactElement<any> {
    const { store } = this.props;
    const rawStatus = store.encryption_status;

    try {
      const decodedStatus = protos.cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus.decode(rawStatus);
      return this.renderSimpleRow("Encryption Status", JSON.stringify(decodedStatus.toJSON(), null, 2));
    } catch (e) {
      console.log("Error decoding protobuf: ", e);
      return null;
    }
  }
}
