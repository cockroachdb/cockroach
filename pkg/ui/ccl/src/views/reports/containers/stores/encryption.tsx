import React from "react";

import * as protos from "src/js/protos";
import moment from "moment";
import { EncryptionStatusProps } from "oss/src/views/reports/containers/stores/encryption";
import { Bytes } from "src/util/format";

const dateFormat = "Y-MM-DD HH:mm:ss Z";

export default class EncryptionStatus extends React.Component<EncryptionStatusProps, {}> {

  renderSimpleRow(header: string, value: string) {
    return (
      <tr className="stores-table__row">
        <th className="stores-table__cell stores-table__cell--header">{header}</th>
        <td className="stores-table__cell" title={value}><pre>{value}</pre></td>
      </tr>
    );
  }

  renderSimpleAlternateRow(header: string, value: string) {
    return (
      <tr className="stores-table__row">
        <th className="stores-table__cell stores-table__cell--header">{header}</th>
        <td className="stores-table__cell stores-table__cell--alternate" title={value}><pre>{value}</pre></td>
      </tr>
    );
  }


  renderKey(isStoreKey: boolean, key: protos.cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo) {
    // Get the enum name from its value (eg: "AES128_CTR" for 1).
    const encryptionType = protos.cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionType.__proto__[key.encryption_type];
    const createdAt = moment.unix(key.creation_time).utc().format(dateFormat);

    if (isStoreKey) {
      return [
        this.renderSimpleAlternateRow("Active Store Key", encryptionType),
        this.renderSimpleAlternateRow("Key ID", key.key_id),
        this.renderSimpleAlternateRow("Created", createdAt),
        this.renderSimpleAlternateRow("Source", key.source),
      ];
    } else {
      return [
        this.renderSimpleRow("Active Data Key", encryptionType),
        this.renderSimpleRow("Key ID", key.key_id),
        this.renderSimpleRow("Created", createdAt),
        this.renderSimpleRow("Parent Key", key.parent_key_id),
      ];
    }
  }

  renderFileStats(stats: protos.cockroach.server.serverpb.StoreDetails$Properties) {
    if (stats.total_files === 0 && stats.total_bytes === 0) {
      return null;
    }

    var percentFiles = 100;
    if (stats.active_files != stats.total_files) {
      percentFiles = (stats.active_files * 100) / stats.total_files;
    }
    var fileDetails = percentFiles.toFixed(2) + "%";
    fileDetails += " (" + stats.active_files + "/" + stats.total_files + ")";

    var percentBytes = 100;
    if (stats.active_bytes != stats.total_bytes) {
      percentBytes = (stats.active_bytes * 100) / stats.total_bytes;
    }
    var byteDetails = percentBytes.toFixed(2) + "%";
    byteDetails += " (" + Bytes(stats.active_bytes) + "/" + Bytes(stats.total_bytes) + ")";


    return [
      this.renderSimpleAlternateRow("Encryption Progress", "Fraction encrypted using the active data key"),
      this.renderSimpleAlternateRow("Files", fileDetails),
      this.renderSimpleAlternateRow("Bytes", byteDetails),
    ];
    return null;
  }

  render(): React.ReactElement<any> {
    const { store } = this.props;
    const rawStatus = store.encryption_status;
    if (_.isEmpty(rawStatus)) {
      return null;
    }

    var decodedStatus;

    // Attempt to decode protobuf.
    try {
      decodedStatus = protos.cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus.decode(rawStatus);
    } catch (e) {
      console.log("Error decoding protobuf: ", e);
      return null;
    }

    return [
      this.renderKey(true, decodedStatus.active_store_key),
      this.renderKey(false, decodedStatus.active_data_key),
      this.renderFileStats(store),
    ];
  }
}
