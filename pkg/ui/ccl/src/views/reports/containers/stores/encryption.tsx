import React from "react";

import * as protos from "src/js/protos";
import _ from "lodash";
import Long from "long";
import moment from "moment";
import { EncryptionStatusProps } from "oss/src/views/reports/containers/stores/encryption";
import { Bytes } from "src/util/format";

const dateFormat = "Y-MM-DD HH:mm:ss Z";

export default class EncryptionStatus extends React.Component<EncryptionStatusProps, {}> {

  renderHeaderRow(header: string) {
    return (
      <tr className="stores-table__row">
        <td colspan=2 className="stores-table__cell stores-table__cell--header--row">{header}</td>
      </tr>
    );
  }

  renderSimpleRow(header: string, value: string) {
    return (
      <tr className="stores-table__row">
        <th className="stores-table__cell stores-table__cell--header">{header}</th>
        <td className="stores-table__cell" title={value}><pre>{value}</pre></td>
      </tr>
    );
  }

  renderKey(isStoreKey: boolean, key: protos.cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties) {
    // Get the enum name from its value (eg: "AES128_CTR" for 1).
    const encryptionType = protos.cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionType[key.encryption_type];
    const createdAt = moment.unix(key.creation_time.toNumber()).utc().format(dateFormat);

    if (isStoreKey) {
      return [
        this.renderHeaderRow("Active Store Key: user specified"),
        this.renderSimpleRow("Algorithm", encryptionType),
        this.renderSimpleRow("Key ID", key.key_id),
        this.renderSimpleRow("Created", createdAt),
        this.renderSimpleRow("Source", key.source),
      ];
    } else {
      return [
        this.renderHeaderRow("Active Data Key: automatically generated"),
        this.renderSimpleRow("Algorithm", encryptionType),
        this.renderSimpleRow("Key ID", key.key_id),
        this.renderSimpleRow("Created", createdAt),
        this.renderSimpleRow("Parent Key ID", key.parent_key_id),
      ];
    }
  }

  renderFileStats(stats: protos.cockroach.server.serverpb.StoreDetails$Properties) {
    if (stats.total_files.eq(0) && stats.total_bytes.eq(0)) {
      return null;
    }

    let percentFiles = 100;
    if (stats.active_key_files !== stats.total_files) {
      percentFiles = Long.fromInt(100).mul(stats.active_key_files).toNumber() / stats.total_files.toNumber();
    }
    let fileDetails = percentFiles.toFixed(2) + "%";
    fileDetails += " (" + stats.active_key_files + "/" + stats.total_files + ")";

    let percentBytes = 100;
    if (stats.active_key_bytes !== stats.total_bytes) {
      percentBytes = Long.fromInt(100).mul(stats.active_key_bytes).toNumber() / stats.total_bytes.toNumber();
    }
    let byteDetails = percentBytes.toFixed(2) + "%";
    byteDetails += " (" + Bytes(stats.active_key_bytes.toNumber()) + "/" + Bytes(stats.total_bytes.toNumber()) + ")";

    return [
      this.renderHeaderRow("Encryption Progress: fraction encrypted using the active data key"),
      this.renderSimpleRow("Files", fileDetails),
      this.renderSimpleRow("Bytes", byteDetails),
    ];
  }

  render() {
    const { store } = this.props;
    const rawStatus = store.encryption_status;
    if (_.isEmpty(rawStatus)) {
      return null;
    }

    let decodedStatus;

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
