import React from "react";

import * as protos from "src/js/protos";
import _ from "lodash";
import Long from "long";
import moment from "moment";
import { EncryptionStatusProps } from "oss/src/views/reports/containers/stores/encryption";
import { Bytes } from "src/util/format";
import { FixLong } from "src/util/fixLong";

const dateFormat = "Y-MM-DD HH:mm:ss";

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
        <td className="stores-table__cell" title={value}>{value}</td>
      </tr>
    );
  }

  renderStoreKey(key: protos.cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties) {
    // Get the enum name from its value (eg: "AES128_CTR" for 1).
    const encryptionType = protos.cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionType[key.encryption_type];
    const createdAt = moment.unix(key.creation_time.toNumber()).utc().format(dateFormat);

    return [
      this.renderHeaderRow("Active Store Key: user specified"),
      this.renderSimpleRow("Algorithm", encryptionType),
      this.renderSimpleRow("Key ID", key.key_id),
      this.renderSimpleRow("Created", createdAt),
      this.renderSimpleRow("Source", key.source),
    ];
  }

  renderDataKey(key: protos.cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties) {
    // Get the enum name from its value (eg: "AES128_CTR" for 1).
    const encryptionType = protos.cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionType[key.encryption_type];
    const createdAt = moment.unix(key.creation_time.toNumber()).utc().format(dateFormat);

    return [
      this.renderHeaderRow("Active Data Key: automatically generated"),
      this.renderSimpleRow("Algorithm", encryptionType),
      this.renderSimpleRow("Key ID", key.key_id),
      this.renderSimpleRow("Created", createdAt),
      this.renderSimpleRow("Parent Key ID", key.parent_key_id),
    ];
  }

  calculatePercentage(active: Long, total: Long): number {
    if (active === total) {
      return 100;
    }
    return Long.fromInt(100).mul(active).toNumber() / total.toNumber();
  }

  renderFileStats(stats: protos.cockroach.server.serverpb.StoreDetails$Properties) {
    let total_files = FixLong(stats.total_files);
    let total_bytes = FixLong(stats.total_bytes);
    if (total_files.eq(0) && total_bytes.eq(0)) {
      return null;
    }

    let active_files = FixLong(stats.active_key_files);
    let active_bytes = FixLong(stats.active_key_bytes);

    let fileDetails = this.calculatePercentage(active_files, total_files).toFixed(2) + "%";
    fileDetails += " (" + active_files + "/" + total_files + ")";

    let byteDetails = this.calculatePercentage(active_bytes, total_bytes).toFixed(2) + "%";
    byteDetails += " (" + Bytes(active_bytes.toNumber()) + "/" + Bytes(total_bytes.toNumber()) + ")";

    return [
      this.renderHeaderRow("Encryption Progress: fraction encrypted using the active data key"),
      this.renderSimpleRow("Files", fileDetails),
      this.renderSimpleRow("Bytes", byteDetails),
    ];
  }

  decodeEncryptionStatus(data: Uint8Array): protos.cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus {
    let decodedStatus;

    // Attempt to decode protobuf.
    try {
      decodedStatus = protos.cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus.decode(data);
    } catch (e) {
      console.log("Error decoding protobuf: ", e);
      return null;
    }
    return decodedStatus;
  }

  render() {
    const { store } = this.props;
    const rawStatus = store.encryption_status;
    if (_.isEmpty(rawStatus)) {
      return null;
    }

    let decodedStatus = this.decodeEncryptionStatus(rawStatus);
    if (decodedStatus == null) {
      return null;
    }

    return [
      this.renderStoreKey(decodedStatus.active_store_key),
      this.renderDataKey(decodedStatus.active_data_key),
      this.renderFileStats(store),
    ];
  }
}
