import React from "react";

import * as protos from "src/js/protos";

export interface EncryptionStatusProps {
  store: protos.cockroach.server.serverpb.IStoreDetails;
}

export default class EncryptionStatus extends React.Component<EncryptionStatusProps, {}> {

  render(): React.ReactElement<any> {
    return null;
  }
}
