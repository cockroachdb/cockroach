import React from "react";

import * as protos from "src/js/protos";

interface EncryptionStatusProps {
  store: protos.cockroach.server.serverpb.StoreDetails$Properties;
}

export default class EncryptionStatus extends React.Component<EncryptionStatusProps, {}> {

  render(): React.ReactElement<any> {
    return null;
  }
}
