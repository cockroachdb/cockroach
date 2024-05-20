// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { storiesOf } from "@storybook/react";
import noop from "lodash/noop";

import { Button } from "../button";

import {
  ActivateDiagnosticsModalRef,
  ActivateStatementDiagnosticsModal,
} from "./activateStatementDiagnosticsModal";

storiesOf("ActivateStatementDiagnosticsModal", module).add("default", () => {
  const ref = React.useRef<ActivateDiagnosticsModalRef>();
  const onClick = React.useCallback(() => {
    ref.current?.showModalFor("select * from _", [
      "gistaslkdjfas;ldjfls;adjkfl;asjdflsadjfkljshdkfljhsdkjfgsadjkhgfdkjsasdfasdfasfdasdfsadfdahfd1",
      "gist2",
    ]);
  }, [ref]);
  return (
    <>
      <Button type="primary" onClick={onClick}>
        Open
      </Button>
      <ActivateStatementDiagnosticsModal
        activate={noop}
        refreshDiagnosticsReports={noop}
        ref={ref}
      />
    </>
  );
});
