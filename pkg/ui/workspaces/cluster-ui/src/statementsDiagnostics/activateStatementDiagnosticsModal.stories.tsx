// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import noop from "lodash/noop";
import React from "react";

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
