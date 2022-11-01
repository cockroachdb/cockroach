// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Alert, Button, Checkbox, Divider, Input, Radio, Select } from "antd";
import "antd/lib/radio/style";
import "antd/lib/button/style";
import "antd/lib/input/style";
import "antd/lib/checkbox/style";
import "antd/lib/divider/style";
import "antd/lib/select/style";
import "antd/lib/alert/style";
import React, { useCallback, useImperativeHandle, useState } from "react";
import { Modal } from "src/modal";
import { Anchor } from "src/anchor";
import { Text } from "src/text";
import { statementDiagnostics, statementsSql } from "src/util";
import classNames from "classnames/bind";
import styles from "./activateStatementDiagnosticsModal.scss";
import { InfoCircleFilled } from "@cockroachlabs/icons";
import { InsertStmtDiagnosticRequest } from "../api";

const cx = classNames.bind(styles);
const { Option } = Select;

export interface ActivateDiagnosticsModalProps {
  activate: (insertStmtDiagnosticsRequest: InsertStmtDiagnosticRequest) => void;
  refreshDiagnosticsReports: () => void;
  onOpenModal?: (statement: string) => void;
}

export interface ActivateDiagnosticsModalRef {
  showModalFor: (statement: string) => void;
}

export const ActivateStatementDiagnosticsModal = React.forwardRef(
  (
    { activate, onOpenModal }: ActivateDiagnosticsModalProps,
    ref: React.RefObject<ActivateDiagnosticsModalRef>,
  ) => {
    const [visible, setVisible] = useState(false);
    const [statement, setStatement] = useState<string>();
    const [conditional, setConditional] = useState(false);
    const [expires, setExpires] = useState(true);
    const [minExecLatency, setMinExecLatency] = useState(100);
    const [minExecLatencyUnit, setMinExecLatencyUnit] =
      useState("milliseconds");
    const [expiresAfter, setExpiresAfter] = useState(15);

    const handleSelectChange = (value: string) => {
      setMinExecLatencyUnit(value);
    };

    const getMinExecLatency = (
      conditional: boolean,
      value: number,
      unit: string,
    ) => {
      const multiplier = unit == "milliseconds" ? 0.001 : 1;
      return conditional ? value * multiplier : 0; // num seconds
    };

    const getExpiresAfter = (expires: boolean, expiresAfter: number) => {
      const numMinutes = expires ? expiresAfter : 0;
      return numMinutes * 60; // num seconds
    };

    const onOkHandler = useCallback(() => {
      activate({
        stmtFingerprint: statement,
        minExecutionLatencySeconds: getMinExecLatency(
          conditional,
          minExecLatency,
          minExecLatencyUnit,
        ),
        expiresAfterSeconds: getExpiresAfter(expires, expiresAfter),
      });
      setVisible(false);
    }, [
      activate,
      statement,
      conditional,
      minExecLatency,
      minExecLatencyUnit,
      expires,
      expiresAfter,
    ]);

    const onCancelHandler = useCallback(() => setVisible(false), []);

    useImperativeHandle(ref, () => {
      return {
        showModalFor: (forwardStatement: string) => {
          setStatement(forwardStatement);
          setVisible(true);
          onOpenModal && onOpenModal(forwardStatement);
        },
      };
    });

    return (
      <Modal
        visible={visible}
        onOk={onOkHandler}
        onCancel={onCancelHandler}
        okText="Activate"
        cancelText="Cancel"
        title="Activate statement diagnostics"
        className={cx("modal-body")}
      >
        <Text>
          Diagnostics will be collected for the next execution that matches this{" "}
          <Anchor href={statementsSql}>statement fingerprint</Anchor>, or when
          the execution of the statement fingerprint exceeds a specified
          latency. The request is cancelled when a single bundle is captured.{" "}
          <Anchor href={statementDiagnostics}>Learn more</Anchor>
        </Text>
        <div className={cx("diagnostic__options-container")}>
          <Text className={cx("diagnostic__heading")}>Collect diagnostics</Text>
          <Radio.Group value={conditional}>
            <Button.Group className={cx("diagnostic__btn-group")}>
              <Radio
                value={false}
                className={cx("diagnostic__radio-btn")}
                onChange={() => setConditional(false)}
              >
                On the next execution
              </Radio>
              <Radio
                value={true}
                className={cx("diagnostic__radio-btn")}
                onChange={() => setConditional(true)}
              >
                On the next execution where the latency exceeds
                <div className={cx("diagnostic__conditional-container")}>
                  <div className={cx("diagnostic__min-latency-container")}>
                    <Input
                      type="number"
                      className={cx("diagnostic__input__min-latency-time")}
                      disabled={!conditional}
                      value={minExecLatency}
                      onChange={e => {
                        if (parseInt(e.target.value) > 0) {
                          setMinExecLatency(parseInt(e.target.value));
                        }
                      }}
                      size="large"
                    />
                    <Select
                      disabled={!conditional}
                      defaultValue="milliseconds"
                      onChange={handleSelectChange}
                      className={cx("diagnostic__select__min-latency-unit")}
                      size="large"
                    >
                      <Option value="seconds">seconds</Option>
                      <Option value="milliseconds">milliseconds</Option>
                    </Select>
                  </div>
                  <Divider type="horizontal" style={{ marginBottom: 0 }} />
                </div>
              </Radio>
            </Button.Group>
          </Radio.Group>
          <Checkbox checked={expires} onChange={() => setExpires(!expires)}>
            <div className={cx("diagnostic__checkbox-text")}>
              Diagnostics request expires after:
            </div>
            <div className={cx("diagnostic__expires-after-container")}>
              <Input
                type="number"
                size="large"
                className={cx("diagnostic__input__expires-after-time")}
                disabled={!expires}
                value={expiresAfter}
                onChange={e => {
                  if (parseInt(e.target.value) > 0) {
                    setExpiresAfter(parseInt(e.target.value));
                  }
                }}
              />
              <div className={cx("diagnostic__checkbox-text")}>minutes</div>
            </div>
            {conditional && !expires && (
              <div className={cx("diagnostic__alert")}>
                <Alert
                  icon={
                    <div className={cx("diagnostic__alert-icon")}>
                      <InfoCircleFilled fill="#0055FF" height={20} width={20} />
                    </div>
                  }
                  message={
                    <div className={cx("diagnostic__alert-message")}>
                      Executions of the same statement fingerprint will run
                      slower while diagnostics are activated, so it is
                      recommended to set an expiration time if collecting
                      according to a latency threshold.
                    </div>
                  }
                  type="info"
                  showIcon
                />
              </div>
            )}
          </Checkbox>
        </div>
      </Modal>
    );
  },
);
