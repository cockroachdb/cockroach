// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { InlineAlert } from "@cockroachlabs/ui-components";
import {
  Checkbox,
  Divider,
  Input,
  Radio,
  Select,
  Space,
  ConfigProvider,
  Row,
  Col,
} from "antd";
import classNames from "classnames/bind";
import React, { useCallback, useImperativeHandle, useState } from "react";

import { Anchor } from "src/anchor";
import { Modal } from "src/modal";
import { Text, TextTypes } from "src/text";
import { statementDiagnostics, statementsSql } from "src/util";

import { crlTheme } from "../antdTheme";
import { InsertStmtDiagnosticRequest } from "../api";

import styles from "./activateStatementDiagnosticsModal.module.scss";

const cx = classNames.bind(styles);

export interface ActivateDiagnosticsModalProps {
  activate: (insertStmtDiagnosticsRequest: InsertStmtDiagnosticRequest) => void;
  refreshDiagnosticsReports: () => void;
  onOpenModal?: (statement: string, planGists: string[]) => void;
}

export interface ActivateDiagnosticsModalRef {
  showModalFor: (statement: string, planGists: string[]) => void;
}

export const ActivateStatementDiagnosticsModal = React.forwardRef<
  ActivateDiagnosticsModalRef,
  ActivateDiagnosticsModalProps
>(({ activate, onOpenModal }, ref) => {
  const [visible, setVisible] = useState(false);
  const [statement, setStatement] = useState<string>();
  const [planGists, setPlanGists] = useState<string[]>();
  const [conditional, setConditional] = useState(true);
  const [filterPerPlanGist, setFilterPerPlanGist] = useState(false);
  const [selectedPlanGist, setSelectedPlanGist] = useState<string>("");
  const [expires, setExpires] = useState(true);
  const [minExecLatency, setMinExecLatency] = useState(100);
  const [minExecLatencyUnit, setMinExecLatencyUnit] = useState("milliseconds");
  const [expiresAfter, setExpiresAfter] = useState(15);
  const [traceSampleRate, setTraceSampleRate] = useState(0.01);
  const [redacted, setRedacted] = useState(false);

  const handleSelectChange = (value: string) => {
    setMinExecLatencyUnit(value);
  };

  const getMinExecLatency = (
    conditional: boolean,
    value: number,
    unit: string,
  ) => {
    const multiplier = unit === "milliseconds" ? 0.001 : 1;
    return conditional ? value * multiplier : 0; // num seconds
  };

  const getExpiresAfter = (expires: boolean, expiresAfter: number) => {
    const numMinutes = expires ? expiresAfter : 0;
    return numMinutes * 60; // num seconds
  };

  const getTraceSampleRate = (
    conditional: boolean,
    traceSampleRate: number,
  ) => {
    if (conditional) {
      return traceSampleRate;
    }
    return 0;
  };

  const onOkHandler = useCallback(() => {
    activate({
      stmtFingerprint: statement,
      planGist: filterPerPlanGist ? selectedPlanGist : null,
      minExecutionLatencySeconds: getMinExecLatency(
        conditional,
        minExecLatency,
        minExecLatencyUnit,
      ),
      expiresAfterSeconds: getExpiresAfter(expires, expiresAfter),
      samplingProbability: getTraceSampleRate(conditional, traceSampleRate),
      redacted: redacted,
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
    traceSampleRate,
    filterPerPlanGist,
    selectedPlanGist,
    redacted,
  ]);

  const onCancelHandler = useCallback(() => setVisible(false), []);

  useImperativeHandle(ref, () => {
    return {
      showModalFor: (forwardStatement: string, forwardPlanGists: string[]) => {
        setStatement(forwardStatement);
        setPlanGists(forwardPlanGists);
        setVisible(true);
        onOpenModal && onOpenModal(forwardStatement, forwardPlanGists);
      },
    };
  });

  if (planGists && selectedPlanGist === "") {
    setSelectedPlanGist(planGists[0]);
  }

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
      <ConfigProvider theme={crlTheme}>
        <Space direction="vertical" className={cx("root")}>
          <Space className={cx("space-bottom")}>
            <Text>
              Diagnostics will be collected for the next execution that matches
              this <Anchor href={statementsSql}>statement fingerprint</Anchor>,
              or according to the trace and latency thresholds set below. The
              request is cancelled when a single diagnostics bundle is captured.{" "}
              <Anchor href={statementDiagnostics}>Learn more</Anchor>
            </Text>
          </Space>
          <Space className={cx("space-bottom")}>
            <Text textType={TextTypes.BodyStrong}>Collect diagnostics:</Text>
          </Space>
          <Radio.Group value={conditional}>
            <Space direction="vertical" size="middle">
              <Radio
                value={true}
                onChange={() => setConditional(true)}
                className={cx("radio")}
              >
                <Text>Trace and collect diagnostics</Text>
              </Radio>
              <Space direction="vertical" className={cx("radio-offset")}>
                <Text>At a sampled rate of:</Text>
                <Row gutter={16}>
                  <Col span={12}>
                    <Select<number>
                      disabled={!conditional}
                      defaultValue={0.01}
                      onChange={setTraceSampleRate}
                      options={[
                        { value: 0.01, label: "1% (recommended)" },
                        { value: 0.02, label: "2%" },
                        { value: 0.03, label: "3%" },
                        { value: 0.04, label: "4%" },
                        { value: 0.05, label: "5%" },
                        { value: 1, label: "100% (not recommended)" },
                      ]}
                      rootClassName={cx("full-width")}
                    />
                  </Col>
                  <Col span={12}>
                    <Text textType={TextTypes.Caption}>
                      We recommend starting at 1% to minimize the impact on
                      performance.
                    </Text>
                  </Col>
                </Row>
                {getTraceSampleRate(conditional, traceSampleRate) === 1 && (
                  <InlineAlert
                    intent="warning"
                    title={
                      <Text>
                        Tracing will be turned on at a 100% sampled rate until
                        diagnostics are collected based on the specified latency
                        threshold setting. This may have a significant impact on
                        performance.
                      </Text>
                    }
                  />
                )}
                <Text>When the statement execution latency exceeds:</Text>
                <Row gutter={16}>
                  <Col flex={"100px"}>
                    <Input
                      type="number"
                      disabled={!conditional}
                      value={minExecLatency}
                      onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
                        if (parseInt(e.target.value) > 0) {
                          setMinExecLatency(parseInt(e.target.value));
                        }
                      }}
                    />
                  </Col>
                  <Col flex={"140px"}>
                    <Select
                      disabled={!conditional}
                      defaultValue="milliseconds"
                      onChange={handleSelectChange}
                      options={[
                        { value: "seconds", label: "seconds" },
                        { value: "milliseconds", milliseconds: "seconds" },
                      ]}
                    />
                  </Col>
                </Row>
              </Space>
              <Radio
                value={false}
                onChange={() => setConditional(false)}
                className={cx("radio")}
              >
                <Text>
                  Trace and collect diagnostics on the next statement execution
                </Text>
              </Radio>
            </Space>
          </Radio.Group>
          <Divider type="horizontal" />

          <Radio.Group
            value={filterPerPlanGist}
            rootClassName={cx("full-width")}
          >
            <Space direction="vertical" size="middle">
              <Radio
                value={false}
                onChange={() => setFilterPerPlanGist(false)}
                className={cx("radio")}
              >
                <Text>For all plan gists</Text>
              </Radio>
              <Space direction="vertical">
                <Radio
                  value={true}
                  onChange={() => setFilterPerPlanGist(true)}
                  className={cx("radio")}
                >
                  <Text>For the following plan gist:</Text>
                </Radio>
                <Row className={cx("radio-offset")}>
                  <Col span={12}>
                    <Select
                      disabled={!filterPerPlanGist}
                      value={selectedPlanGist}
                      defaultValue={planGists ? planGists[0] : ""}
                      onChange={(selected: string) =>
                        setSelectedPlanGist(selected)
                      }
                      showSearch={true}
                      options={planGists?.map((gist: string) => ({
                        value: gist,
                        label: gist,
                      }))}
                      rootClassName={cx("trim-text")}
                    />
                  </Col>
                </Row>
              </Space>
            </Space>
          </Radio.Group>
          <Divider type="horizontal" />
          <Checkbox checked={expires} onChange={() => setExpires(!expires)}>
            <Text>Diagnostics request expires after:</Text>
          </Checkbox>
          <Space className={cx("radio-offset")}>
            <Input
              type="number"
              disabled={!expires}
              value={expiresAfter}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
                if (parseInt(e.target.value) > 0) {
                  setExpiresAfter(parseInt(e.target.value));
                }
              }}
              rootClassName={cx("compact")}
            />
            <Text>minutes</Text>
          </Space>
          {conditional && !expires && (
            <div className={cx("radio-offset")}>
              <InlineAlert
                intent="info"
                title={
                  <Text>
                    Executions of the same statement fingerprint will run slower
                    while diagnostics are activated, so it is recommended to set
                    an expiration time if collecting according to a latency
                    threshold.
                  </Text>
                }
              />
            </div>
          )}
          <Divider type="horizontal" />
          <Checkbox checked={redacted} onChange={() => setRedacted(!redacted)}>
            <Text>Redact</Text>
          </Checkbox>
        </Space>
      </ConfigProvider>
    </Modal>
  );
});
