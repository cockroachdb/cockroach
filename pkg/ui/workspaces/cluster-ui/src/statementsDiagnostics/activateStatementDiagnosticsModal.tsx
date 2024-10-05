// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Button, Checkbox, Divider, Input, Radio, Select } from "antd";
import "antd/lib/radio/style";
import "antd/lib/button/style";
import "antd/lib/input/style";
import "antd/lib/checkbox/style";
import "antd/lib/divider/style";
import "antd/lib/select/style";
import React, { useCallback, useImperativeHandle, useState } from "react";
import { Modal } from "src/modal";
import { Anchor } from "src/anchor";
import { Text } from "src/text";
import { statementDiagnostics, statementsSql } from "src/util";
import classNames from "classnames/bind";
import styles from "./activateStatementDiagnosticsModal.scss";
import { InsertStmtDiagnosticRequest } from "../api";
import { InlineAlert } from "@cockroachlabs/ui-components";

const cx = classNames.bind(styles);
const { Option } = Select;

export interface ActivateDiagnosticsModalProps {
  activate: (insertStmtDiagnosticsRequest: InsertStmtDiagnosticRequest) => void;
  refreshDiagnosticsReports: () => void;
  onOpenModal?: (statement: string, planGists: string[]) => void;
}

export interface ActivateDiagnosticsModalRef {
  showModalFor: (statement: string, planGists: string[]) => void;
}

export const ActivateStatementDiagnosticsModal = React.forwardRef(
  (
    { activate, onOpenModal }: ActivateDiagnosticsModalProps,
    ref: React.RefObject<ActivateDiagnosticsModalRef>,
  ) => {
    const [visible, setVisible] = useState(false);
    const [statement, setStatement] = useState<string>();
    const [planGists, setPlanGists] = useState<string[]>();
    const [conditional, setConditional] = useState(true);
    const [filterPerPlanGist, setFilterPerPlanGist] = useState(false);
    const [selectedPlanGist, setSelectedPlanGist] = useState<string>("");
    const [expires, setExpires] = useState(true);
    const [minExecLatency, setMinExecLatency] = useState(100);
    const [minExecLatencyUnit, setMinExecLatencyUnit] =
      useState("milliseconds");
    const [expiresAfter, setExpiresAfter] = useState(15);
    const [traceSampleRate, setTraceSampleRate] = useState(0.01);

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
    ]);

    const onCancelHandler = useCallback(() => setVisible(false), []);

    useImperativeHandle(ref, () => {
      return {
        showModalFor: (
          forwardStatement: string,
          forwardPlanGists: string[],
        ) => {
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
        <Text>
          Diagnostics will be collected for the next execution that matches this{" "}
          <Anchor href={statementsSql}>statement fingerprint</Anchor>, or
          according to the trace and latency thresholds set below. The request
          is cancelled when a single diagnostics bundle is captured.{" "}
          <Anchor href={statementDiagnostics}>Learn more</Anchor>
        </Text>
        <div className={cx("diagnostic__options-container")}>
          <Text className={cx("diagnostic__heading")}>
            Collect diagnostics:
          </Text>
          <Radio.Group value={conditional}>
            <Button.Group className={cx("diagnostic__btn-group")}>
              <Radio
                value={true}
                className={cx("diagnostic__radio-btn")}
                onChange={() => setConditional(true)}
              >
                Trace and collect diagnostics
                <div className={cx("diagnostic__conditional-container")}>
                  <div className={cx("diagnostic__select-text")}>
                    At a sampled rate of:
                  </div>
                  <div className={cx("diagnostic__trace-container")}>
                    <Select<number>
                      disabled={!conditional}
                      defaultValue={0.01}
                      onChange={setTraceSampleRate}
                      className={cx("diagnostic__select__trace")}
                      size="large"
                    >
                      <Option value={0.01}>1% (recommended)</Option>
                      <Option value={0.02}>2%</Option>
                      <Option value={0.03}>3%</Option>
                      <Option value={0.04}>4%</Option>
                      <Option value={0.05}>5%</Option>
                      <Option value={1}>100% (not recommended)</Option>
                    </Select>
                    <span className={cx("diagnostic__trace-warning")}>
                      We recommend starting at 1% to minimize the impact on
                      performance.
                    </span>
                  </div>
                  {getTraceSampleRate(conditional, traceSampleRate) === 1 && (
                    <div className={cx("diagnostic__warning")}>
                      <InlineAlert
                        intent="warning"
                        title="Tracing will be turned on at a 100% sampled rate until
                      diagnostics are collected based on the specified latency threshold
                      setting. This may have a significant impact on performance."
                      />
                    </div>
                  )}
                  <div className={cx("diagnostic__select-text")}>
                    When the statement execution latency exceeds:
                  </div>
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
                </div>
              </Radio>
              <Radio
                value={false}
                className={cx("diagnostic__radio-btn")}
                onChange={() => setConditional(false)}
              >
                Trace and collect diagnostics on the next statement execution
              </Radio>
            </Button.Group>
          </Radio.Group>
          <Divider type="horizontal" />

          <Radio.Group
            value={filterPerPlanGist}
            className={cx("diagnostic__plan-gist-group")}
          >
            <Button.Group className={cx("diagnostic__btn-group")}>
              <Radio
                value={false}
                className={cx("diagnostic__radio-btn", "margin-bottom")}
                onChange={() => setFilterPerPlanGist(false)}
              >
                For all plan gists
              </Radio>
              <br />
              <Radio
                value={true}
                className={cx("diagnostic__radio-btn")}
                onChange={() => setFilterPerPlanGist(true)}
              >
                For the following plan gist:
                <div className={cx("diagnostic__plan-gist-container")}>
                  <Select
                    disabled={!filterPerPlanGist}
                    value={selectedPlanGist}
                    defaultValue={planGists ? planGists[0] : ""}
                    onChange={(selected: string) =>
                      setSelectedPlanGist(selected)
                    }
                    className={cx("diagnostic__select__plan-gist")}
                    size="large"
                    showSearch={true}
                  >
                    {planGists?.map((gist: string) => {
                      return (
                        <Option value={gist} key={gist}>
                          {gist}
                        </Option>
                      );
                    })}
                  </Select>
                </div>
              </Radio>
            </Button.Group>
          </Radio.Group>
          <Divider type="horizontal" />
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
                <InlineAlert
                  intent="info"
                  title="Executions of the same statement fingerprint will run
                      slower while diagnostics are activated, so it is
                      recommended to set an expiration time if collecting
                      according to a latency threshold."
                />
              </div>
            )}
          </Checkbox>
        </div>
      </Modal>
    );
  },
);
