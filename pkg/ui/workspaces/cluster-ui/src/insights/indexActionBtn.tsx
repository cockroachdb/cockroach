// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import CopyOutlined from "@ant-design/icons/CopyOutlined";
import { InlineAlert } from "@cockroachlabs/ui-components";
import { message } from "antd";
import classNames from "classnames/bind";
import copy from "copy-to-clipboard";
import React, { useCallback, useState } from "react";

import { Anchor } from "../anchor";
import {
  executeIndexRecAction,
  IndexActionResponse,
} from "../api/indexActionsApi";
import { Button } from "../button";
import { Modal } from "../modal";
import { Text, TextTypes } from "../text";
import {
  alterIndex,
  createIndex,
  dropIndex,
  onlineSchemaChanges,
} from "../util";

import styles from "./indexActionBtn.module.scss";
import { InsightType } from "./types";

const cx = classNames.bind(styles);

interface IdxRecProps {
  actionQuery: string;
  actionType: InsightType;
  database: string;
}

const IdxRecAction = (props: IdxRecProps): React.ReactElement => {
  const [visible, setVisible] = useState(false);
  const [applying, setApplying] = useState(false);
  const [btnOkLabel, setBtnOkLabel] = useState("Apply");
  const [error, setError] = useState("");
  const query = addIdxName(props.actionQuery);
  const onOkHandler = useCallback(() => {
    setError("");
    setBtnOkLabel("Applying");
    setApplying(true);
    executeIndexRecAction(query, props.database).then(
      (r: IndexActionResponse) => {
        setBtnOkLabel("Apply");
        setApplying(false);
        let foundError = false;
        for (let i = 0; i < r.length; i++) {
          if (r[i].status === "FAILED") {
            foundError = true;
            setError(r[i].error);
            break;
          }
        }
        if (!foundError) {
          setVisible(false);
          message.success("Recommendation applied");
        }
      },
    );
  }, [props.database, query]);

  const showModal = (): void => {
    setError("");
    setVisible(true);
  };

  const onCancelHandler = useCallback(() => setVisible(false), []);
  const onCopyClick = () => {
    copy(query);
    message.success("Copied to clipboard");
  };

  let title = "Update index";
  let btnLAbel = "Update Index";
  let descriptionDocs = <></>;
  switch (props.actionType) {
    case "CreateIndex":
      title = "create a new index";
      btnLAbel = "Create Index";
      descriptionDocs = (
        <>
          {"a "}
          <Anchor href={createIndex} target="_blank">
            CREATE INDEX
          </Anchor>
          {" statement"}
        </>
      );
      break;
    case "DropIndex":
      title = "drop the unused index";
      btnLAbel = "Drop Index";
      descriptionDocs = (
        <>
          {"a "}
          <Anchor href={dropIndex} target="_blank">
            DROP INDEX
          </Anchor>
          {" statement"}
        </>
      );
      break;
    case "ReplaceIndex":
      title = "replace the index";
      btnLAbel = "Replace Index";
      descriptionDocs = (
        <>
          {" "}
          <Anchor href={createIndex} target="_blank">
            CREATE INDEX
          </Anchor>
          {" and "}
          <Anchor href={dropIndex} target="_blank">
            DROP INDEX
          </Anchor>
          {" statements"}
        </>
      );
      break;
    case "AlterIndex":
      title = "alter the index";
      btnLAbel = "Alter Index";
      descriptionDocs = (
        <>
          {"an "}
          <Anchor href={alterIndex} target="_blank">
            ALTER INDEX
          </Anchor>
          {" statement"}
        </>
      );
      break;
  }

  return (
    <>
      <Button onClick={showModal} type="secondary" size="small">
        {btnLAbel}
      </Button>
      <Modal
        visible={visible}
        onOk={onOkHandler}
        onCancel={onCancelHandler}
        okText={btnOkLabel}
        cancelText="Cancel"
        title={`Do you want to ${title}?`}
        okLoading={applying}
      >
        <Text>
          This action will apply the single-statement index recommendation by
          executing {descriptionDocs}.{" "}
          <Anchor href={onlineSchemaChanges} target="_blank">
            Learn more
          </Anchor>
        </Text>
        <Text textType={TextTypes.Code} className={"code-area"}>
          {query}
          <br />
          <Button
            type={"unstyled-link"}
            size={"small"}
            className={cx("bottom-corner")}
            icon={<CopyOutlined className={cx("copy-icon")} />}
            onClick={onCopyClick}
          >
            Copy
          </Button>
        </Text>
        <InlineAlert
          intent="warning"
          className={cx("alert-area")}
          title={`Schema changes consume additional
          resources and can potentially negatively impact workload
          responsiveness.`}
        />
        {error.length > 0 && (
          <InlineAlert
            intent="danger"
            className={cx("alert-area")}
            title={`Fail to execute recommendation. ${error}`}
          />
        )}
      </Modal>
    </>
  );
};

function addIdxName(statement: string): string {
  if (!statement.toUpperCase().includes("CREATE INDEX ON")) {
    return statement;
  }
  let result = "";
  const statements = statement.split(";");
  for (let i = 0; i < statements.length; i++) {
    if (statements[i].trim().toUpperCase().startsWith("CREATE INDEX ON ")) {
      result = `${result}${createIdxName(statements[i])}; `;
    } else if (statements[i].length !== 0) {
      result = `${result}${statements[i]};`;
    }
  }
  return result;
}

// createIdxName creates an index name, roughly following
// Postgres's conventions for naming anonymous indexes.
// For example:
//
//   CREATE INDEX ON t (a)
//   => t_a_rec_idx
//
//   CREATE INDEX ON t ((a + b), c, lower(d))
//   => t_expr_c_expr1_rec_idx
//
// Adding an extra _rec to the name, so we can identify statistics on indexes
// that were creating using this feature.
export function createIdxName(statement: string): string {
  let idxName = "";
  let info = statement.toLowerCase().replace("create index on ", "");

  idxName += info.substring(0, info.indexOf("(")).trim(); // Add the table name.
  info = info.substring(info.indexOf("(") + 1);

  let parenthesis = 1;
  let i = 0;
  while (parenthesis > 0 && i < info.length) {
    if (info[i] === "(") {
      parenthesis++;
    }
    if (info[i] === ")") {
      parenthesis--;
    }
    i++;
  }
  info = info.substring(0, i - 1);

  const variables = info.split(",");
  let expressions = 0;
  let value;
  for (let i = 0; i < variables.length; i++) {
    value = variables[i].trim();
    if (isExpression(value)) {
      idxName += "_expr";
      if (expressions > 0) {
        idxName += expressions;
      }
      expressions++;
    } else {
      idxName += "_" + value;
    }
  }
  const suffix =
    statement.indexOf("STORING") >= 0 ? "_storing_rec_idx" : "_rec_idx";
  // The table name is fully qualified at this point, but we don't need the full name,
  // so just use the last value (also an index name can't have ".")
  const idxNameArr = idxName.split(".");
  idxName =
    idxNameArr[idxNameArr.length - 1].replace(/"/g, "").replace(/\s/g, "_") +
    suffix;

  return statement.replace(
    "CREATE INDEX ON ",
    `CREATE INDEX IF NOT EXISTS ${idxName} ON `,
  );
}

function isExpression(value: string): boolean {
  const expElements = ["(", "+", "-", "*", "/", "%"];
  for (let i = 0; i < expElements.length; i++) {
    if (value.includes(expElements[i])) {
      return true;
    }
  }
  return false;
}

export default IdxRecAction;
