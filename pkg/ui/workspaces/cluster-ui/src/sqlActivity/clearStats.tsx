// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useCallback, useState } from "react";
import { StatisticType } from "../statsTableUtil/statsTableUtil";
import classNames from "classnames/bind";
import styles from "./sqlActivity.module.scss";
import { Modal } from "../modal";
import { Text } from "../text";

const cx = classNames.bind(styles);

interface clearStatsProps {
  resetSQLStats: () => void;
  tooltipType: StatisticType;
}

const ClearStats = (props: clearStatsProps): React.ReactElement => {
  const [visible, setVisible] = useState(false);
  const onOkHandler = useCallback(() => {
    props.resetSQLStats();
    setVisible(false);
  }, [props]);

  const showModal = (): void => {
    setVisible(true);
  };

  const onCancelHandler = useCallback(() => setVisible(false), []);

  return (
    <>
      <a className={cx("action", "separator")} onClick={showModal}>
        Reset SQL Stats
      </a>
      <Modal
        visible={visible}
        onOk={onOkHandler}
        onCancel={onCancelHandler}
        okText="Continue"
        cancelText="Cancel"
        title="Do you want to reset SQL stats?"
      >
        <Text>
          This action will reset SQL stats on the Statements and Transactions
          pages and crdb_internal tables. Statistics will be cleared and
          unrecoverable for all users across the entire cluster.
        </Text>
      </Modal>
    </>
  );
};

export default ClearStats;
