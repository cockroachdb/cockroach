// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { CaretLeft, CaretRight } from "@cockroachlabs/icons";
import { Button, Tooltip } from "antd";
import classNames from "classnames/bind";
import React from "react";

import styles from "./timeFrameControls.module.scss";
import { ArrowDirection } from "./timeScaleTypes";

const cx = classNames.bind(styles);

const ButtonGroup = Button.Group;

export interface RangeSelectProps {
  // If onArrowClick exists, don't display the arrow next to the dropdown,
  // display left and right arrows to either side instead.
  onArrowClick?: (direction: ArrowDirection) => void;
  // Disable any arrows in the arrow direction array.
  disabledArrows?: ArrowDirection[];
}

export const TimeFrameControls = ({
  onArrowClick,
  disabledArrows,
}: RangeSelectProps): React.ReactElement => {
  const handleChangeArrow = (direction: ArrowDirection) => () =>
    onArrowClick(direction);

  const left = disabledArrows.includes(ArrowDirection.LEFT);
  const canForward = !disabledArrows.includes(ArrowDirection.RIGHT);
  const delay = 0.3;

  return (
    <div className={cx("controls-content")}>
      <ButtonGroup>
        <Tooltip
          placement="bottom"
          title="Previous time interval"
          mouseEnterDelay={delay}
          mouseLeaveDelay={delay}
        >
          <Button
            onClick={handleChangeArrow(ArrowDirection.LEFT)}
            disabled={left}
            className={cx("_action", left ? "disabled" : "active")}
            aria-label={"previous time interval"}
          >
            <CaretLeft className={cx("icon")} />
          </Button>
        </Tooltip>
        <Tooltip
          placement="bottom"
          title="Next time interval"
          mouseEnterDelay={delay}
          mouseLeaveDelay={delay}
        >
          <Button
            onClick={handleChangeArrow(
              canForward ? ArrowDirection.RIGHT : ArrowDirection.CENTER,
            )}
            className={cx("_action", "active")}
            aria-label={"next time interval"}
          >
            <CaretRight className={cx("icon")} />
          </Button>
        </Tooltip>
      </ButtonGroup>
      <Tooltip
        placement="bottom"
        title="Most recent interval"
        mouseEnterDelay={delay}
        mouseLeaveDelay={delay}
      >
        <Button
          onClick={handleChangeArrow(ArrowDirection.CENTER)}
          className={cx("_action", "active", "btn__now")}
        >
          Now
        </Button>
      </Tooltip>
    </div>
  );
};

export default TimeFrameControls;
