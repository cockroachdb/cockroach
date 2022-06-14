// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useState } from "react";
import classNames from "classnames/bind";
import { CaretUp, CaretDown } from "@cockroachlabs/icons";
import { Radio } from "antd";
import "antd/lib/radio/style";
import type { RadioChangeEvent } from "antd/lib/radio";
import { Button } from "../button";

import styles from "../statementsPage/statementTypeSelect.module.scss";

const cx = classNames.bind(styles);

export type Option = {
  value: string;
  label: string;
  description: React.ReactChild;
  component: React.ReactElement;
};

type SelectProps = {
  options: Option[];
  value: string;
  onChange: (value: string) => void;
};

export const SelectWithDescription = ({
  options,
  value,
  onChange,
}: SelectProps): React.ReactElement => {
  const [showDescription, setShowDescription] = useState<boolean>(false);
  const onSelectChange = (e: RadioChangeEvent) => {
    onChange(e.target.value);
  };

  const toggleDescription = (): void => {
    setShowDescription(!showDescription);
  };

  const getDescription = (): React.ReactChild => {
    return options.find(option => option.value === value).description;
  };

  const description = getDescription();

  return (
    <div className={cx("statement-select")}>
      <div className={cx("select-options")}>
        <Radio.Group
          className={cx("radio-group")}
          value={value}
          onChange={onSelectChange}
        >
          {options.map(option => (
            <Radio key={option.value} value={option.value}>
              {option.label}
            </Radio>
          ))}
        </Radio.Group>
        <Button
          className={cx("description-button")}
          onClick={toggleDescription}
          type="unstyled-link"
          icon={showDescription ? <CaretUp /> : <CaretDown />}
          iconPosition="right"
        >
          {showDescription ? "Hide" : "Show"} description
        </Button>
      </div>
      <div className={cx("description", !showDescription && "collapsed")}>
        {description}
      </div>
    </div>
  );
};
