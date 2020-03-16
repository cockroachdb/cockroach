// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useState } from "react";
import cn from "classnames";
import _ from "lodash";
import "./select.styl";
import { Button, ButtonTypes } from "../button";
import { CaretDownIcon } from "../icon/caretDown";
import { OutsideEventHandler } from "../outsideEventHandler";

export interface SelectOption {
  label: string;
  value: string;
}

interface SelectProps {
  type?: ButtonTypes;
  value?: string;
  options?: SelectOption[];
  title: string;
  onChange?: (option: SelectOption) => void;
  icon?: React.ReactNode;
  iconPosition?: "left" | "right";
  children?: any;
  id?: string;
  label?: any;
}

export function Select(props: SelectProps) {
  const { value, options, id, type, children, title, icon, iconPosition, onChange } = props;
  const [visible, setVisible] = useState(false);
  const classNames = cn("crl-select", `crl-select-type-${type}`, { "crl-select--active": visible });
  const optionsClassName = cn("crl-select--options", { "crl-select--options-visible": visible, "crl-select--options-custom": children });

  const toggleSelect = () => setVisible(!visible);
  const closeSelect = () => setVisible(false);
  const onChangeSelect = (option: SelectOption) => () => onChange(option);
  const selectedOption = _.find(options, { value }) || options[0] || { label: "", value: "" };
  const label = `${title}${selectedOption.label.length > 0 ? ":" : ""} ${selectedOption.label}`;
  const renderIcon = () => icon || <CaretDownIcon />;

  return (
    <div className={classNames}>
      <OutsideEventHandler
        onOutsideClick={closeSelect}
      >
        <Button type={type} iconPosition={iconPosition} className="crl-select--label" onClick={toggleSelect} icon={renderIcon}>
          {label}
        </Button>
        {children ? (
          <div className={optionsClassName} id={id}>
            {children}
          </div>
        ) : (
          <ul className={optionsClassName} id={id}>
            {_.map(options, (option, index) => (
              <li key={index} className={cn("crl-select--item", { "crl-select--item-active": option.value === value})} onClick={onChangeSelect(option)}>
                {option.label}
              </li>
            ))}
          </ul>
        )}
      </OutsideEventHandler>
    </div>
  );
}

Select.defaultProps = {
  type: "secondary",
  iconPosition: "right",
  options: [],
};
