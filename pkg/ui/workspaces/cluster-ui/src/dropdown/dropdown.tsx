// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { CaretDown } from "@cockroachlabs/icons";
import classnames from "classnames/bind";
import React, { useState, useCallback } from "react";

import { Button, ButtonProps } from "src/button";

import { OutsideEventHandler } from "../outsideEventHandler";

import styles from "./dropdown.module.scss";

const cx = classnames.bind(styles);

export interface DropdownOption<T = string> {
  value: T;
  name: React.ReactNode | string;
  disabled?: boolean;
}

export interface DropdownItemProps<T> {
  children: React.ReactNode;
  value: T;
  onClick: (value: T) => void;
  disabled?: boolean;
  className?: string;
}
export interface DropdownProps<T> {
  items: Array<DropdownOption<T>>;
  onChange: (item: DropdownOption<T>["value"]) => void;
  children?: React.ReactNode;
  customToggleButton?: React.ReactChild;
  customToggleButtonOptions?: Partial<ButtonProps>;
  menuPosition?: "left" | "right";
  className?: string;
  itemsClassname?: string;
}

interface DropdownButtonProps {
  children: React.ReactNode;
  isOpen: boolean;
  onClick?: (event: React.MouseEvent<HTMLElement>) => void;
  customProps?: Partial<ButtonProps>;
}

export const DropdownButton: React.FC<DropdownButtonProps> = ({
  children,
  customProps = {},
}) => {
  return (
    <Button
      type="secondary"
      size="default"
      iconPosition="right"
      icon={<CaretDown />}
      {...customProps}
    >
      {children}
    </Button>
  );
};

function DropdownItem<T = string>(props: DropdownItemProps<T>) {
  const { children, value, onClick, disabled, className } = props;
  return (
    <div
      onClick={() => onClick(value)}
      className={cx(
        "crl-dropdown__item",
        {
          "crl-dropdown__item--disabled": disabled,
        },
        className,
      )}
    >
      {children}
    </div>
  );
}

export function Dropdown<T = string>({
  items,
  onChange,
  children,
  customToggleButton,
  customToggleButtonOptions,
  menuPosition = "left",
  className,
  itemsClassname,
}: DropdownProps<T>): React.ReactElement {
  const [isOpen, setIsOpen] = useState(false);

  const handleMenuOpen = useCallback((): void => {
    setIsOpen(prev => !prev);
  }, []);

  const changeMenuState = useCallback((nextState: boolean): void => {
    setIsOpen(nextState);
  }, []);

  const handleItemSelection = useCallback(
    (value: T): void => {
      onChange(value);
      setIsOpen(prev => !prev);
    },
    [onChange],
  );

  const renderDropdownToggleButton = (): React.ReactChild => {
    if (customToggleButton) {
      return customToggleButton;
    } else {
      return (
        <DropdownButton isOpen={isOpen} customProps={customToggleButtonOptions}>
          {children}
        </DropdownButton>
      );
    }
  };

  const menuStyles = cx(
    "crl-dropdown__menu",
    `crl-dropdown__menu--align-${menuPosition}`,
    {
      "crl-dropdown__menu--open": isOpen,
    },
  );

  const menuItems = items.map((menuItem, idx) => (
    <DropdownItem<T>
      value={menuItem.value}
      onClick={handleItemSelection}
      key={idx}
      disabled={menuItem.disabled}
      className={itemsClassname}
    >
      {menuItem.name}
    </DropdownItem>
  ));

  return (
    <div className={cx("crl-dropdown", className)}>
      <OutsideEventHandler onOutsideClick={() => changeMenuState(false)}>
        <div className={cx("crl-dropdown__handler")} onClick={handleMenuOpen}>
          {renderDropdownToggleButton()}
        </div>
        <div className={cx("crl-dropdown__overlay")}>
          <div className={menuStyles}>
            <div className={cx("crl-dropdown__container")}>{menuItems}</div>
          </div>
        </div>
      </OutsideEventHandler>
    </div>
  );
}
