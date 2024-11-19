// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { CaretDownOutlined } from "@ant-design/icons";
import cn from "classnames";
import React from "react";

import { Button } from "src/components/button";

import { OutsideEventHandler } from "../outsideEventHandler";

import "./dropdown.styl";

export interface Item {
  value: string;
  name: React.ReactNode | string;
  disabled?: boolean;
}

export interface DropdownProps {
  items: Array<Item>;
  onChange: (item: Item["value"]) => void;
  children?: React.ReactNode;
  dropdownToggleButton?: () => React.ReactNode;
  className?: string;
  menuPlacement?: "right" | "left";
}

interface DropdownState {
  isOpen: boolean;
}

interface DropdownButtonProps {
  children: React.ReactNode;
  isOpen: boolean;
  onClick?: (event: React.MouseEvent<HTMLElement>) => void;
}

function DropdownButton(props: DropdownButtonProps) {
  const { children } = props;
  return (
    <Button
      type="flat"
      size="small"
      iconPosition="right"
      icon={() => <CaretDownOutlined className="collapse-toggle__icon" />}
    >
      {children}
    </Button>
  );
}

export class Dropdown extends React.Component<DropdownProps, DropdownState> {
  state = {
    isOpen: false,
  };

  handleMenuOpen = () => {
    this.setState({
      isOpen: !this.state.isOpen,
    });
  };

  changeMenuState = (nextState: boolean) => {
    this.setState({
      isOpen: nextState,
    });
  };

  handleItemSelection = (value: string) => {
    this.props.onChange(value);
    this.handleMenuOpen();
  };

  renderDropdownToggleButton = () => {
    const { children, dropdownToggleButton } = this.props;
    const { isOpen } = this.state;

    if (dropdownToggleButton) {
      return dropdownToggleButton();
    } else {
      return <DropdownButton isOpen={isOpen}>{children}</DropdownButton>;
    }
  };

  render() {
    const { items, className, menuPlacement = "left" } = this.props;
    const { isOpen } = this.state;

    const menuStyles = cn(
      "crl-dropdown__menu",
      {
        "crl-dropdown__menu--open": isOpen,
      },
      `crl-dropdown__menu--placement-${menuPlacement}`,
    );

    const menuItems = items.map((menuItem, idx) => (
      <DropdownItem
        value={menuItem.value}
        onClick={this.handleItemSelection}
        key={idx}
        disabled={menuItem.disabled}
      >
        {menuItem.name}
      </DropdownItem>
    ));

    return (
      <div className={`crl-dropdown ${className}`}>
        <OutsideEventHandler onOutsideClick={() => this.changeMenuState(false)}>
          <div className="crl-dropdown__handler" onClick={this.handleMenuOpen}>
            {this.renderDropdownToggleButton()}
          </div>
          <div className="crl-dropdown__overlay">
            <div className={menuStyles}>
              <div className="crl-dropdown__container">{menuItems}</div>
            </div>
          </div>
        </OutsideEventHandler>
      </div>
    );
  }
}

export interface DropdownItemProps {
  children: React.ReactNode;
  value: string;
  onClick: (value: string) => void;
  disabled?: boolean;
}

export function DropdownItem(props: DropdownItemProps) {
  const { children, value, onClick, disabled = false } = props;
  const onClickHandler = React.useCallback(
    () => !disabled && onClick(value),
    [disabled, onClick, value],
  );
  return (
    <div
      onClick={onClickHandler}
      className={cn("crl-dropdown__item", {
        "crl-dropdown__item--disabled": disabled,
      })}
    >
      {children}
    </div>
  );
}
