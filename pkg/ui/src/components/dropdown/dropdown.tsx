// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import cn from "classnames";

import { OutsideEventHandler } from "../outsideEventHandler";
import "./dropdown.styl";
import { Icon } from "antd";
import { Button } from "src/components/button";

export interface Item {
  value: string;
  name: React.ReactNode | string;
}

export interface DropdownProps {
  items: Array<Item>;
  onChange: (item: Item["value"]) => void;
  children: React.ReactNode;
  dropdownToggleButton?: () => React.ReactNode;
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
      icon={() => (
        <Icon
          className="collapse-toggle__icon"
          type="caret-down" />
      )}
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
  }

  changeMenuState = (nextState: boolean) => {
    this.setState({
      isOpen: nextState,
    });
  }

  handleItemSelection = (value: string) => {
    this.props.onChange(value);
    this.handleMenuOpen();
  }

  renderDropdownToggleButton = () => {
    const { children, dropdownToggleButton } = this.props;
    const { isOpen } = this.state;

    if (dropdownToggleButton) {
      return dropdownToggleButton();
    } else {
      return (
        <DropdownButton
          isOpen={isOpen}
        >
          {children}
        </DropdownButton>
      );
    }
  }

  render() {
    const { items } = this.props;
    const { isOpen } = this.state;

    const menuStyles = cn(
      "crl-dropdown__menu",
      {
        "crl-dropdown__menu--open": isOpen,
      },
    );

    const menuItems = items.map((menuItem, idx) => (
      <DropdownItem
        value={menuItem.value}
        onClick={this.handleItemSelection}
        key={idx}
      >
        { menuItem.name }
      </DropdownItem>
    ));

    return (
      <div className="crl-dropdown">
        <OutsideEventHandler
          onOutsideClick={() => this.changeMenuState(false)}
        >
          <div
            className="crl-dropdown__handler"
            onClick={this.handleMenuOpen}>
            { this.renderDropdownToggleButton() }
          </div>
          <div className="crl-dropdown__overlay">
            <div className={menuStyles}>
              <div className="crl-dropdown__container">
                {menuItems}
              </div>
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
}

export function DropdownItem(props: DropdownItemProps) {
  const { children, value, onClick } = props;
  return (
    <div
      onClick={() => onClick(value)}
      className="crl-dropdown__item"
    >
      { children }
    </div>
  );
}
