import React, { useState } from "react";
import { Select, Tag } from "antd";
import classNames from "classnames/bind";

import { useSelector } from "react-redux";

import { nodeIDsStringifiedSelector } from "src/redux/nodes";

import styles from "./hotRanges.module.styl";

const cx = classNames.bind(styles);

const ALL_KEY = "all";
const ALL_OPTION = { label: "All (⚠ may be slow)", value: ALL_KEY };

export default function NodeIDSelector(props: NodeIDSelectorProps) {
  const { node_ids, onSubmit } = props;
  const [nodes, setNodes] = useState<string[]>(node_ids);
  const options = [ALL_OPTION].concat(useSelector(nodeIDsStringifiedSelector).map(n => ({
    value: n,
    label: "N" + n,
  })));

  const onSelect = (value: string) => {
    if (value === ALL_KEY) {
      setNodes(options.map(o => o.value));
    } else {
      if (nodes.length === options.length - 2) {
        setNodes([ALL_KEY, value, ...nodes]);
      } else {
        setNodes([value, ...nodes]);
      }
    }
  }

  const onDeselect = (value: string) => {
    if (value === ALL_KEY) {
      setNodes([]);
    } else {
      // strip the value, and the all option if it's selected.
      setNodes(nodes.filter(n => ![ALL_KEY, value].includes(n)))
    }
  }

  const onBlur = () => {
    onSubmit(nodes.filter(n => n !== ALL_KEY));
  };

  const onClear = () => {
    setNodes([]);
    onSubmit([]);
  };

  return (
    <div>
      <Select
        mode="multiple"
        className="select__container"
        optionLabelProp="name"
        options={options.map(o => ({
          value: o.value,
          name: o.label,
          label: <CheckboxOption label={o.label} checked={nodes.includes(o.value)} />,
        }))}
        placeholder="Select Nodes"
        value={nodes}
        popupMatchSelectWidth={false}
        maxTagCount={5}
        allowClear={true}
        onSelect={onSelect}
        onDeselect={onDeselect}
        onBlur={onBlur}
        onClear={onClear}
        tagRender={WithoutAllTag}
        optionRender={CheckboxOption}
      />
    </div>
  );
}

type NodeIDSelectorProps = {
  node_ids: string[];
  setNodes: (nodes: string[]) => void;
  onSubmit: (nodes: string[]) => void;
};

// WithoutAllTag is an override which hides the all tag from the
// selected tag list.
const WithoutAllTag = ({ label, value }: { label: string; value: string }) => {
  if (value === "all") {
    return null;
  }
  return <Tag>{label}</Tag>;
}

const CheckboxOption = ({ label, checked }: any) => {
  return (
    <span>
      <input
        className={cx("checkbox-option-input")}
        type="checkbox"
        checked={checked}
        onChange={() => null}
      />
      <label className={cx("checkbox-option-label")}>{label}</label>
    </span>
  );
};

