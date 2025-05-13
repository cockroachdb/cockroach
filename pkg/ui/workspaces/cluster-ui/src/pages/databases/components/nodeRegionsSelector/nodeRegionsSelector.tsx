// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { WarningOutlined } from "@ant-design/icons";
import { Divider, Select } from "antd";
import classNames from "classnames";
import React, { useMemo, useState } from "react";

import { useNodeStatuses } from "src/api";
import { Button } from "src/button";
import { applyBtn } from "src/queryFilter/filterClasses";
import { NodeID } from "src/types/clusterTypes";

import styles from "./nodeRegionsSelector.module.scss";

const ALL_LABEL = "All";

type NodeRegionsSelectorProps = {
  value?: string[];
  onChange: (selected: string[]) => void;
  includeAllOption?: boolean;
};

// The NodeRegionsSelector component is a component for selecting nodes
// within anything which imports the cluster-ui. It provides a dropdown
// which groups nodes by region, and optionally allows for selection
// of all of the nodes in the cluster.
export const NodeRegionsSelector: React.FC<NodeRegionsSelectorProps> = ({
  value: appliedNodes = [],
  onChange,
  includeAllOption,
}) => {
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const { nodeStatusByID } = useNodeStatuses();
  const [selectedNodes, setSelectedNodes] = useState<NodeID[]>(
    appliedNodes.map(n => parseInt(n) as NodeID),
  );

  // Compute a region to list of node ids mapping for selection grouping.
  const regions: Record<string, NodeID[]> = useMemo(() => {
    const optionsMap: Record<string, NodeID[]> = {};
    const nodeIds = Object.keys(nodeStatusByID ?? {});

    nodeIds.forEach(node => {
      const nid = parseInt(node) as NodeID;
      const region = nodeStatusByID[nid].region;
      if (optionsMap[region] == null) {
        optionsMap[region] = [];
      }
      optionsMap[region].push(nid);
    });
    return optionsMap;
  }, [nodeStatusByID]);

  const nodeIds: NodeID[] = useMemo(() => {
    const nodeIds = Object.keys(nodeStatusByID ?? {});

    return nodeIds.map(node => parseInt(node) as NodeID);
  }, [nodeStatusByID]);

  const onSelect = (...values: NodeID[]) => {
    const deduplicated = [...new Set([...selectedNodes, ...values])];
    setSelectedNodes(deduplicated);
  };

  const onDeselect = (...values: NodeID[]) => {
    setSelectedNodes(selectedNodes.filter(n => !values.includes(n)));
  };

  const onDeselectTag = (value: NodeID) => {
    const updatedNodes = selectedNodes.filter(n => n !== value);
    setSelectedNodes(updatedNodes);
    onChange(updatedNodes.map(n => n.toString()));
  };

  const onApply = () => {
    setDropdownOpen(false);
    onChange(selectedNodes.map(n => n.toString()));
  };

  const onClear = () => {
    setSelectedNodes([]);
    onChange([]);
  };

  // render the all tag only if all nodes are selected.
  const selection =
    appliedNodes.length === nodeIds.length && includeAllOption
      ? [ALL_LABEL]
      : appliedNodes.map(n => ({ label: `n${n}`, value: n }));

  return (
    <Select
      name="nodeRegions"
      className={styles["selector"]}
      placeholder={"Select Nodes"}
      mode="multiple"
      allowClear
      popupMatchSelectWidth={false}
      open={dropdownOpen}
      onDropdownVisibleChange={setDropdownOpen}
      value={selection}
      maxTagCount={6}
      onClear={onClear}
      onDeselect={onDeselectTag}
      dropdownRender={() => (
        <div>
          {/* Option to select all nodes */}
          {includeAllOption && (
            <div>
              <GroupCheckboxOption
                label={ALL_LABEL}
                selected={selectedNodes}
                options={nodeIds}
                onSelect={onSelect}
                onDeselect={onDeselect}
              >
                {/* Warning message for all nodes */}
                <div className={styles.warningAllNodes}>
                  <WarningOutlined className={styles.warningIcon} />
                  <span className={styles.warningAllText}>
                    Might take a longer time to load
                  </span>
                </div>
              </GroupCheckboxOption>
              <Divider className={styles.divider} />
            </div>
          )}
          <div className={styles.regionsSection}>
            {Object.entries(regions).map(([region, group]) => {
              return (
                <div key={region} className={styles.regionSelector}>
                  {/* Option to select whole region */}
                  <GroupCheckboxOption
                    key={region}
                    label={region}
                    selected={selectedNodes}
                    options={group}
                    onSelect={onSelect}
                    onDeselect={onDeselect}
                  />
                  {/* Option for each node in region */}
                  {group.map((node: NodeID) => (
                    <CheckboxOption
                      className={styles.nodeOption}
                      key={node}
                      label={`n${node}`}
                      isSelected={selectedNodes.includes(node)}
                      onSelect={() => onSelect(node)}
                      onDeselect={() => onDeselect(node)}
                    />
                  ))}
                </div>
              );
            })}
          </div>
          <Divider className={styles.divider} />
          {/* Apply button */}
          <div className={styles.applyBtnContainer}>
            <Button
              className={`${applyBtn.btn}} ${styles.applyBtn}`}
              textAlign="center"
              onClick={onApply}
              disabled={selectedNodes.length === 0}
            >
              Apply
            </Button>
          </div>
        </div>
      )}
    ></Select>
  );
};

// The CheckboxOption is a simple utility component for rendering
// the options in the above selector as a checkbox.
interface CheckboxOptionProps {
  className?: string;
  label: string;
  isSelected: boolean;
  onSelect: () => void;
  onDeselect: () => void;
}
const CheckboxOption = (props: CheckboxOptionProps) => {
  const isSelected = props.isSelected;
  const onClick = isSelected ? props.onDeselect : props.onSelect;
  return (
    <div
      className={classNames(styles["option"], props.className)}
      onClick={onClick}
    >
      {/* set readOnly to satisfy the property checker */}
      <input type="checkbox" checked={isSelected} readOnly />
      <label>{props.label}</label>
    </div>
  );
};

// The GroupCheckboxOption is another utility component for the above
// selector, renders a checkbox as an option for selecting an entire
// group. Note that the children parameter in this component are for
// rendering additional components as part of this option, and not
// options which are considered part of the group.
interface GroupCheckboxOptionProps<T> {
  label: string;
  options: T[];
  selected: T[];
  onSelect: (...t: T[]) => void;
  onDeselect: (...t: T[]) => void;
  children?: React.ReactNode;
}
const GroupCheckboxOption = <T,>({
  label,
  options,
  selected,
  onSelect,
  onDeselect,
  children,
}: GroupCheckboxOptionProps<T>) => {
  const isAllSelected = options.every(o => selected.includes(o));

  return (
    <div>
      <CheckboxOption
        label={label}
        isSelected={isAllSelected}
        onSelect={() => onSelect(...options)}
        onDeselect={() => onDeselect(...options)}
      />
      {children}
    </div>
  );
};
