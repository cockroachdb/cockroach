// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Divider, Select } from "antd";
import classNames from "classnames";
import React, { useEffect, useMemo, useState } from "react";

import { useNodeStatuses } from "src/api";
import { NodeStatus } from "src/api/nodesApi";
import { Button } from "src/button";
import { applyBtn } from "src/queryFilter/filterClasses";
import { NodeID } from "src/types/clusterTypes";

import styles from "./nodeSelector.module.scss";

const ALL_LABEL = "All";

type NodeSelectorProps = {
  onChange: (nodeIDs: NodeID[], nodes: NodeStatus[]) => void;
  initialValue?: NodeID[];
  allWarning?: React.ReactNode;
};

// The NodeSelector component is a component for selecting nodes
// within anything which imports the cluster-ui. It provides a grouped
// dropdown for the nodes in the cluster, in which nodes with a region
// are grouped by their region, and are selectable either individually,
// by a region, or all at once.
export const NodeSelector: React.FC<NodeSelectorProps> = ({
  initialValue = [],
  onChange: parentOnChange,
  allWarning,
}) => {
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const { nodeStatusByID } = useNodeStatuses();

  // A note on the difference between selectedNodes and appliedNodes.
  // This component has two sets of nodes which can be thought of as
  // "selected". The first (selectedNodes) is those which appear
  // checked in the dropdown, which the user is currently in the
  // process of choosing but have not yet been passed to the parent.
  // The second are the ones which have been passed to the parent
  // which are rendered as tags within the input section of the dropdown.
  const [selectedNodes, setSelectedNodes] = useState<NodeID[]>(initialValue);
  const [appliedNodes, setAppliedNodes] = useState<NodeID[]>(initialValue);

  // Because this component has custom group rendering and selection
  // the rendered options and group selection require manual filtering.
  const [search, setSearch] = useState<string>();

  // Compute a region to list of node ids mapping for selection grouping.
  const nodesByRegion: Record<string, NodeID[]> = useMemo(() => {
    const optionsMap: Record<string, NodeID[]> = {};
    for (const [id, node] of Object.entries(nodeStatusByID)) {
      const region = node.region;
      if (optionsMap[region] == null) {
        optionsMap[region] = [];
      }
      optionsMap[region].push(parseInt(id, 10) as NodeID);
    }
    return optionsMap;
  }, [nodeStatusByID]);

  // Separate regionless nodes into their own list.
  const regionlessNodes =
    nodesByRegion[""]?.filter((id: NodeID) => matchesFilter(id, search)) || [];

  const nodeIds: NodeID[] = useMemo(() => {
    const nodeIds = Object.keys(nodeStatusByID ?? {});

    return nodeIds.map(node => parseInt(node) as NodeID);
  }, [nodeStatusByID]);

  const onSelect = (...values: NodeID[]) => {
    setSearch("");
    const deduplicated = [...new Set([...selectedNodes, ...values])];
    setSelectedNodes(deduplicated);
  };

  const onDeselect = (...values: NodeID[]) => {
    setSearch("");
    setSelectedNodes(selectedNodes.filter(n => !values.includes(n)));
  };

  const onChange = (value: NodeID[]) => {
    const nodes = value.map((id: NodeID) => nodeStatusByID[id]);
    setAppliedNodes(value);
    parentOnChange(value, nodes);
  };

  const onDeselectTag = (value: NodeID) => {
    // When the "All" tag is deselected, we clear all selected nodes.
    const updatedNodes =
      (value as unknown as string) === ALL_LABEL
        ? []
        : selectedNodes.filter(n => n !== value);

    setSelectedNodes(updatedNodes);
    onChange(updatedNodes);
  };

  const onApply = () => {
    setSearch("");
    setDropdownOpen(false);
    onChange(selectedNodes);
  };

  const onClear = () => {
    setSelectedNodes([]);
    onChange([]);
  };

  // render the all tag only if there are options and all nodes are selected.
  const tags =
    nodeIds.length && appliedNodes.length === nodeIds.length
      ? [ALL_LABEL]
      : appliedNodes.map(n => ({ label: `n${n}`, value: n }));

  // Only show the all option if all options are visible.
  const showAllOption =
    nodeIds.filter((id: NodeID) => matchesFilter(id, search)).length ===
    nodeIds.length;

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
      value={tags}
      maxTagCount={6}
      onClear={onClear}
      onDeselect={onDeselectTag}
      onSearch={setSearch}
      dropdownRender={() => (
        <div>
          {/* Option to select all nodes */}
          {showAllOption && (
            <div>
              <GroupCheckboxOption
                label={ALL_LABEL}
                selected={selectedNodes}
                options={nodeIds}
                onSelect={onSelect}
                onDeselect={onDeselect}
              >
                {/* Warning message for all nodes */}
                {allWarning}
              </GroupCheckboxOption>
              <Divider className={styles.divider} />
            </div>
          )}
          <div className={styles.regionsSection}>
            {/* Add regionless nodes on their own. */}
            {regionlessNodes.map((node: NodeID) => (
              <CheckboxOption
                key={node}
                label={`n${node}`}
                checked={selectedNodes.includes(node)}
                onSelect={() => onSelect(node)}
                onDeselect={() => onDeselect(node)}
              />
            ))}

            {/* Finally add nodes grouped by region. */}
            {Object.entries(nodesByRegion)
              .filter(([region, _]) => !!region) // filter out regionless nodes
              .map(([region, group]) => {
                const filteredOptions = group.filter((id: NodeID) =>
                  matchesFilter(id, search),
                );
                const showGroupOption = filteredOptions.length === group.length;

                return (
                  <div key={region} className={styles.regionSelector}>
                    {/* Option to select whole region */}
                    {showGroupOption && (
                      <GroupCheckboxOption
                        key={region}
                        label={region}
                        selected={selectedNodes}
                        options={group}
                        onSelect={onSelect}
                        onDeselect={onDeselect}
                      />
                    )}
                    {/* Option for each node in region */}
                    {filteredOptions.map((node: NodeID) => (
                      <CheckboxOption
                        className={styles.regionNodeOption}
                        key={node}
                        label={`n${node}`}
                        checked={selectedNodes.includes(node)}
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
  checked: boolean;
  indeterminate?: boolean;
  onSelect: () => void;
  onDeselect: () => void;
}
const CheckboxOption = ({
  className,
  label,
  checked,
  indeterminate,
  onSelect,
  onDeselect,
}: CheckboxOptionProps) => {
  const isSelected = checked;
  const onClick = isSelected ? onDeselect : onSelect;
  const checkboxRef = React.useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (checkboxRef.current) {
      checkboxRef.current.indeterminate = indeterminate || false;
    }
  }, [indeterminate]);

  return (
    <div className={classNames(styles["option"], className)} onClick={onClick}>
      <input type="checkbox" ref={checkboxRef} checked={isSelected} readOnly />
      <label>{label}</label>
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
  const isSomeSelected = options.some(o => selected.includes(o));

  return (
    <div>
      <CheckboxOption
        label={label}
        checked={isAllSelected}
        indeterminate={isSomeSelected && !isAllSelected}
        onSelect={() => onSelect(...options)}
        onDeselect={() => onDeselect(...options)}
      />
      {children}
    </div>
  );
};

function matchesFilter(id: NodeID, search: string): boolean {
  if (!search) {
    return true;
  }
  return `n${id}`.includes(search);
}
