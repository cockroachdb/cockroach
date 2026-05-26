// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Input } from "antd";
import React, { useState, useRef, useEffect } from "react";

interface EditableTitleProps {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  className?: string;
  size?: "small" | "medium" | "large";
}

const EditableTitle: React.FC<EditableTitleProps> = ({
  value,
  onChange,
  placeholder = "Enter title...",
  className = "",
  size = "medium",
}) => {
  const [isEditing, setIsEditing] = useState(false);
  const [editValue, setEditValue] = useState(value);
  const [isHovered, setIsHovered] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (isEditing && inputRef.current) {
      inputRef.current.focus();
      inputRef.current.select();
    }
  }, [isEditing]);

  useEffect(() => {
    setEditValue(value);
  }, [value]);

  const handleClick = () => {
    setIsEditing(true);
    setEditValue(value);
  };

  const handleBlur = () => {
    if (editValue.trim() !== value) {
      onChange(editValue.trim() || placeholder);
    }
    setIsEditing(false);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      e.preventDefault();
      if (editValue.trim() !== value) {
        onChange(editValue.trim() || placeholder);
      }
      setIsEditing(false);
    } else if (e.key === "Escape") {
      setEditValue(value);
      setIsEditing(false);
    }
  };

  const sizeStyles = {
    small: { fontSize: "14px", padding: "2px 6px" },
    medium: { fontSize: "16px", padding: "4px 8px" },
    large: { fontSize: "20px", padding: "6px 10px", fontWeight: 500 },
  };

  const baseStyle: React.CSSProperties = {
    display: "inline-flex",
    alignItems: "center",
    gap: "8px",
    cursor: "pointer",
    borderRadius: "4px",
    transition: "all 0.2s ease",
    userSelect: "none",
    maxWidth: "100%",
  };

  const hoverStyle: React.CSSProperties = {
    backgroundColor: "rgba(0, 0, 0, 0.02)",
  };

  if (isEditing) {
    return (
      <Input
        ref={inputRef}
        value={editValue}
        onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
          setEditValue(e.target.value)
        }
        onBlur={handleBlur}
        onKeyDown={handleKeyDown}
        placeholder={placeholder}
        style={{
          ...sizeStyles[size],
          border: "1px solid #d9d9d9",
          borderRadius: "4px",
        }}
        className={className}
        bordered={false}
      />
    );
  }

  return (
    <div
      className={className}
      onClick={handleClick}
      role="button"
      tabIndex={0}
      onKeyDown={(e: React.KeyboardEvent) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          handleClick();
        }
      }}
      style={{
        ...baseStyle,
        ...sizeStyles[size],
        ...(isHovered ? hoverStyle : {}),
      }}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      <span
        style={{
          flex: 1,
          minWidth: 0,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
      >
        {value || (
          <span style={{ color: "#bfbfbf", fontStyle: "italic" }}>
            {placeholder}
          </span>
        )}
      </span>
      <span
        style={{
          opacity: isHovered ? 0.6 : 0,
          transition: "opacity 0.2s ease",
          color: "#8c8c8c",
          fontSize: "0.9em",
          flexShrink: 0,
        }}
      >
        ✎
      </span>
    </div>
  );
};

export default EditableTitle;
