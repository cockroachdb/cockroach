import React from "react";

interface RowCellProps {
  cellClasses: string;
  children: string | React.ReactNode;
}

export const RowCell: React.FC<RowCellProps> = ({ cellClasses, children }) => (
  <td className={cellClasses}>{children}</td>
);
