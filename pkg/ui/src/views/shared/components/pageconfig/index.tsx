import React from "react";

export function PageConfig(props: {children?: React.ReactNode}) {
  return (
    <div className="page-config">
      <ul className="page-config__list">
        { props.children }
      </ul>
    </div>
  );
}

export function PageConfigItem(props: {children?: React.ReactNode}) {
  return (
    <li className="page-config__item">
      { props.children }
    </li>
  );
}
