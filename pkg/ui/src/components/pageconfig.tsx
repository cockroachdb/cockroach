import * as React from "react";
import { Sticky } from "react-sticky";

export function PageConfig(props: {children?: React.ReactNode}) {
    return <Sticky className="page-config" stickyClassName="page-config--fixed">
      <ul className="page-config__list">
        { props.children }
      </ul>
    </Sticky>;
}

export function PageConfigItem(props: {children?: React.ReactNode}) {
    return <li className="page-config__item">
        { props.children }
    </li>;
}
