import * as React from "react";
import { RouteComponentProps } from "react-router";

export interface TitledComponent {
  title(routeProps: RouteComponentProps<any, any>): React.ReactElement<any>;
}
