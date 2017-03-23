import * as React from "react";
import { RouterProps } from "react-router";

export interface TitledComponent {
  title(routeProps: RouterProps): React.ReactElement<any>;
}
