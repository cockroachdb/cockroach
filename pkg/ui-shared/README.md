# Admin UI Shared

Admin UI Shared (`admin-ui-shared`) is a component library for the Admin UI. It contains components used in Cockroach Cloud.

```
  npm install --save-dev @cockroachlabs/admin-ui-shared
```

Components are exported individually from the package,

```javascript
import { Badge } from "@cockroachlabs/admin-ui-shared";

export default props => (
  <div>
    <Badge>Admin UI Badge</Badge>
  </div>
);
```

## Style Guide for Components

Components in this repo will follow some strict rules in order to keep them isolated and simple:

1. One component per file (no exceptions. yes, this means many files.)
2. Only SASS used for styles (no stylus. moving stylus-based components from Admin UI requires conversion)
