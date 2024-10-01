// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/**
 * @fileoverview Restrict direct child elements of compound components.
 * Compound components are components that export sub-components. The sub-components
 * are exported with the compound component name as a property. For example:
 *
 * export default function InfoCard({ children }) {
 *    return <div className="info-card">{children}</div>;
 *  }
 *
 * InfoCard.Section = function Section({ children }) {
 *    return <div className="section">{children}</div>;
 * };
 *
 * These components make it easier to structure layouts, but often custom CSS and layouts
 * can sneak into the top level ones. This rule prevents non-compound children from being
 * used with the top-level components. This helps avoid scenarios like Modals being not using
 * their correct footer and instead, custom action containers. Doing so helps keep more consistent
 * layouts.
 *
 * Note: This was done through a linter, due to JSX components only exporting with a type of
 * JSX.Element. This cannot be differentiated with Typescript. We could also do runtime checks,
 * but doing so makes it verbose in the components themselves. A linter hits a good middle ground
 * of catching these issues before commits.
 *
 * See: https://stackoverflow.com/questions/57627929/only-allow-specific-components-as-children-in-react-and-typescript
 * (Stack overflow link has a comment on the first answer describing why this doesn't work.)
 */
module.exports = {
  "restricted-compound-components": {
    create: function (context) {
      // NOTE: DashboardHeader is intentionally not included here, to allow more
      // flexible styling for internal use-cases.
      const compoundComponents = new Set(["InfoCard", "Section", "Summary"]);
      return {
        JSXElement(node) {
          const componentName = node.openingElement.name.name;
          if (!compoundComponents.has(componentName)) return;

          node.children.forEach((child) => {
            if (child.type !== "JSXElement") return;

            const isSubComponent =
              child.openingElement.name.object &&
              child.openingElement.name.object.name === componentName;

            if (isSubComponent) return;

            context.report({
              node: child,
              message: `Direct child elements of ${componentName} are not allowed. Use sub-components exported with ${componentName} instead.`,
            });
          });
        },
      };
    },
  },
};
