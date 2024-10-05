// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import type { TSESLint } from "@typescript-eslint/utils";
import { TSESTree } from "@typescript-eslint/utils";

type MessageIds = "missingImport";

const requireAntdStyleImport: TSESLint.RuleModule<MessageIds> = {
  meta: {
    type: "problem",
    messages: {
      missingImport: "antd component '{{ imported }}' imported without corresponding style '{{ style }}'",
    },
    docs: {
      description: "require antd style import with antd component import",
      recommended: "error",
    },
    fixable: "code",
    schema: [],
  },
  create: function(ctx) {
    let importedComponents: TSESTree.ImportClause[] = [];
    const importedStyles = new Set<string>();

    const STYLE_IMPORT_PATTERN = /^antd\/lib\/.*\/style$/;

    return {
      ImportDeclaration(node): void {
        // Ignore type-only imports, since they don't have an effect at runtime.
        if (node.importKind === "type") {
          return;
        }

        // Gather all components imported directly from antd for evaluation when the Program exits.
        if (node.source.value === "antd") {
          importedComponents = [
            ...importedComponents,
            ...node.specifiers,
          ];
          return;
        }

        // Gather all imports that look like bare style imports
        if (STYLE_IMPORT_PATTERN.test(node.source.value) && node.specifiers.length === 0) {
          importedStyles.add(node.source.value);
          return;
        }

        // Ignore imports from anything other than antd or nested imports.
        return;
      },
      "Program:exit": function(_node): void {
        // After the entire Program has finished parsing, ensure all imported components have a
        // corresponding style import.
        importedComponents
          .filter(isImportSpecifier) // Don't test default or namespace imports
          .forEach((clause) => {
            const name = clause.imported.name;
            const expectedImport = `antd/lib/${toSnakeCase(name)}/style`;

            if (importedStyles.has(expectedImport)) {
              return;
            }

            ctx.report({
              messageId: "missingImport",
              node: clause,
              data: {
                imported: name,
                style: expectedImport,
              },
              fix: function(fixer) {
                return fixer.insertTextAfter(
                  clause.parent!,
                  `\nimport "${expectedImport}";`
                );
              },
            });
          });
      },
    };
  },
};

export function toSnakeCase(pascal: string): string {
  const tmp = pascal[0].toLowerCase() + pascal.substring(1);
  return tmp.replace(/([A-Z])/g, (cap) => `-${cap.toLowerCase()}`);
}

function isImportSpecifier(clause: TSESTree.ImportClause): clause is TSESTree.ImportSpecifier {
  return clause.type === TSESTree.AST_NODE_TYPES.ImportSpecifier;
}

export default requireAntdStyleImport;

