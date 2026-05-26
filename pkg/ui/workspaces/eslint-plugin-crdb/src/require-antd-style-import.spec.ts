// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { TSESLint } from "@typescript-eslint/utils";

import rule from "./require-antd-style-import";

const ruleTester = new TSESLint.RuleTester({
  parser: "@typescript-eslint/parser",
});

ruleTester.run("require-antd-style-import", rule, {
  valid: [
    {
      name: "single import statement",
      code: `
        import { Col } from "antd";
        import "antd/lib/col/style";
      `,
    },
    {
      name: "multiple import statements",
      code: `
        import { Col } from "antd";
        import "antd/lib/col/style";
        import { Row } from "antd";
        import "antd/lib/row/style";
      `,
    },
    {
      name: "multiple values in one import statement",
      code: `
        import { Col, Row } from "antd";
        import "antd/lib/col/style";
        import "antd/lib/row/style";
      `,
    },
    {
      name: "renamed import",
      code: `
        import { Col as AntdCol } from "antd";
        import "antd/lib/col/style";
      `,
    },
    {
      name: "PascalCase import",
      code: `
        import { PageHeader } from "antd";
        import "antd/lib/page-header/style";
      `,
    },
    {
      name: "PascalCase renamed import",
      code: `
        import { PageHeader as AntdPageHeader } from "antd";
        import "antd/lib/page-header/style";
      `,
    },
    {
      name: "out of order imports",
      code: `
        import "antd/lib/col/style";
        import { Table } from "antd";
        import "antd/lib/row/style";
        import { Row, Col as Column } from "antd";
        import "antd/lib/table/style";
      `,
    },
    {
      name: "type-only import with no style",
      code: `
        import type { Foo } from "antd";
      `,
    },
    {
      name: "nested import with no style",
      code: `
        import { CheckboxProps } from "antd/lib/checkbox";
      `,
    },
    {
      name: "default antd imports",
      code: `
        import ant_design from "antd";
      `,
    },
    {
      name: "non-antd imports",
      code: `
        import { useEffect } from "react";
      `,
    },
  ],
  invalid: [
    {
      name: "missing single import",
      code: `
//  0   0   1   1   2   2
//  5   9   3   7   1   5
import { Row } from "antd";
`.trim(),
      errors: [
        {
          messageId: "missingImport",
          data: {
            imported: "Row",
            style: "antd/lib/row/style",
          },
          line: 3,
          column: 10,
          endColumn: 13,
        }
      ],
      output: `
//  0   0   1   1   2   2
//  5   9   3   7   1   5
import { Row } from "antd";
import "antd/lib/row/style";
`.trim(),
    },
    {
      name: "missing multiple import statements",
      code: `
//  0   0   1   1   2   2
//  5   9   3   7   1   5
import { Row } from "antd";
import { Table } from "antd";
`.trim(),
      errors: [
        {
          messageId: "missingImport",
          data: {
            imported: "Row",
            style: "antd/lib/row/style",
          },
          line: 3,
          column: 10,
          endColumn: 13,
        },
        {
          messageId: "missingImport",
          data: {
            imported: "Table",
            style: "antd/lib/table/style",
          },
          line: 4,
          column: 10,
          endColumn: 15,
        }
      ],
      output: `
//  0   0   1   1   2   2
//  5   9   3   7   1   5
import { Row } from "antd";
import "antd/lib/row/style";
import { Table } from "antd";
import "antd/lib/table/style";
`.trim(),
    },
    {
      name: "missing import from multi-import",
      code: `
//  0   0   1   1   2   2
//  5   9   3   7   1   5
import { Row, Col } from "antd";
`.trim(),
      errors: [
        {
          messageId: "missingImport",
          data: {
            imported: "Row",
            style: "antd/lib/row/style",
          },
          line: 3,
          column: 10,
          endColumn: 13,
        },
        {
          messageId: "missingImport",
          data: {
            imported: "Col",
            style: "antd/lib/col/style",
          },
          line: 3,
          column: 15,
          endColumn: 18,
        }
      ],
      // eslint's RuleTester only applies a single pass of fixers for each AST node, so the second
      // fix (for "Col") is intentionally not included here.
      output: `
//  0   0   1   1   2   2
//  5   9   3   7   1   5
import { Row, Col } from "antd";
import "antd/lib/row/style";
`.trim(),
    },
    {
      name: "missing import from multi-import",
      code: `
//  0   0   1   1   2   2
//  5   9   3   7   1   5
import { Row, Col } from "antd";
`.trim(),
      errors: [
        {
          messageId: "missingImport",
          data: {
            imported: "Row",
            style: "antd/lib/row/style",
          },
          line: 3,
          column: 10,
          endColumn: 13,
        },
        {
          messageId: "missingImport",
          data: {
            imported: "Col",
            style: "antd/lib/col/style",
          },
          line: 3,
          column: 15,
          endColumn: 18,
        }
      ],
      // eslint's RuleTester only applies a single pass of fixers for each AST node, so the second
      // fix (for "Col") is intentionally not included here.
      output: `
//  0   0   1   1   2   2
//  5   9   3   7   1   5
import { Row, Col } from "antd";
import "antd/lib/row/style";
`.trim(),
    },
    {
      name: "missing renamed import",
      code: `
//  0   0   1   1   2   2
//  5   9   3   7   1   5
import { Row as AntdRow } from "antd";
`.trim(),
      errors: [
        {
          messageId: "missingImport",
          data: {
            imported: "Row",
            style: "antd/lib/row/style",
          },
          line: 3,
          column: 10,
          endColumn: 24,
        },
      ],
      output: `
//  0   0   1   1   2   2
//  5   9   3   7   1   5
import { Row as AntdRow } from "antd";
import "antd/lib/row/style";
`.trim(),
    },
    {
      name: "missing PascalCase import",
      code: `
//  0   0   1   1   2   2
//  5   9   3   7   1   5
import { PageHeader } from "antd";
`.trim(),
      errors: [
        {
          messageId: "missingImport",
          data: {
            imported: "PageHeader",
            style: "antd/lib/page-header/style",
          },
          line: 3,
          column: 10,
          endColumn: 20,
        },
      ],
      output: `
//  0   0   1   1   2   2
//  5   9   3   7   1   5
import { PageHeader } from "antd";
import "antd/lib/page-header/style";
`.trim(),
    },
    {
      name: "missing renamed PascalCase import",
      code: `
//  0   0   1   1   2   2   2
//  5   9   3   7   1   5   9
import { PageHeader as Header } from "antd";
`.trim(),
      errors: [
        {
          messageId: "missingImport",
          data: {
            imported: "PageHeader",
            style: "antd/lib/page-header/style",
          },
          line: 3,
          column: 10,
          endColumn: 30,
        },
      ],
      output: `
//  0   0   1   1   2   2   2
//  5   9   3   7   1   5   9
import { PageHeader as Header } from "antd";
import "antd/lib/page-header/style";
`.trim(),
    }
  ],
});
