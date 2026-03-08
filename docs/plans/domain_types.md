# User-Defined DOMAIN Types (Issue #27796)

## Overview

PostgreSQL DOMAIN types are named data types based on an existing type with
optional constraints (CHECK, NOT NULL, DEFAULT). They enforce data integrity
at the type level rather than per-column. Example:

```sql
CREATE DOMAIN us_postal_code AS TEXT
  CHECK(VALUE ~ '^\d{5}$' OR VALUE ~ '^\d{5}-\d{4}$');
```

CockroachDB already has partial scaffolding: `CreateTypeVariety.Domain` exists
in the AST enum, the grammar has `CREATE DOMAIN type_name error` stubs, and
`information_schema` has empty domain-related views. This plan builds on the
existing ENUM/COMPOSITE type infrastructure.

## Phase 1: Protobuf Schema + AST + Parser

**Goal**: Parse `CREATE DOMAIN` and `DROP DOMAIN`, store in descriptors. No
constraint enforcement yet — just the plumbing.

### 1a. Protobuf: TypeDescriptor additions

**File**: `pkg/sql/catalog/descpb/structured.proto`

- Add `DOMAIN = 5` to `TypeDescriptor.Kind` enum (after COMPOSITE = 4)
- Add `Domain` sub-message:
  ```protobuf
  message Domain {
    option (gogoproto.equal) = true;
    optional sql.sem.types.T base_type = 1;
    optional bool not_null = 2 [(gogoproto.nullable) = false];
    optional string default_expr = 3 [(gogoproto.nullable) = false];
    message CheckConstraint {
      option (gogoproto.equal) = true;
      optional string name = 1 [(gogoproto.nullable) = false];
      optional string expr = 2 [(gogoproto.nullable) = false];
    }
    repeated CheckConstraint check_constraints = 4 [(gogoproto.nullable) = false];
  }
  ```
- Add `optional Domain domain = 21;` field to `TypeDescriptor`
- Fix "Next field is 19" comment → 22

### 1b. AST: Extend CreateType

**File**: `pkg/sql/sem/tree/create.go`

- Add fields to `CreateType`:
  ```go
  DomainType ResolvableTypeReference
  DomainDefault Expr
  DomainNotNull bool
  DomainConstraints []DomainConstraintDef
  ```
- Add `DomainConstraintDef` struct:
  ```go
  type DomainConstraintDef struct {
    Name Name
    Expr Expr
  }
  ```
- Extend `Format()` to handle `Variety == Domain`

### 1c. Parser grammar

**File**: `pkg/sql/parser/sql.y`

Replace the `CREATE DOMAIN type_name error` rule with:
```
CREATE DOMAIN type_name AS typename domain_constraint_list_opt
CREATE DOMAIN type_name typename domain_constraint_list_opt
```
Where `domain_constraint_list_opt` is zero or more of:
- `DEFAULT a_expr`
- `NOT NULL`
- `NULL` (no-op, for compat)
- `CONSTRAINT name CHECK '(' a_expr ')'`
- `CHECK '(' a_expr ')'`

Also replace `DROP DOMAIN error` with actual DROP support.
Replace `ALTER DOMAIN error` stub similarly (Phase 4).

### 1d. Regenerate

- Run `./dev generate bazel` after proto changes
- Run `./dev generate go` for stringer, proto codegen
- Run `./dev generate parser` if grammar changes need it

## Phase 2: Type System + Descriptor + Execution of CREATE/DROP

**Goal**: CREATE DOMAIN creates a TypeDescriptor with Kind=DOMAIN. DROP DOMAIN
removes it. The domain type can be referenced in column definitions.

### 2a. Type system: types.T representation

**File**: `pkg/sql/types/types.go`

Domains use the **base type's Family** with domain metadata attached. This
means a `DOMAIN pos_int AS INT` has `Family = IntFamily` and `Oid` pointing
to the domain's descriptor. This lets all existing operators/functions work
on domain values without special-casing.

- Add `DomainMetadata` struct to `UserDefinedTypeMetadata`:
  ```go
  type DomainMetadata struct {
    BaseType *T
    NotNull  bool
    DefaultExpr string
    CheckConstraints []DomainCheckConstraint
  }
  type DomainCheckConstraint struct {
    Name string
    Expr string
  }
  ```
- Add `MakeDomain(baseType *T, typeOID, arrayTypeOID oid.Oid) *T` constructor.

### 2b. Catalog interfaces

**File**: `pkg/sql/catalog/descriptor.go`

- Add `DomainTypeDescriptor` interface:
  ```go
  type DomainTypeDescriptor interface {
    NonAliasTypeDescriptor
    GetBaseType() *types.T
    IsNotNull() bool
    GetDefaultExpr() string
    NumCheckConstraints() int
    GetCheckConstraintName(idx int) string
    GetCheckConstraintExpr(idx int) string
  }
  ```
- Add `AsDomainTypeDescriptor()` to `TypeDescriptor` interface

### 2c. Type descriptor implementation

**File**: `pkg/sql/catalog/typedesc/type_desc.go`

- Extend `makeImmutable()` to handle `Kind == DOMAIN`
- Implement `AsDomainTypeDescriptor()` returning `desc` when `Kind == DOMAIN`
- Implement `DomainTypeDescriptor` interface methods on `immutable`
- Extend `AsTypesT()` to handle `DOMAIN`: call `types.MakeDomain(...)`

### 2d. Type hydration

**File**: `pkg/sql/catalog/typedesc/hydrate.go`

- Extend `ensureTypeMetadataIsHydrated()` to populate `DomainMetadata` from the
  descriptor when Kind is DOMAIN.

### 2e. CreateUserDefinedArrayTypeDesc

**File**: `pkg/sql/create_type.go`

- Extend `CreateUserDefinedArrayTypeDesc` switch to handle
  `descpb.TypeDescriptor_DOMAIN`: create element type via `types.MakeDomain(...)`.

### 2f. CREATE DOMAIN execution

**File**: `pkg/sql/create_type.go`

- Add `tree.Domain` case to `createUserDefinedType()` switch
- Implement `createDomainWithID()` following `createEnumWithID()` pattern:
  1. Call `getCreateTypeParams()` for schema validation
  2. Resolve the base type via `tree.ResolveType()`
  3. Validate CHECK expressions: parse, type-check with a synthetic `VALUE`
     column reference of the base type
  4. Build `descpb.TypeDescriptor` with `Kind: DOMAIN` and `Domain` sub-message
  5. Call `finishCreateType()` (creates implicit array type + persists)

### 2g. DROP DOMAIN

**File**: `pkg/sql/drop_type.go`

- Domain types are dropped via `DROP TYPE` (which already exists) or
  `DROP DOMAIN` (new grammar alias). The existing DROP TYPE machinery should
  work once the descriptor exists — verify and extend if needed.
- Ensure dependent columns are checked (CASCADE/RESTRICT semantics).

### 2h. Column definitions using domain types

When a column is defined with a domain type, the column's type in the table
descriptor should reference the domain's OID. This should largely work
through existing user-defined type resolution — verify and fix gaps.

## Phase 3: Constraint Enforcement

**Goal**: Domain CHECK and NOT NULL constraints are validated on assignment
(INSERT/UPDATE) and explicit CAST.

### 3a. VALUE keyword in CHECK expressions

In domain CHECK constraints, `VALUE` refers to the value being checked. During
type-checking of the CHECK expression:
- Replace `VALUE` with a typed placeholder or `IndexedVar` of the base type
- During evaluation, bind `VALUE` to the actual datum being validated

### 3b. Domain constraint validation function

**File**: `pkg/sql/sem/eval/cast.go` or new `pkg/sql/sem/eval/domain.go`

Create `ValidateDomainConstraints(ctx *Context, d tree.Datum, domainType *types.T) error`:
1. If `domainType.TypeMeta.DomainData.NotNull` and datum is DNull → error
2. For each CHECK constraint, parse expr, evaluate with VALUE bound → pgcode.CheckViolation

### 3c. Enforce on CAST to domain type

In `PerformCast()`, after the base-type cast succeeds, if target is a domain,
call `ValidateDomainConstraints()`.

### 3d. Enforce on INSERT/UPDATE assignment

Domain constraints validated through assignment casts in `PerformCast()`.

### 3e. Domain DEFAULT values

When a column uses a domain type and has no explicit DEFAULT, the domain's
DEFAULT (if any) should be used.

## Phase 4: ALTER DOMAIN

PostgreSQL ALTER DOMAIN subcommands:
- `ALTER DOMAIN name SET DEFAULT expression`
- `ALTER DOMAIN name DROP DEFAULT`
- `ALTER DOMAIN name SET NOT NULL`
- `ALTER DOMAIN name DROP NOT NULL`
- `ALTER DOMAIN name ADD CONSTRAINT name CHECK (expr) [NOT VALID]`
- `ALTER DOMAIN name DROP CONSTRAINT name [IF EXISTS] [CASCADE|RESTRICT]`
- `ALTER DOMAIN name RENAME CONSTRAINT name TO new_name`
- `ALTER DOMAIN name VALIDATE CONSTRAINT name`
- `ALTER DOMAIN name OWNER TO new_owner`
- `ALTER DOMAIN name RENAME TO new_name`
- `ALTER DOMAIN name SET SCHEMA new_schema`

Some (OWNER TO, RENAME TO, SET SCHEMA) can reuse existing AlterType infrastructure.

## Phase 5: information_schema + pg_catalog

- 5a. `information_schema.domains` — populate with domain type descriptors
- 5b. `information_schema.domain_constraints` — CHECK and NOT NULL constraints
- 5c. `information_schema.column_domain_usage` — columns referencing domain types
- 5d. `information_schema.domain_udt_usage` — domains to base UDT types
- 5e. `pg_catalog.pg_type` — domain types with `typtype = 'd'` and `typbasetype`

## Phase 6: Optimizer + Backup/Restore + Edge Cases

- 6a. Optimizer: synthesize CHECK constraints from domain types
- 6b. Backup/Restore: include domain TypeDescriptors, compat checks
- 6c. Cluster version gate for mixed-version clusters
- 6d. Schema change job integration
- 6e. SHOW CREATE TYPE extension for domains

## Next Actions

1. Create a domain logic test (need to run `./dev gen logictest` to register the new test file, then run it)
2. Phase 3: Constraint enforcement
3. Phase 4: ALTER DOMAIN
4. Phase 5: information_schema + pg_catalog
5. Phase 6: Optimizer + backup/restore + edge cases

## Implementation Progress

| Phase | Status | Notes |
|-------|--------|-------|
| 1a. Protobuf | Done | DOMAIN=5 kind, Domain sub-message |
| 1b. AST | Done | DomainType, DomainDefault, DomainNotNull, DomainConstraints fields |
| 1c. Parser | Done | CREATE/DROP DOMAIN grammar, parser tests |
| 1d. Regenerate | Done | protobuf + bazel |
| 2a. Type system | Done | DomainMetadata, MakeDomain() |
| 2b. Catalog interfaces | Done | DomainTypeDescriptor interface |
| 2c-2d. Type desc + hydration | Done | ValidateSelf, AsTypesT, hydration |
| 2e-2h. Execution | Done | createDomainWithID, DROP DOMAIN, array type |
| 3. Constraint enforcement | Not Started | |
| 4. ALTER DOMAIN | Not Started | |
| 5. info_schema + pg_catalog | Not Started | |
| 6. Optimizer + backup + edge cases | Not Started | |

## Key Files

| Area | File |
|------|------|
| Protobuf | `pkg/sql/catalog/descpb/structured.proto` |
| AST | `pkg/sql/sem/tree/create.go` |
| Grammar | `pkg/sql/parser/sql.y` |
| types.T | `pkg/sql/types/types.go` |
| Catalog interface | `pkg/sql/catalog/descriptor.go` |
| Type descriptor | `pkg/sql/catalog/typedesc/type_desc.go` |
| Type hydration | `pkg/sql/catalog/typedesc/hydrate.go` |
| CREATE execution | `pkg/sql/create_type.go` |
| DROP execution | `pkg/sql/drop_type.go` |
| Constraint eval | `pkg/sql/sem/eval/cast.go` |
| info_schema | `pkg/sql/information_schema.go` |
| pg_catalog | `pkg/sql/pg_catalog.go` |
