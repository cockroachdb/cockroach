# System Privileges

## Overview
The system.privileges table allows privileges to be defined for non-descriptor 
backed objects. The system.privileges table uses a path (STRING) to identify 
an object and stores a list of privileges and grant options ([]STRING) for that 
object. Whenever we need to check privileges for that object, we synthesize a 
PrivilegeDescriptor to use to check for privileges.

## Guide on adding a new Privilege Object.
This readme walks through how to add a new privilege object
from adding the syntax, to adding the PrivilegeObject, checking privileges
and adding tests.

### Steps:
1. [Add syntax to sql.y](#add-syntax-to-sql.y)
2. [Adding the new PrivilegeObject type.](#adding-the-new-privilegeobject-type)
3. [Defining relevant privileges.](#defining-relevant-privileges)
4. [Testing](#testing)

#### Add syntax to sql.y
In most cases, you'll be updating the `targets` field that is used in `grant_stmt` to support whichever
new keyword object you'll be adding.
```sql
grant_stmt:
  GRANT privileges ON targets TO role_spec_list opt_with_grant_option
  {
    $$.val = &tree.Grant{Privileges: $2.privilegeList(), Grantees: $6.roleSpecList(), Targets: $4.targetList(), WithGrantOption: $7.bool(),}
  }
```

Add your new keyword to the `targets` syntax rule.
```sql
targets:
...
| complex_table_pattern
  {
    $$.val = tree.TargetList{Tables: tree.TableAttrs{SequenceOnly: false, TablePatterns: tree.TablePatterns{$1.unresolvedName()}}}
  }
| SEQUENCE table_pattern_list
  {
    $$.val = tree.TargetList{Tables: tree.TableAttrs{SequenceOnly: true, TablePatterns: $2.tablePatterns()}}
  }
| table_pattern ',' table_pattern_list
  {
    remainderPats := $3.tablePatterns()
    $$.val = tree.TargetList{Tables: tree.TableAttrs{SequenceOnly: false, TablePatterns: append(tree.TablePatterns{$1.unresolvedName()}, remainderPats...)}}
  }
| TABLE table_pattern_list
  {
    $$.val = tree.TargetList{Tables: tree.TableAttrs{SequenceOnly: false, TablePatterns: $2.tablePatterns()}}
  }
// TODO(knz): This should learn how to parse more complex expressions
// and placeholders.
| TENANT iconst64
  {
    tenID := uint64($2.int64())
    if tenID == 0 {
      return setErr(sqllex, errors.New("invalid tenant ID"))
    }
    $$.val = tree.TargetList{TenantID: tree.TenantID{Specified: true, ID: tenID}}
  }
| TENANT IDENT
  {
    // TODO(knz): This rule can go away once the main clause above supports
    // arbitrary expressions.
    if $2 != "_" {
       return setErr(sqllex, errors.New("invalid syntax"))
    }
    $$.val = tree.TargetList{TenantID: tree.TenantID{Specified: true}}
  }
| DATABASE name_list
  {
    $$.val = tree.TargetList{Databases: $2.nameList()}
  }
```

Example - update the rule to add

```sql
NewObject name_list
  {
    $$.val = tree.TargetList{NewObject: $2.nameList()}
  }
```

If `NewObject` is not already defined as a keyword in the `sql.y` file, you'll 
have to add it as a `%token` and under the `unreserved_keyword` list.

See the PR [#85556](https://github.com/cockroachdb/cockroach/pull/85556/files#diff-570c32ab78bd63a6e239714da4bcbaed11c42a87d8b0f44552a646875ba9b317R7465) adding `External Connections` privileges for an example of adding to 
sql.y.

``` goyacc
| EXTERNAL CONNECTION name_list
  {
    $$.val = tree.GrantTargetList{ExternalConnections: $3.nameList()}
  }
```

The above allows external connection to be a target in grant such that `GRANT USAGE ON EXTERNAL CONNECTION foo TO testuser` is valid.

Tokens are at the top of the sql.y file:
```goyacc
// If you want to make any keyword changes, add the new keyword here as well as
// to the appropriate one of the reserved-or-not-so-reserved keyword lists,
// below; search this file for "Keyword category lists".

// Ordinary key words in alphabetical order.
%token <str> ABORT ABSOLUTE ACCESS ACTION ADD ADMIN AFTER AGGREGATE
```

Unreserved_keywords:
```goyacc
// Keyword category lists. Generally, every keyword present in the Postgres
// grammar should appear in exactly one of these lists.
//
// Put a new keyword into the first list that it can go into without causing
// shift or reduce conflicts. The earlier lists define "less reserved"
// categories of keywords.
//
// "Unreserved" keywords --- available for use as any kind of name.
unreserved_keyword:
```

You'll also have to update `TargetList` in `tree/grant.go` to include a field 
for your new type.

```go
// TargetList represents a list of targets.
// Only one field may be non-nil.
type TargetList struct {
	Databases NameList
	Schemas   ObjectNamePrefixList
	Tables    TableAttrs
	TenantID  TenantID
	Types     []*UnresolvedObjectName
	// If the target is for all sequences in a set of schemas.
	AllSequencesInSchema bool
	// If the target is for all tables in a set of schemas.
	AllTablesInSchema bool
	// If the target is system.
	System bool

	// ForRoles and Roles are used internally in the parser and not used
	// in the AST. Therefore they do not participate in pretty-printing,
	// etc.
	ForRoles bool
	Roles    RoleSpecList
}
```

It is likely that the type of your new field should be of type `NameList`.

That should be all for adding the syntax.

#### Adding the new PrivilegeObject type
Next we'll have to add the `PrivilegeObject` type and add it to the object 
registry.

Firstly, define the new `PrivilegeObject` in `sql/privilege/privilege.go`

```go
const (
	// Any represents any object type.
	Any ObjectType = "any"
	// Database represents a database object.
	Database ObjectType = "database"
	// Schema represents a schema object.
	Schema ObjectType = "schema"
	// Table represents a table object.
	Table ObjectType = "table"
	// Type represents a type object.
	Type ObjectType = "type"
	// Sequence represents a sequence object.
	Sequence ObjectType = "sequence"
	// Global represents global privileges.
	Global ObjectType = "global"
	// NewObject represents ... (example)
	NewObject ObjectType = "new_object_string_representation"
)
```

Update the `isDescriptorBacked` map, your new object should be false.
```go
var isDescriptorBacked = map[ObjectType]bool{
	Database: true,
	Schema:   true,
	Table:    true,
	Type:     true,
	Sequence: true,
	Global:   false,
	NewObject: false,
}
```

Update the `getGrantOnObject` function that maps the `TargetList` type to a 
`PrivilegeObject`.

```go
func (p *planner) getGrantOnObject(
	ctx context.Context, targets tree.TargetList, incIAMFunc func(on string),
) (privilege.ObjectType, error) {
    switch {
    case targets.Databases != nil:
        incIAMFunc(sqltelemetry.OnDatabase)
        return privilege.Database, nil
    // ** ADD CASE FOR NEW OBJECT **
    case targets.NewObject != nil:
        incIAMFunc(sqltelemetry.NewObject) // Add a new sqltelemetry object.
        return privilege.NewObject, nil
    ...
    }
}
```

Create a new `PrivilegeObject` implementing `SyntheticPrivilegeObject`.

```go
type SyntheticPrivilegeObject interface {
	PrivilegeObject
	ToString() string
	PrivilegeObjectType() privilege.ObjectType
}
```

See: `sql/syntheticprivilege/global_privilege.go` as an example.
This step is essentially copy/pasting `global_privilege.go` and making
minor updates.

Add your new `PrivilegeObject` to `registry` in `sql/syntheticprivilege/synthetic_privilege_registry.go`

```go
var registry = []*Metadata{
    {
        prefix: "/global",
        regex:  regexp.MustCompile("(/global/)$"),
        val:    reflect.TypeOf((*GlobalPrivilege)(nil)),
    },
    {
        prefix: "/newobject",
        regex:  regexp.MustCompile("(/newobject/(?P<ObjectName>[a-zA-Z]+))$"),
        val:    reflect.TypeOf((*NewObject)(nil)),
    },
}
```

Note, the regex path uses name captured groups to match to fields using go
struct tags in the struct.

As an example, the type definition for `NewObject` would look like:
```go
type NewObject struct{
        ObjectName string `priv:ObjectName`
}
```

The `Parse` function takes the privilege object's `path` and creates the
corresponding `PrivilegeObject`

Note that new fields need to be added to the `unmarshal` function.
For string fields, we can generally return the string itself.
```go
func unmarshal(val reflect.Value, f reflect.StructField) (reflect.Value, error) {
        switch f.Name {
        case "TableName":
                return val, nil
        case "SchemaName":
                return val, nil
        case "PrivilegeObject"
                return val, nil
        default:
            panic(errors.AssertionFailedf("unhandled type %v", f.Type))
    }
```

#### Defining relevant privileges
You may need to define **new** privileges for your object type in 
`sql/privilege/privilege.go`

```go
// List of privileges. ALL is specifically encoded so that it will automatically
// pick up new privileges.
// Do not change values of privileges. These correspond to the position
// of the privilege in a bit field and are expected to stay constant.
const (
	ALL    Kind = 1
	CREATE Kind = 2
	DROP   Kind = 3
	// DEPRECATEDGRANT is a placeholder to make sure that 4 is not reused.
	// It was previously used for the GRANT privilege that has been replaced with the more granular Privilege.GrantOption.
	DEPRECATEDGRANT      Kind = 4 // GRANT
	SELECT               Kind = 5
	INSERT               Kind = 6
	DELETE               Kind = 7
	UPDATE               Kind = 8
	USAGE                Kind = 9
	ZONECONFIG           Kind = 10
	CONNECT              Kind = 11
	RULE                 Kind = 12
	MODIFYCLUSTERSETTING Kind = 13
)
```

Once that is done, create a list of the valid privileges for your object type.
```go
// Predefined sets of privileges.
var (
	AllPrivileges    = List{ALL, CONNECT, CREATE, DROP, SELECT, INSERT, DELETE, UPDATE, USAGE, ZONECONFIG}
	ReadData         = List{SELECT}
	ReadWriteData    = List{SELECT, INSERT, DELETE, UPDATE}
	DBPrivileges     = List{ALL, CONNECT, CREATE, DROP, ZONECONFIG}
	TablePrivileges  = List{ALL, CREATE, DROP, SELECT, INSERT, DELETE, UPDATE, ZONECONFIG}
	SchemaPrivileges = List{ALL, CREATE, USAGE}
	TypePrivileges   = List{ALL, USAGE}
	// SequencePrivileges is appended with TablePrivileges as well. This is because
	// before v22.2 we treated Sequences the same as Tables. This is to avoid making
	// certain privileges unavailable after upgrade migration.
	// Note that "CREATE, INSERT, DELETE, ZONECONFIG" are no-op privileges on sequences.
	SequencePrivileges = List{ALL, USAGE, SELECT, UPDATE, CREATE, DROP, INSERT, DELETE, ZONECONFIG}
	SystemPrivileges   = List{ALL, MODIFYCLUSTERSETTING}
)
```

Add privilege checks for your new object type, this depends on where you need the checks.

See below for an example of privilege checking.

The example below checks for **either** the `MODIFYCLUSTERSETTING` role option 
or "global" privilege.

```go
		hasAdmin, err := p.HasAdminRole(ctx)
		if err != nil {
			return err
		}
		if !hasAdmin {
			hasModify, err := p.HasRoleOption(ctx, roleoption.MODIFYCLUSTERSETTING)
			if err != nil {
				return err
			}
			hasView, err := p.HasRoleOption(ctx, roleoption.VIEWCLUSTERSETTING)
			if err != nil {
				return err
			}
			if !hasModify && !hasView {
				// We check for EITHER the MODIFYCLUSTERSETTING or VIEWCLUSTERSETTING
				// role option OR the MODIFYCLUSTERSETTING system cluster privilege.
				// We return the error for "system cluster privilege" due to
				// the long term goal of moving away from coarse-grained role options.
				if err := p.CheckPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.MODIFYCLUSTERSETTING); err != nil {
					return err
				}
			}
		}
```

#### Testing
Finally, add testing. This can be done using logic tests.
See: `sql/logictest/testdata/logic_test/system_privileges`

```sql
user testuser

statement error pq: user testuser does not have MODIFYCLUSTERSETTING system privilege
SELECT * FROM crdb_internal.cluster_settings;

user root

statement ok
GRANT SYSTEM MODIFYCLUSTERSETTING TO testuser

user testuser

statement ok
SELECT * FROM crdb_internal.cluster_settings;

user root

query TTTT
SELECT * FROM system.privileges
----
testuser  /global/  {MODIFYCLUSTERSETTING}  {}

statement ok
REVOKE SYSTEM MODIFYCLUSTERSETTING FROM testuser

user testuser

statement error pq: user testuser does not have MODIFYCLUSTERSETTING system privilege
SELECT * FROM crdb_internal.cluster_settings;

user root

query TTTT
SELECT * FROM system.privileges
----
```

Also ensure that your version gate works:
See: `sql/logictest/testdata/logic_test/system_privileges_mixed`
```sql
# LogicTest: local-mixed-22.1-22.2

user testuser

statement error pq: only users with either MODIFYCLUSTERSETTING or VIEWCLUSTERSETTING privileges are allowed to read crdb_internal.cluster_settings
SELECT * FROM crdb_internal.cluster_settings;

user root

statement error pq: system cluster privileges are not supported until upgrade to version SystemPrivilegesTable is finalized
GRANT SYSTEM MODIFYCLUSTERSETTING TO root

statement error pq: system cluster privileges are not supported until upgrade to version SystemPrivilegesTable is finalized
REVOKE SYSTEM MODIFYCLUSTERSETTING FROM root
```

Add a `pkg/sql/parser/testdata` test to make sure your privilege keywords
parse properly.

#### Supporting SHOW on your new privilege type.
Add relevant syntax to `sql.y` to support your new object type in a show statement.

```goyacc
| SHOW SYSTEM GRANTS for_grantee_clause
  {
    $$.val = &tree.ShowGrants{
      Targets: &tree.GrantTargetList{System: true},
        Grantees: $4.roleSpecList(),
      }
  }
```

Modify `show_grants.go` to craft a SQL query for your new object.
```go
	const externalConnectionPrivilegeQuery = `
SELECT *
  FROM (
        SELECT name,
               a.username AS grantee,
               privilege,
               a.privilege
               IN (
                  SELECT unnest(grant_options)
                    FROM system.privileges
                   WHERE username = a.username
                ) AS grantable
          FROM (
                SELECT regexp_extract(
                        path,
                        e'/externalconn/(\\S+)'
                       ) AS name,
                       username,
                       unnest(privileges) AS privilege
                  FROM system.privileges
                 WHERE path ~* '^/externalconn/'
               ) AS a
       )
`
```

Adding a show command can vary a lot so refer to [#86700](https://github.com/cockroachdb/cockroach/pull/86700) as a general reference..

#### All done!

Finally, open your PR for review. Request a review from `#sql-experience`.

#### Additional resources
[PR for adding "global" privileges](https://github.com/cockroachdb/cockroach/pull/82166/commits/430babc9959a3561a1b183652bec5d02780ec0a2#diff-61137cc13c92f7de04ddc5ea247245abe40e6037bd45a3280b7256f53e34493f)
