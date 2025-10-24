- Feature Name: External Connections
- Status: Draft
- Start Date: 06/30/2022
- Authors: Aditya Maru, Yevgeniy Miretskiy
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

## Summary

This RFC introduces the concept of an External Connection to CockroachDB, that allows users to
define and manage interactions with resources outside of the database. Examples of such external
connections are a bucket being written to by a backup schedule, and a changefeed sink.

Today, the interaction with such an external connection is configured entirely via a user-inputted
URI, and is tightly coupled with the database operation accessing the connection, typically being
supplied in full each time the user initiates or alters the operation. For example, when browsing
backups in a collection, a user needs to supply the full collection URI to `SHOW BACKUPS` to list
their backups, then again to each subsequent `SHOW BACKUP` as they browse through individual
backups. The layer of abstraction provided by an External Connection object will help address some
of the significant shortcomings of the current implementation. Most notably, the introduction of a
first-class object allows for the design of a fine-grained permission model that enables the
delegation of creation and usage of these objects to various roles and users in CockroachDB.

## Motivation

Currently, all bulk (backup, import, restore, export) and changefeed queries require a URI
describing the external bucket or sink they wish to interact with. The URIs are provider-specific
and support numerous, provider-specific query parameters that describe the configuration of the
client interacting with the external connection. These query parameters often contain sensitive
information such as access keys, certificates, usernames, and passwords, and depending on the
provider, may have to be URL escaped or encoded to a particular format.

Having to enter a multi-line, error-prone URI every time a user wants to interact with disaster
recovery features or changefeeds, creates a poor user experience. Further, it is critically
important from a security point of view to be able to delegate the creation, management, and usage
of an external connection to different roles/users in the cluster. Our current implementation
mandates that the user connecting to the external connection, for example, creating a changefeed,
must have admin-level access to secret keys, certificates, and resource configuration parameters.
This prevents the adoption of such database operations in environments where the admin-level
operator is not the same as the database developer.

The jobs that interact with external connections are usually long-running in nature. This means that
they likely access the external connection more than once during their execution. To enable this,
every job persists its URI in the `system.jobs` table. Every job is individually responsible for
redacting sensitive information, before displaying the persisted URI via the various job
observability tools that CockroachDB offers. There have been several occasions where we have leaked
secrets because a job had forgotten to sanitize the URI before displaying it.

At the root of these shortcomings is the tight coupling between the systems that use the external
connection, and the representation of the external connection. Every operation that accesses an
external connection is also made aware of the raw URI that is parsed to configure, authenticate, and
interact with the connection. This RFC proposes separating the creation and management of an
External Connection object from its use. This separation would facilitate the development of a
standardized interface and a fine-grained permission model, which would create a more user-friendly,
secure experience.

## Technical design

### Creating an External Connection

An External Connection object represents a resource that resides outside of CockroachDB. In the
scope of this RFC, the discussion is limited to the two types of external connections that are used
by bulk operations and changefeeds, namely:

- Storage: a connection capable of reading and writing bytes to an external store.

- Key Management Service (KMS): a connection capable of interacting with keys hosted in a KMS, to
  encrypt and decrypt data.

External Connections will be persisted in a new system table called `system.external_connections`
with the following schema:

```sql
CREATE TABLE system.external_connections (
    name               STRING NOT NULL,
    created            TIMESTAMPZ NOT NULL DEFAULT NOW(),
    connection_type    STRING NOT NULL,
    connection_details BYTES,

    CONSTRAINT "primary" PRIMARY KEY name,
)
```

The following SQL syntax will be used to create an External Connection:

```sql
CREATE EXTERNAL CONNECTION <name> AS <scheme://uri.....?params> [WITH connection_options]
```

An External Connection object is uniquely identifiable by its `name`, which is also how it will be
referred to by other database operations, for example, backup and changefeeds. `name` is a free-form
user input string that is specified at the time of creation. The creation of an External Connection
is a privileged operation as discussed in the privilege model below.

The `created` column stores the timestamp at which the External Connection was created.

The `connection_type` categorizes the External Connection based on the capabilities of the resource
it represents. To begin with, we will support the `Storage` and `KMS` connection types, each
corresponding to a unique connection capability as described above. Every connection type can in
turn have several supported integrations. The `scheme` of the URI passed in the SQL query above,
describes the integration that the External Connection represents. Following is a mapping from
connection type to the integrations that we support today:

Storage: s3, gs, azure, nodelocal, userfile, http, https, null, kafka, webhook-http, webhook-https, gcpubsub, experimental-sql

KMS: aws, gs

The `connection_type` can be used to filter for External Connection objects that can be used for a
particular database operation. This is discussed in the observability section of this RFC. Note, in
our initial implementation the `connection_type` will not be used to restrict the database
operations that can access the External Connection. For example, a `Storage` External Connection
created for a Changefeed will also be usable by a Backup provided they both support writing to the
underlying external resource scheme.

Similar to the cloud registry in `pkg/cloud`, every scheme will register itself as a supported
External Connection implementation in the newly introduced `pkg/cloud/external`. The implementation
will be responsible for parsing the URI, validating parameters, and specifying the `connection_type`
the scheme belongs to. Note, we do not attempt to connect to the external connection described by
the URI, this will be done by the database operation that uses the External Connection object. The
implementation will also construct an opaque byte representation of the underlying resource that
will be stored in the `connection_details` column.

```go
// ConnectionType identifies the type of the External Connection.
enum ConnectionType {
   UNSPECIFIED = 0;
   STORAGE = 1;
   KMS = 2;
}

enum ExternalConnectionProvider {
   unknown = 0;
   aws_storage = 1;
   gs_storage = 2;
   kafka_storage = 3;
   aws_kms_storage = 4;
   ...
}

// ExternalConnectionDetails describes an External Connection.
message ExternalConnectionDetails {
  ExternalConnectionProvider provider = 1;

  oneof details {
    AWSStorageDetails aws = 2;
    GSStorageDetails gs = 3;
    KafkaSinkDetails kafka = 4;
    AWSKMSDetails = aws_kms = 5;
    ...
 }
}
```

`ExternalConnectionDetails` and all the provider specific details will be defined
in  `pkg/cloud/external/connectionpb`. The details will store information parsed from the endpoint
URI specified in the `CREATE EXTERNAL CONNECTION` query, as well as any connection-level
configurations that might be specified in the WITH clause as a `connection_option`. For the initial
implementation, we will not support any connection options but we anticipate the need for such
options to configure parameters that cannot be described in the endpoint URI, for example, AWS retry
policies or Sarama configs for a Kafka sink. Every provider will implement the `ConnectionDetails`
interface in their respective `pkg/cloud` package. The interface is described below:

```go
// ConnectionDetails is an interface that can be used for interacting with
// the external resource represented by an External Connection.
type ConnectionDetails interface {
  // ConnectionType returns the type of the External Connection.
  ConnectionType() connectionpb.ConnectionType
  // ConnectionProto prepares the ConnectionDetails for serialization.
  ConnectionProto() *connectionpb.ExternalConnectionDetails
  // Dial establishes a connection to the external resource.
  Dial() Connection 
  // DisplayString returns the string representation of the
  // external resource with all sensitive information redacted.
  DisplayString() string
}

// Connection is a marker interface that is implemented by objects that
// support interaction with the external resource.  
type Connection interface {}
```

The `ConnectionProto` method returns the concrete `connectionpb.ExternalConnectionDetails` that can
be serialized and stored in the `connection_details` columns of the system table.
The `ConnectionType` method returns the connection type that the External Connection belongs to.

To prevent misuse of the underlying endpoint URI and configuration information, we will expose an
opaque `Dial` method. This method will be responsible for establishing a connection to the external
resource and returning a handle to interact with it. The `Connection` interface is a marker
interface that will be implemented by the `cloud.ExternalStorage`, `cloud.KMS`,
and `changefeedccl.Sink` interfaces described in the section below. The handle will therefore
support all the methods that are used by bulk operations, and changefeeds to interact with external
connections today.

The `DisplayString` method returns a string representation of the external connection with all
secrets redacted. This string will be used by observability tools that display information about the
external resource. The scheme-specific implementations have control over how to display their
endpoint URI and configurations as well as what information they consider privileged.

### Accessing an External Connection

Database operations such as backup, restore, import, export, and changefeeds interact with external
resources via user-inputted URIs as part of the SQL query. Depending on the database operation and
the scheme of the URI, the parsing, validation, and creation of a handle to the external connection
are managed by different packages in CockroachDB. Each package recognizes a set of URI schemes and
in turn returns one of the following interfaces to interact with the external connection:

`cloud.ExternalStorage`: Used by bulk operations to read and write files.

`cloud.KMS`: Used by bulk operations to encrypt and decrypt data using a Key Management Service.

`changefeedccl.Sink`: Used by changefeeds to stream changes to an external connection.

This RFC proposes introducing a new URI scheme that can be used with all bulk operations and
changefeeds. The URI will be of the form `external://<external-connection-name>`.

The URI will only permit a hostname, in other words, it will not accept a path or any parameters.
This is to prevent users from accidentally specifying sensitive information in the URI. The hostname
will refer to the name of the External Connection object to be used by the database operation. For
example, an encrypted scheduled backup to an External Connection can be created as follows:

``` sql
CREATE EXTERNAL CONNECTION backup-location AS gs://bucket-foo?params;

CREATE EXTERNAL CONNECTION backup-kms AS gs:///masterkey?params;

CREATE SCHEDULE backup-schedule
FOR BACKUP INTO  'external://backup-location'
WITH kms = 'external://backup-kms'
RECURRING '@daily';
```

As mentioned before, a job will persist the External Connection URI in its job payload since it
might need to access the connection multiple times during its execution. Since the `external` URI
only permits a hostname, the job no longer has to worry about implementing bespoke URI redaction
logic.

Similar to other integrations, we will register parsing and validation logic for the `external`
scheme with the aforementioned packages. The implementation will resolve the External Connection
identified by the `host` of the URI by querying for the object in `system.external_connections`.
After ensuring the user has adequate privileges, we will unmarshal the objects’ `ConnectionDetails`
and call `Dial` to establish a connection. The returned `Connection` interface can then be returned
as a `cloud.ExternalStorage`, `cloud.KMS` or `changefeedccl.Sink` depending on the caller that wants
to interact with it.

### Altering an External Connection

The following SQL syntax will be used to alter an External Connection:

```sql
ALTER EXTERNAL CONNECTION <name> AS <scheme://uri.....?params> [WITH connection_options];
```

A user may want to alter an existing External Connection for various reasons such as rotating
credentials or changing a connection configuration. Altering an ExternalConnection goes through many
of the same motions as creating a new one. The endpoint URI and connection options will be reparsed
and used to construct a new `connectionpb.ExternalConnectionDetails` that will replace the existing
bytes in the `connection_details` column of the row in the system table. The user must be the owner
of the External Connection object to be able to alter it.

It is possible that we have a running database operation using the External Connection at the time
it is altered. For example, a changefeed dials the external connection once at the time of setting
up its processors and then reuses the returned connection handle until the next time the flow is
planned. Ideally, we would like any interactions with the external connection after the alter, to
redial and use the new endpoint and configuration to establish a connection. In our initial
implementation, we will recommend users pause the operation, alter the external connection, and then
resume the operation to force a redial with the updated configuration.

In the future we can have an `ExternalConnectionWatcher` that sets up a rangefeed
on `system.external_connections`. Every database operation that uses the External Connection object
can then register itself with the `ExternalConnectionWatcher` to watch for updates on the row
corresponding to the External Connection in use, and react to updates to that row.

### Dropping an External Connection

The following SQL syntax will be used to drop an External Connection:

```sql
DROP EXTERNAL CONNECTION <name>;
```

Dropping an External Connection involves deleting the row from the `system.external_connections`
table. The user must be the owner of the External Connection object to be able to drop it.

Similar to the `ALTER` statement above, we must be mindful of database operations running at the
time we drop the connection. Ideally, we would prevent a user from dropping the External Connection
while there are operations using it. For our initial implementation, we will do nothing to block the
drop or proactively notify operations if their External Connection has been dropped. The operation
will run into an “object not found” error the next time they resolve the `external` URI.

In the future, we hope to be able to rely on the `ExternalConnectionWatcher` mentioned above to
appropriately handle the deletion of an in-use External Connection.

### Permission Model

A primary motivation of decoupling the creation of an External Connection from its usage was the
ability to delegate those operations to different users or roles in the cluster. Here we outline a
privilege model that will govern a users interaction with an External Connection.

#### CREATE

We will introduce a `GLOBAL` privilege called `CREATEEXTERNALCONNECTION` that can be granted to a
user or a role. The privilege will follow the general inheritance rules of other privileges in
CockroachDB. An External Connection can only be created by a user that is part of the admin role or
a role with the `CREATEEXTERNALCONNECTION` global privilege. The user or role that creates the
External Connection is the default owner of the object and is the only user except for root and
admin that can `ALTER` or `DROP` the External Connection.

We will add support for granting the above mentioned global privilege:

```sql
GRANT SYSTEM CREATEEXTERNALCONNECTION TO [role_spec_list];
```

#### USAGE

A user or role requires the `USAGE` privilege to use an External Connection via other database
operations such as backup, restore, changefeed, import, and export.

#### GRANT

A user or role requires the GRANT privilege to in turn grant privileges to other users. The owner of
an object implicitly has the `GRANT OPTION` for all privileges. If a privilege is
granted `WITH GRANT OPTION` to a user, then the user can in turn grant that privilege to other
users.

To support granting the above privileges we will add the following statements to the
existing `GRANT` SQL grammar:

```sql
GRANT ALL ON EXTERNAL CONNECTION <name> TO <role_spec_list> [WITH GRANT OPTION];

GRANT USAGE, GRANT ON EXTERNAL CONNECTION <name> TO <role_spec_list> [WITH GRANT OPTION];
```

An External Connection is a non-descriptor backed object and will store a synthetic privilege
descriptor in the `system.privileges` table. The privileges will be addressable by the unique path
`/externalconnection/<external-connection-name>` and will be marshaled and unmarshaled using the
APIs provided by `pkg/sql/syntheticprivilege`.

As mentioned above, the default owner of an External Connection is the user that created the object.
The owner of the object can alter the External Connection to have a new owner if the user executing
the command is a member of the new owner role. This will be done using:

```sql
ALTER EXTERNAL CONNECTION <name> OWNER TO <role_spec_list>;
```

### Observability

Variations of the `SHOW` statement will be used to provide observability into the External
Connection objects in a cluster. Similar to other implementations, the show statements will be
powered by a delegate that issues `SELECT` queries against the
underlying `system.external_resources` table. The permissions required to run particular `SHOW`
queries are discussed below:

```sql
SHOW GRANTS ON EXTERNAL CONNECTION <name>;
```

No privileges are required for a user to view the grants on an External Connection. The query will
output a table with each row specifying the `name` of the External Connection, the `grantee`,
the `privilege_type`, and whether the privilege `is_grantable`. `is_grantable` will be true if the
privilege was granted `WITH GRANT OPTION` to the user or role.

```sql
SHOW EXTERNAL CONNECTION <name>;

SHOW EXTERNAL CONNECTIONS;
```

The above queries will output a table with each row specifying the `name` of the External
Connection, the `created` timestamp, the `owner`, and the `connection_type` of the object. There
will be an additional column for `connection_details` whose content will be powered by
the `DisplayString` method exposed by the `ConnectionDetails` interface. This query can be run by
any user or role regardless of their privileges since it will only display redacted representations
of the external connections.

```sql
SHOW CREATE EXTERNAL CONNECTION <name>;
```

This query can only be run by the root, admin, or the owner of the External Connection object. It
will display the original SQL query that was used to create the External Connection. This could
contain sensitive information such as secret keys, usernames, and passwords.

```sql
SHOW ALL [BACKUP|CHANGEFEED|RESTORE|IMPORT|EXPORT] EXTERNAL CONNECTIONS;
```

In addition to the above statements, this RFC proposes showing External Connections that are capable
of servicing specific database operations. Every database operation is aware of the type of
connection it requires and the URI schemes in that connection type that it supports. Implementations
can therefore filter on the basis of the `connection_type` column in
the `system.external_connections` table and display information about the relevant resources. The
format and content of every row outputted by these statements will be identical to that
of `SHOW EXTERNAL CONNECTIONS`. This is not considered a requirement for the initial implementation
of External Connections.

### Future Work

- External Connections should be consumable from the debug/cloud console.
- Add External Connection options to define connection level configurations.
- Phase-out support for accpeting s3, gs etc. URIs in SQL statements, and migrate to an External
  Connection only world.
- Implement ConnectionWatcher to provide better semantics around altering and dropping an External
  Connection.
