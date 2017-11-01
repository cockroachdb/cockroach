- Feature Name: Encryption at rest
- Status: draft
- Start Date: 2017-11-01
- Authors: Marc Berhault
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [#19783](https://github.com/cockroachdb/cockroach/issues/19783)

Table of Contents
=================

   * [Summary](#summary)
   * [Motivation](#motivation)
   * [Related resources](#related-resources)
   * [Out of scope](#out-of-scope)
   * [Guide-level explanation](#guide-level-explanation)
      * [Terminology](#terminology)
      * [User-level explanation](#user-level-explanation)
         * [Store keys](#store-keys)
         * [Data keys](#data-keys)
         * [User control of encryption](#user-control-of-encryption)
      * [Contributor impact](#contributor-impact)
   * [Reference-level explanation](#reference-level-explanation)
      * [Detailed design](#detailed-design)
         * [Encryption layer for rocksdb](#encryption-layer-for-rocksdb)
         * [Preamble format](#preamble-format)
         * [Enabling the preamble format](#enabling-the-preamble-format)
         * [Key levels](#key-levels)
         * [Key status](#key-status)
         * [Key file format](#key-file-format)
         * [Loading store keys](#loading-store-keys)
         * [Rotating store keys](#rotating-store-keys)
         * [Loading data keys](#loading-data-keys)
         * [Generating data keys](#generating-data-keys)
         * [Rotating data keys](#rotating-data-keys)
         * [Reporting encryption status](#reporting-encryption-status)
         * [Other uses of rocksdb](#other-uses-of-rocksdb)
         * [Enterprise enforcement](#enterprise-enforcement)
      * [Security considerations](#security-considerations)
      * [Drawbacks](#drawbacks)
      * [Rationale and Alternatives](#rationale-and-alternatives)
      * [Unresolved questions](#unresolved-questions)
      * [Future improvements](#future-improvements)

# Summary

This feature is Enterprise.

We propose to add support for encryption at rest on cockroach nodes, with
encryption being done at the rocksdb layer for each file.

We provide CTR-mode AES encryption for all files written through rocksdb.
Encryption-related information is stored in a 4KiB preamble for each file.

Keys are split into user-provided store keys, and dynamically-generated data keys.
Store keys are used to encrypt the data keys. Data keys are used to encrypt the actual data.
Store keys can be rotated at the user's discretion. Data keys can be rotated automatically
on a regular schedule, relying on rocksdb churn to re-encrypt data.

Data can be transitioned from plaintext to encrypted and back. However, the preamble
file format cannot be enabled on stores written using previous versions of cockroach,
requiring new stores to be added to the cluster.

# Motivation

Encryption is desired for security reasons (prevent access from other users on the same
machine, prevent data leak through drive theft/disposal) as well as regulatory reasons
(GDPR, HIPPA, OCI DSS).

Encryption at rest is necessary when other methods of encryption are either not desirable,
or not sufficient (eg: filesystem-level encryption cannot be used if DBAs do not have
access to filesystem encryption utilities).

# Related resources

* [Crypto++](https://www.cryptopp.com/)
* [overview of block cipher modes](https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Common_modes)
* [rocksdb PR adding env_encryption](https://github.com/facebook/rocksdb/pull/2424)

# Out of scope

The following are not in scope but should not be hindered by implementation of this RFC:
* encryption of non-rocksdb data (eg: log files)
* integration with external key storage systems such as Vault, AWS KMS, KeyWhiz
* auditing of key usage and encryption status
* integration with HSM (hardware security module) or TPM (Trusted Platform Module)
* FIPS-140-2 compliance

The following are unrelated to encryption-at-rest as currently proposed:
* encrypted backup (should be supported regardless of encryption-at-rest status)
* fine-granularity encryption (that cannot use zone configs to select encrypted replicas)

# Guide-level explanation

## Terminology

Terminology used in this RFC:
* **data key**: used to encrypt the actual on-disk data. These are generated automatically.
* **store key**: used the encrypt the set of data keys. Provided by the user.
* **active key**: the key being used to encrypt new data.
* **key rotation**: encrypting data with a new key. Rotation starts when the new key is provided and ends when no data encrypted with the old key remains.
* **plaintext**: unencrypted data.
* **preamble**: the first 4KiB of a file containing encryption-related information.
* **preamble file format**: the new file format using a preamble.
* **classic file format**: the "normal" rocksdb file format, without a preamble.

## User-level explanation

Encryption-at-rest is an optional feature that can be enabled on a per-store basis.

In order to enable encryption on a given store, the user needs two things:
* an enterprise license
* one or more store key(s)

Support for encryption requires the new preamble file format in rocksdb. This can be enabled
before turning on encryption, or concurrently. However, an existing rocksdb node without the preamble data
format **CANNOT** use encryption.

An existing node cannot be converted from classic file format to preamble file format or vice versa.

### Store keys

The store key is a symmetric key provided by the user. It has the following properties:
* unique for each store
* available only to the cockroach process on the node
* not stored on the same disk as the cockroach data

Each store key on a store must have a unique ID and a creation timestamp associated.
Store keys are stored in a plaintext file with one line per key:

A user can create a key as follows:
1. key ID: 1 higher than the current highest key ID on this store. First key has ID 1.
1. cipher: the cipher used. Choice of `PLAIN`, `AES128-CTR`, `AES192-CTR`, `AES256-CTR`.
1. timestamp: the current time in seconds since epoch (eg: `date +%s`)
1. key: a key in hexadecimal format (eg: `openssl rand -hex 32` **WARNING** do not use this to generate keys)

The timestamp should be approximately the time at which we start using the key. This value
is used to compute and report the age of a key.

The cipher is used to encrypt the list of data keys. It is also the same cipher used to encrypt
the data files (eg: if you use a 128 bit store key, data keys will be 128 bits).

An example file with the first key for this store would look like:
```
1;1509649195;AES256-CTR;7acff104117d59ae1a6f997a7cd0d2f348b038d09a47ae8cbaaa6288999fef10
```

After adding additional keys, the file may look like:
```
1;1509649195;AES256-CTR;7acff104117d59ae1a6f997a7cd0d2f348b038d09a47ae8cbaaa6288999fef10
2;1509767342;AES128-CTR;a4b5a9160208a6af8477de25b10809a3
3;1510289457;AES256-CTR;9238e4168d41a8e862ad74df5c3a5ca39884c3b080863a8d337990a05bde3bb7
4;1512412094;PLAIN;
```

This sample file shows the progression of keys and ciphers used:
1. AES256 encryption
1. AES128 encryption
1. AES256 encryption
1. No encryption (this effectively removes encryption from the entire data directory)

We will call this file `cockroach.keys` and make it available only to the cockroach user.

The store key with highest ID is the active store key.

### Data keys

Data keys are automatically generated by cockroach. They are stored in the data directory and
encrypted with the active store key. Data keys are used to encrypt the actual files inside the data
directory.

This two-level approach is used to allow easy rotation of store keys. To rotate the store key, all we need
to do is re-encrypt the file containing the data keys, leaving the bulk of the data as is.

Data keys are generated and rotated by cockroach.
There are two parameters controlling how data keys behave:
* encryption cipher: the cipher in use for data encryption. Possible values: `PLAIN`, `AES128-CTR`, `AES192-CTR`, `AES256-CTR`. Default value: `PLAIN`. This is the same as the active store key.
* rotation period: the time before a new key is generated and used. Default value: 1 week. This can be set through a flag.

The size of each data key will depend on the choice of cipher.

### User control of encryption

#### Flag changes for the cockroach binary

* `--rocksdb-preamble-format`: use the new preamble file format in rocksdb. This must be specified at node-creation
time to enable the preamble file format. Specifying this on a node with existing data in classic format fails with `ERROR cockroach data directory using classic format, cannot be converted to preamble format`.
* `--enterprise-encryption-keys=store=<store path>,keys=<key path>`: filename containing the store keys.
This enables encryption. Keys contained inside dictate the type of encryption to use, including `PLAIN`
to migrate data back to plaintext. `<store path>` specifies which store uses these keys. The flag can be specified multiple times (once for each store).
* `--enterprise-encryption-rotation-period=store=<store path>,keys=<key path>`: default: `168h` (one week). How often a new data key should be generated and used. This is per-store flag and can be specified more than once.

The first step in allowing at-rest encryption is using the preamble data format.
This must be done at node-creation time with the `--rocksdb-preamble-format` flag.

#### Attempting to convert an existing node

Given an existing node created without `--rocksdb-preamble-format` or with a version of cockroach
unaware of the preamble format:
```
cockroach start <regular options> --rocksdb-preamble-format
ERROR: cockroach data directory using classic format, cannot be converted to preamble format.
```

#### Attempting to use encryption without preamble format

Attempting to use encryption without the preamble format also fails:
```
cockroach start <regular options> --enterprise-encryption-keys=store=/mnt/data,keys=/path/to/cockroach.keys
ERROR: cockroach data directory using classic format, cannot use encryption.
```

#### Initializing a new node with preamble format

Creating a new cockroach node with preamble format, but no encryption:
```
cockroach start <regular options> --rocksdb-preamble-format
SUCCESS
```

#### Enabling encryption on a node with preamble format

Turning on encryption for a node with preamble format. This can be done when initializing the node
(start with encryption on), or on a non-encrypted node with preamble format (turning on encryption).

**WARNING**: this is for demonstration purposes only, do not generate keys this way.
```
# Create a 256 bit key with ID 1. Use AES256-CTR cipher.
$ echo "1;$(date +%s);AES256-CTR;$(openssl rand -hex 32)" > cockroach.keys
$ cat cockroach.keys
1;1509715352;AES256-CTR;67cceefb7319d98b88464f81c5a7786d78ab42600008f117be2884821df7636a
# Tell cockroach to use a AES128-CTR cipher for data encryption.
$ cockroach start <regular options> \
    --rocksdb-preamble-format \
    --enterprise-encryption-keys=store=/mnt/data,keys=/path/to/cockroach.keys
```

The node will generate a 128 bit data key, encrypt the list of data keys with the store key, and use AES128-CTR
encryption for all new files.

Examine the logs or node debug pages to see that store key 1 is in use.

Examine logs and debug pages to see progress of data encryption. This may take some time.

#### Rotating the store key.

Given the previous configuration, we can add a new key to the file and restart the node to change the store key.

**WARNING**: this is for demonstration purposes only, do not generate keys this way.
```
# Create a 256 bit key with ID 2 (higher than the current highest key in the file). Use AES256-CTR cipher.
$ echo "2;$(date +%s);AES256-CTR;$(openssl rand -hex 32)" >> cockroach.keys
$ cat cockroach.keys
1;1509715352;AES256-CTR;67cceefb7319d98b88464f81c5a7786d78ab42600008f117be2884821df7636a
2;1509715953;AES256-CTR;05ea263650495760de8e517c25ce2e430710b3251624268c0ee923fb1886e64d
# Tell cockroach to use a AES128-CTR cipher for data encryption.
$ cockroach start <regular options> \
    --rocksdb-preamble-format \
    --enterprise-encryption-keys=store=/mnt/data,keys=/path/to/cockroach.keys
```

Examine the logs or node debug pages to see that key 2 is now in use. It is now safe to delete key 1 from the file.

#### Disabling encryption

To disable encryption on a currently-encrypted node, we need to rewrite the list of data keys in plaintext.
We still need to specify the path to the store keys to properly decrypt the data keys.

```
# Add a key with cipher "PLAIN" in the key file. This means plaintext.
$ echo "3;$(date +%s);PLAIN;" >> cockroach.keys
$ cat cockroach.keys
1;1509715352;AES256-CTR;67cceefb7319d98b88464f81c5a7786d78ab42600008f117be2884821df7636a
2;1509715953;AES256-CTR;05ea263650495760de8e517c25ce2e430710b3251624268c0ee923fb1886e64d
3;1509716141;PLAIN;
# Tell cockroach to use a PLAIN cipher for data encryption.
$ cockroach start <regular options> \
    --rocksdb-preamble-format \
    --enterprise-encryption-keys=store=/mnt/data,keys=/path/to/cockroach.keys
```

Examine the logs or node debug pages to see that the node encryption status is now plaintext. It is now safe to delete key 2 from the file.

Examine logs and debug pages to see progress of data encryption. This may take some time.

## Contributor impact

The biggest impact of this change on contributors is the fact that all data on a given node must be encrypted.

There are three main categories:
* using the store rocksdb instance: encryption is done automatically
* using a separate rocksdb instance: encryption settings **must** be given to the new instance. Care must be taken to ensure that users know not to place store keys on the same disks as the rocksdb directory
* using anything other than rocksdb: logs (written at the Go level) are marked out of scope for this document. However, any raw data written to disk should use the same encryption settings as the node

# Reference-level explanation

## Detailed design

### Encryption layer for rocksdb

We use the new `rocksdb::EncryptedEnv` introduced in [PR 2424](https://github.com/facebook/rocksdb/pull/2424).

The `Env` in rocksdb is the interface to filesystem operations with the default env being used to interface
with Posix file systems.
A wrapper env (such as the encrypted env) implements the `Env` interface but can perform its own logic and
make use of another env.

The `EncryptedEnv` takes an `EncryptionProvider` with the following spec:
```
// The encryption provider is used to create a cipher stream for a specific file.
// The returned cipher stream will be used for actual encryption/decryption
// actions.
class EncryptionProvider {
 public:
    virtual ~EncryptionProvider() {};

    // GetPrefixLength returns the length of the prefix that is added to every file
    // and used for storing encryption options.
    // For optimal performance, the prefix length should be a multiple of
    // the a page size.
    virtual size_t GetPrefixLength() = 0;

    // CreateNewPrefix initialized an allocated block of prefix memory
    // for a new file.
    virtual Status CreateNewPrefix(const std::string& fname, char *prefix, size_t prefixLength) = 0;

    // CreateCipherStream creates a block access cipher stream for a file given
    // given name and options.
    virtual Status CreateCipherStream(const std::string& fname, const EnvOptions& options,
      Slice& prefix, unique_ptr<BlockAccessCipherStream>* result) = 0;
};
```

This allows for the injection of a data prefix (called preamble in this document) at the beginning of every file.

This prefix can be used to store encryption-related information specific to the file such as cipher in use,
key ID, initialization vector, and counter.

We propose a custom implementation of the `EncryptionProvider` using a 4KiB preamble (matching the page size).
This preamble will be written to every file written through rocksdb. The contents are generated at the time a
file is first written, and are available on subsequent file operations.

The encryption provider has access to the list of data keys and knows which is the active data key.
There are two instances of the encryption provider:
* storee encryption provider: holds the store keys, used on the file storing the data keys
* data encryption provider: holds the data keys, used on all other data written through rocksdb

Some specific details of the encryption provider:
```
CreateNewPrefix()
  if encryptionDisabled
    set prefix flag to 0 (no encryption)
  else
    set prefix flag to 1 (AES-CTR)
    set key ID to current active key ID
    set pseudo-random IV and counter

CreateCipherStream()
  if prefix.flag == 0 (no encryption)
    return PassThroughStream
  else
    return CTRCipherStream(prefix.keyID, prefix.IV, prefix.Counter)
```

The two lower-level block access streams are:
* `PassThroughStream`: `Encrypt` and `Decrypt` return without modifying the data
* `CTRCipherStream`: CTR stream cipher already present in rocksdb. Is provided an AES block cipher.

### Preamble format

The preamble is a 4KiB block of data containing a number of fields:
```
-----------------------------------------------------
| encryption flag | key ID | IV | counter | padding |
-----------------------------------------------------
```

| Field | Size | Description |
| --- | --- | --- |
| encryption flag | 64 bits | uint64 flag. 0: no encryption, 1: AES-CTR encryption |
| key ID | 64 bits | uint64 key ID |
| IV | `AES::BlockSize` | initialization vector for CTR mode |
| counter | `AES::BlockSize` | uint64 counter for CTR mode |
| padding | remainder | the rest of the preamble. zero-filled |

The encryption flag acts as a switch to determine the remainder of the contents. For example:
* `flag = 0`: no encryption: ignore the rest of the preamble, do not encrypt data.
* `flag = 1`: read the fields described above and perform AES-CTR encryption/decryption.

Possible extensions of the encryption flag include:
* other modes (eg: GCM)
* other ciphers (eg: Triple DES, SkipJack, etc...)

For AES, the specific type is not included in the preamble as it is dictated by the key size.
We could encode it in the preamble to validate loaded keys or to make some reports clearer (eg: no need
for key lookup when computing encryption usage, clearer error messages on missing keys).

### Enabling the preamble format

Using a preamble as part of encryption means that data written by older versions of cockroach (unaware of preambles)
cannot be read when using the preamble format, and vice versa.

This means that we need two things:
* control the format of new rocksdb instances
* detect the format of existing rocksdb instances

We propose:
* `--rocksdb-preamble-format` to start a new node with preamble format enabled. Will fail if the data exists in classical format for any store on the node.
* a `PREAMBLE_FORMAT` file written at rocksdb-creation time. Its presence indicates use of the preamble format.


### Key levels

We introduce two levels of encryption with their corresponding keys:
* data keys:
	* used to encrypt the data itself
	* automatically generated and rotated
	* stored in the `DATA_KEYS-<sequence>` file
	* encrypted using the store keys, or plaintext when encryption is disabled
* store keys:
	* used to encrypt the list of data keys
	* provided by the user
	* plaintext
	* should be stored on a separate disk
	* should only be accessible to the cockroach process

### Key status

We have three distinct status for keys:
* active: key is being used for all new data
* in-use: key is still needed to read some data but is not being used for new data
* inactive: there is no remaining data encrypted with this key

### Key file format

The file format is the same for store keys and unencrypted data keys.

The file may be empty, contain a single key, or contain multiple keys.
```
keyid;timestamp;cipher;key
```

| Field | Type | Description |
| --- | --- | --- |
| keyid | ascii number (uint64) | key ID, greater than zero |
| timestamp | ascii number (time_t) | key creation time in seconds since epoch |
| cipher | string | cipher for this key. one of: `PLAIN`, `AES128-CTR`, `AES192-CTR`, `AES256-CTR` |
| key | hexadecimal string | hexadecimal key. empty if cipher is `PLAIN` |

Ordering does not matter, the key with highest ID is always the active encryption key.

Notice the use of a `PLAIN` cipher. This is to be able to pass the concept of "do not encrypt" through
key specification.

### Loading store keys

Store keys are stored in a plaintext file provided by the user.
At no point does cockroach modify the file.

It can be specified with `--enterprise-encryption-keys=store=<store data dir>,keys=/path/to/file.keys`

The following **should** be true:
* the file is only accessibly to the cockroach process (or user running the process)
* the file should not be on the same disk as the cockroach data

If no key file is present, a "fake" PLAIN key with ID 0 is used indicating the lack of encryption.

### Rotating store keys

The store keys file is read at startup (or other mechanisms, see [Reloading store keys](#reloading-store-keys)).

Upon opening and decrypting the data keys file if the key ID used is not the active store key, we
write a new data keys file encrypted with the active store key.

### Loading data keys

Data keys are stored in `<data dir>/DATA_KEYS-<sequence>` where `sequence` is a monotonically increasing number.
Upon startup, we look for the highest sequence file.

The file contains the preamble indicating how to decode it using store keys.

### Generating data keys

To generate a new data key, we look up the following:
* current maximum key ID
* current timestamp
* desired cipher (eg: `AES128-CTR`)

If the cipher is other than `PLAIN`, we generate a key of the desired length using the pseudorandom `CryptoPP::OS_GenerateRandomBlock(blocking=false`) (see [Random number generator](#random-number-generator) for alternatives).

We then generate the following new key entry:
* **ID**: current max key ID + 1
* **timestamp**: current time
* **cipher**: as specified
* **key**: hexadecimal representation of the created random bytes

### Rotating data keys

Rotation is the act of using a new key as the active encryption key. This can be due to:
* a new cipher is desired (including turning encryption on and off)
* a different key size is desired
* the store key was rotated
* rotation is needed (time based or other)

When a new key has been generated (see above), we build a temporary list of data keys (using the existing
data keys and the new key). We write the file with encryption to `DATA_KEYS-<sequence>` where `sequence` is a
monotonically increasing number. Upon successful write, we trigger a data key file reload.

The actual process of creating the new file should match the rocksdb method. We need to guarantee that the latest
file is correct.

Key generation is done inline at startup (we may as well wait for the new key before proceeding), but in the
background for automated changes while the system is already running.

### Reporting encryption status

We need to report basic information about the current status of encryption.

At the very least, we should have:
* log entries
* debug page entries per store

With the following information:
* preamble format support
* active store key ID and cipher
* active data key ID and cipher
* fraction of live data per key ID and cipher

Preamble format support and active key IDs and ciphers are known at all times. We need to log them when they change
(indicating successful key rotation) and propagate the information to the Go layer.

Fraction of data encoded is a bit trickier. We need to:
1. find all files in use
1. read file preambles for encryption status (key ID and cipher)
1. determine file sizes (without the preamble)
1. log a summary
1. report back to the go layer

We can find the list of all in-use files the same way rocksdb's backup does, by calling:
* `rocksdb::GetLiveFiles`: retrieve the list of all files in the database
* `rocksdb::GetSortedWalFiles`: retrieve the sorted list of all wal files

Reading each file preamble means reading the first 4KiB of each file. To avoid performing this too
frequently, we can cache results. We must take particular care with files being overwritten and
using a new key/cipher.

### Other uses of rocksdb

Any other uses of rocksdb to store raw or processed data will use the same encryption setting as the node.

This applies to:
* backup and restore
* temporary disk storage for query processing

Preamble data format support should not be a problem as both cases involve temporary data: we can delete all
existing data and re-create the rocksdb instance with preamble support.

In the case of multiple stores with different encryption settings, we must pick one.
We propose using the keys from the first encrypted store we find.

Documentation must mention the same restriction about key and data colocation: the store keys should not be on the
same disk as the temporary date.

### Enterprise enforcement

Gating at-rest-encryption on the presence of a valid enterprise license is problematic due to the fact that
we have no contact with the cluster when deciding to use encryption.

For now, we propose a reactive approach to license enforcement. When any node in the cluster uses encryption
(determined through node metrics) but we do not have a valid license:
* display a large warning on the admin UI
* log large messages on each encrypted node (perhaps periodically)
* look into "advise" or "motd" type functionality in SQL. This is rumored to be unreliable.

The overall idea is that the cluster is not negatively impacted by the lack of an enterprise license.
See [Enterprise feature gating](#enterprise-feature-gating) for possible alternatives.

## Security considerations

Caveat: this is not a thorough security analysis of the proposed solution, let alone its implementation.

This section should be expanded and studied carefully before this RFC is approved.

### No write access by attackers

A big assumption in this document is that attackers do not have write access to the raw files while
we are operating: we trust the integrity of the store and data key files as well as all data written on disk.

We can relax this assumption by adding integrity checking to all files on disk (eg: using GCM).
This would add complexity and cost to filesystem-level operations in rocksdb as we would need to read entire
files to compute authentication tags.

However, GCM can be cheaply used to encode data keys.

### Random number generator

We need to generate random values for a few things:
* data keys
* IV/counter for each file

Crypto++ provides [OS_GenerateRandomBlock](https://www.cryptopp.com/wiki/RandomNumberGenerator#OS_Entropy)
which can operate in blocking (using `/dev/random`) or non-blocking (using `/dev/urandom`) mode.
We would prefer to use better entropy for data keys, but `/dev/random` is notoriously slow especially
when just starting rocksdb with very little disk/network utilization.

Generating data keys (other than the first one, or when changing encryption ciphers) can be done
in the background so we may be able to use the higher entropy `/dev/random`.
IVs may be safe to keep generating using the lower-entropy `/dev/urandom`.

### Memory safety

We need to provide safety for the keys while held in memory.
On the C++ side, this done using mlock (`man mlock(2)`) on memory holding unencrypted keys.
This prevents the corresponding memory pages from being paged out to disk. This does not prevent
memory access for users with sufficient privileges.

There is no equivalent in Go so the current approach is to avoid loading keys in Go.
This can become problematic if we want to reuse the keys to encrypt log files written in Go.
No good answer presents itself.

## Drawbacks

Implementing encryption-at-rest as proposed has a few drawbacks (in no particular order):

### Directs us towards rocksdb-level encryption

While rocksdb-level encryption does not force us to keep encryption-at-rest at this level,
it strongly discourages us from implementing it elsewhere.

This means that more fine-grained encryption (eg: per column) will need to fit within this
model or will require encryption in a completely different part of the system.

### Cannot migrate to/from preamble data format

This is already discussed elsewhere in this document with the conclusion that we will not
migrate from old data versions to new data versions supporting encryption. Instead, new stores
must be created with the preamble data format.

Alternatively, we could design a system to keep track of encryption status outside the contents
of the files (eg: a list of files and their encryption status, absence of the file denoting
it was written by an older version) but this seems overly complex and fragile.

### Lack of correctness testing of rocksdb encryption layer

The rocksdb `env_encryption` functionality is barely tested and has no known open-source uses.
This raises serious concerns about the correctness of the proposed approach.

We can improve testing of this functionality at the rocksdb level as well as within cockroach.
A testing plan must be developped and implemented to provide some assurances of correctness.

## Rationale and Alternatives

There are a few alternatives available in the major aspects of this design as well as in
specific areas. We address them all here (in no particular order):

### Filesystem encryption

Filesystem encryption can be used without requiring coordination with cockroach or rocksdb.
While this may be an option in some environments, DBAs do not always have sufficient
privileges to use this or may not be willing to.

Filesystem encryption can still be used with cockroach independently of at-rest-encryption.
This can be a reasonable solution for non-enterprise users.

Should we choose this alternative, this entire RFC can be ignored.

### Fine-grained encryption

The solution proposed here allows encryption to be enabled or not for individual rocksdb instances.
This may not be sufficient for fine-grained encryption.

Database and table-level encryption can be accomplished by integrating store encryption status with
zone configs, allowing the placement of certain databases/tables on encrypted disks. This approach is
rather heavy-handed and may not be suitable for all cases of database/table-level encryption.

However, this may not be sufficient for more fine-grained encryption (eg: per column).
It's not clear how encryption for individual keys/values would work.

### Single level of keys

The current choice of two key levels (store keys vs data keys) is debatable:

Advantages:
* rotating store keys is cheap: re-encrypt the list of data keys. Users can deprecated old keys quickly.
* a third-party system could provide us with other types of keys and not impact data encryption

Negated advantage:
* if the store key is compromised, we still need to re-encrypt all data quickly, this does not help

Cons:
* more complicated logic (we have two sets of keys to worry about)
* encryption status is harder to understand for users

We could instead use a single level of keys where the user-provided keys are directly used to encode the data.
This would simplify the logic and reporting (and user understanding). This would however make rotation slower
and potentially make integration with third-party services more difficult. User-provided keys would have to be
available until no data uses them.

### Specifying keys

Since we only need two store keys, we could specify them as individual files rather than encoded within
a file.

The advantage of the store key file as described is the ability to specify a creation timestamp and cipher mode.

### Relationship between store and data keys

The current proposal uses the same cipher and key size for store and data keys.

Pros:
* more user friendly: only have to specify one cipher
* less chance of mistake when switching encryption on/off

Cons:
* it's not possible to specify a different cipher for store keys

### Enterprise feature gating

The current proposal does gate encryption on a valid license due to the fact that we cannot check the license
when initialising the node.

A possible solution to explore is detection when the node joins a cluster. eg:
* always allow node encryption
* when a node joins, communicate its encryption status and refuse the join if no enterprise license exists
* on bootstrap, an encrypted node will only allow SQL operations on the system tables (to set the license)
* the license can be passed through `init`

This would still cause issues when removing the license (or errors loading/validating the license).

Less drastic actions may be possible.

## Unresolved questions

Before this RFC can be marked as approved, we have a few open questions (in no particular order):

### Non-live rocksdb files

Some files (eg: backups) generated by rocksdb are not included in the "Live files" or "WAL files".
Those files will still be encrypted with the data keys but will not be included in the key-usage computation,
meaning the backup will not count towards key usage and will become unreadable when the key is GCed.

While we do not currently make use of backups, we have in the past and may again.

### Forcing re-encryption

We need to find a way to force re-encryption when we want to remove an old key.
While rocksdb regularly creates new files, we may need to force rewrite for less-frequently
updated files. Other files (such as `MANIFEST`, `OPTIONS`, `CURRENT`, `IDENTITY`, etc...) may need
a different method to rewrite.

Compaction (of the entire key space, or specific ranges determined through live file metadata) may provide
the bulk of the needed functionality.
However, some files (especially with no updates) will not be rewritten.

Some possible solutions to investigate:
* patches to rocksdb to force rotation even if nothing has changed (may be the safest)
* "poking" at the files to add changes (may be impossible to do properly)
* level of indirection in the encryption layer while a file is being rewritten (not supposed to be done for "write-once" files, and probably too dangerous for logs)

### Per-store flags

We introduce two new flags with per-store settings. Other options include:
* single `--enterprise-encryption=store=<store path>,keys=<keys path>[,period=<rotation period>]`
* new fields in the `--store` flag

### Heterogeneous stores on the same node

The current proposal requires all stores to use the preamble format, or none.
* **pros**: easier scenarios: single flag controls all stores
* **cons**: it's not possible to have an existing store in classic data format, then add a store in preamble format

It allows encryption on a single store.
* **pros**: more flexibility: can have one encrypted store, one plain, or different ciphers
* **cons**: flags are tedious: we need to specify per-store keys and rotation periods

### Choice of keys for other rocksdb instances

In a multi-store scenario, other rocksdb instances (eg: backup/restore, temporary space) need to determine
whether to use encryption, and which keys to use.

We can default to:
* use encryption if any of the stores on this node uses encryption
* use the first set of keys we come across (probably my store ID)

### Garbage collection of old data keys

We would prefer not to keep old data keys forever, but we need to be certain that a key is no longer in use
before deleting it. How feasible this is depends on the accuracy of our key usage computation.

### Marking data keys as "exposed" if key list was plaintext at any point

Consider the following scenario:
1. Encryption enabled: data key 1 is in use. Data key list is encrypted.
1. Encryption disabled: new data is plaintext, but key 1 is still in use. Data key list is plaintext.
1. Encryption enabled: new data is encrypted with key 2, some plaintext data remains, some data encrypted with key 1 remains. Data key list is encrypted.

At this point, encryption usage accounting will report the data encrypted by key 1 as "encrypted".
However, during the transition through plaintext, key 1 was readable (data key list was plaintext), meaning the
remaining data encrypted with key 1 should be reported as "not encrypted".

A possible solution would be to mark key 1 as "exposed" in the data keys file and take that into consideration when
reporting encryption status. This status must be set on all keys present in the data key list when it is in plaintext.

Note: "exposed" may be a scary word. We would have to find the appropriate wording for this.

### Processing encrypted data on a non-encrypted node

In a heterogeneous cluster (some nodes are encrypted, some are not), data stored on encrypted nodes should
never hit a disk unencrypted. This means that any processing of the data must be done on similarly-encrypted
nodes.

This most likely needs to be enforced at a much higher level when deciding where to do intermediate data processing.

### Performance impact

The performance impact needs to be measure for a variety of workloads and for all supported ciphers.
This is needed to provide some guidance to users.

### Instruction set support

Crypto++ can determine support for SSE2 and AES-NI at runtime. We should ensure that our builds properly
enable this.

## Future improvements

The improvements listed here can be added later or may be done in the initial implementation.
This is in addition to some of the features mentioned in the Out Of Scope section.

### Encryption-related metrics

We can export high-level metrics about at-rest-encryption through prometheus.
This can include:
* encryption status (enabled/disabled/not-possible-on-this-store)
* amount of encrypted data per key ID
* amount of data per cipher (or plaintext)
* age of in-use keys

### Propagating encrypted status

We may want to automatically mark a store as "encrypted" and make this status available to zone configuration,
allowing database/table placement to specify encryption status.

When to mark a store as "encrypted" is not clear. For example: can we mark it as encrypted just because encryption
is enabled, or should we wait until encryption usage is at 100%?

### Reloading store keys

The current proposal only reloads store keys at node start time.
We can avoid restarts by triggering a refresh of the store key file when receiving a signal (eg: `SIGHUP`) or other
conditions (periodic refresh, admin UI endpoint, filesystem polling, etc...)

### Tooling

At the very least, we want `cockroach debug` tools to continue working correctly with preamble/encrypted files.
We may also need to add tools to convert an existing data directory from/to preample format or encryption, or
to report the current encryption status of a store (without cockroach running).

We should examine which rocksdb-provided tools may need modification as well, possibly involving patches
to rocksdb.

### Support for additional block ciphers

Crypto++ supports multiple block ciphers. It should be reasonably easy to add support for
other ciphers such as Triple DES, Skipjack (both NIST-recommended), and others.

### GCM for data integrity

GCM (Galois Counter Mode) is similar to CTR mode but provides data integrity as well, meaning
we can verify that the encrypted data has not been tampered with.

Implementing GCM would require additional changes to the raw storage format to store the final
authentication tag.
