- Feature Name: Encryption at rest
- Status: in-progress
- Start Date: 2017-11-01
- Authors: Marc Berhault
- RFC PR: [#19785](https://github.com/cockroachdb/cockroach/pull/19785)
- Cockroach Issue: [#19783](https://github.com/cockroachdb/cockroach/issues/19783)


Table of Contents
=================

   * [Summary](#summary)
   * [Motivation](#motivation)
   * [Related resources](#related-resources)
   * [Out of scope](#out-of-scope)
   * [Security analysis](#security-analysis)
      * [Attack profiles](#attack-profiles)
      * [Assumptions](#assumptions)
      * [Considerations](#considerations)
   * [Guide-level explanation](#guide-level-explanation)
      * [Terminology](#terminology)
      * [User-level explanation](#user-level-explanation)
         * [Configuration recommendations](#configuration-recommendations)
         * [Store keys](#store-keys)
         * [Data keys](#data-keys)
         * [User control of encryption](#user-control-of-encryption)
      * [Contributor impact](#contributor-impact)
   * [Reference-level explanation](#reference-level-explanation)
      * [Detailed design](#detailed-design)
         * [Store version](#store-version)
         * [Switching Env](#switching-env)
         * [COCKROACHDB_REGISTRY](#cockroachdb_registry)
         * [Encrypted Env](#encrypted-env)
         * [Key levels](#key-levels)
         * [Key status](#key-status)
         * [Store keys files](#store-keys-files)
         * [Key Manager](#key-manager)
         * [Rotating store keys](#rotating-store-keys)
         * [Data keys file format](#data-keys-file-format)
         * [Generating data keys](#generating-data-keys)
         * [Rotating data keys](#rotating-data-keys)
         * [Reporting encryption status](#reporting-encryption-status)
         * [Other uses of local disk](#other-uses-of-local-disk)
         * [Enterprise enforcement](#enterprise-enforcement)
      * [Drawbacks](#drawbacks)
         * [Directs us towards rocksdb-level encryption](#directs-us-towards-rocksdb-level-encryption)
         * [Lack of correctness testing of rocksdb encryption layer](#lack-of-correctness-testing-of-rocksdb-encryption-layer)
         * [Complexity of configuration and monitoring](#complexity-of-configuration-and-monitoring)
         * [No strong license enforcement](#no-strong-license-enforcement)
         * [Non-live rocksdb files will rot](#non-live-rocksdb-files-will-rot)
         * [CCL code location](#ccl-code-location)
      * [Rationale and Alternatives](#rationale-and-alternatives)
         * [Filesystem encryption](#filesystem-encryption)
         * [Fine-grained encryption](#fine-grained-encryption)
         * [Single level of keys](#single-level-of-keys)
         * [Relationship between store and data keys](#relationship-between-store-and-data-keys)
         * [Directly using the data prefix format](#directly-using-the-data-prefix-format)
      * [Future improvements](#future-improvements)
         * [v1.0: a.k.a. MVP](#v10-aka-mvp)
         * [Possible future additions](#possible-future-additions)

# Summary

This feature is Enterprise.

We propose to add support for encryption at rest on cockroach nodes, with
encryption being done at the rocksdb layer for each file.

We provide CTR-mode AES encryption for all files written through rocksdb.

Keys are split into user-provided store keys and dynamically-generated data keys.
Store keys are used to encrypt the data keys. Data keys are used to encrypt the actual data.
Store keys can be rotated at the user's discretion. Data keys can be rotated automatically
on a regular schedule, relying on rocksdb churn to re-encrypt data.

Plaintext files go through the regular rocksdb interface to the filesystem. Encrypted files
go through an intermediate layer responsible for all encryption tasks.

Data can be transitioned from plaintext to encrypted and back with status being reported
continuously.

# Motivation

Encryption is desired for security reasons (prevent access from other users on the same
machine, prevent data leak through drive theft/disposal) as well as regulatory reasons
(GDPR, HIPAA, PCI DSS).

Encryption at rest is necessary when other methods of encryption are either not desirable,
or not sufficient (eg: filesystem-level encryption cannot be used if DBAs do not have
access to filesystem encryption utilities).

# Related resources

* [Crypto++](https://www.cryptopp.com/)
* [overview of block cipher modes](https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Common_modes)
* [rocksdb PR adding env_encryption](https://github.com/facebook/rocksdb/pull/2424)
* [SEI Cert C coding standard](https://wiki.sei.cmu.edu/confluence/display/c/SEI+CERT+C+Coding+Standard)

# Out of scope

The following are not in scope but should not be hindered by implementation of this RFC:
* encryption of non-rocksdb data (eg: log files)
* integration with external key storage systems such as Vault, AWS KMS, KeyWhiz
* auditing of key usage and encryption status
* integration with HSM (hardware security module) or TPM (Trusted Platform Module)
* FIPS-140-2 compliance
See [Possible future additions](#possible-future-additions) for more currently-out-of-scope features.

The following are unrelated to encryption-at-rest as currently proposed:
* encrypted backup (should be supported regardless of encryption-at-rest status)
* fine-granularity encryption (that cannot use zone configs to select encrypted replicas)
* restricting data processing on encrypted nodes (requires planning/gateway coordination)

# Security analysis

Caveat: this is not a thorough security analysis of the proposed solution, let alone its implementation.

This section should be expanded and studied carefully before this RFC is approved.

## Attack profiles

The goal of this feature is to block two attack vectors:

### Access to raw disk offline

An attacker can gain access to the disk after it has been removed from the system (eg: node decommission).
At-rest encryption should make all data on the disk useless if the following are true:
* none of the store keys are available or previously compromised
* none of the data went through a phase where either store or data encryption was `plaintext`

### Access to a running system by unprivileged user

Unprivileged users (eg: non root) should not be able to extract cockroach data even if they have access to the
raw rocksdb files.
This will still not guard against:
* privileged users (with access to store keys or memory)
* data that was at some point stored as `plaintext`

## Assumptions

Some of the assumptions here can be verified by runtime checks, but others must be satisfied by the user (see
[Configuration Recommendation](#configuration-recommendations).

### No privileged access

We assume attackers do not have privileged access on a running system. Specifically:
* store keys cannot be read
* cockroach memory cannot be directly accessed
* command line flags cannot be modified

### No write access by attackers

A big assumption in this document is that attackers do not have write access to the raw files while
we are operating: we trust the integrity of the store and data key files as well as all data written on disk.

This includes the case of an attacker removing a disk, modifying it, and re-inserting it into the cluster.

A potential future improvement is to use authenticated encryption to verify the integrity of files on disk.
This would add complexity and cost to filesystem-level operations in rocksdb as we would need to read entire
files to compute authentication tags.

However, integrity checking can be cheaply used on the data keys file.

## Considerations

### Random number generator

We need to generate random values for a few things:
* data keys
* nonce/counter for each file

Crypto++ provides [OS_GenerateRandomBlock](https://www.cryptopp.com/wiki/RandomNumberGenerator#OS_Entropy)
which can operate in blocking (using `/dev/random`) or non-blocking (using `/dev/urandom`) mode.
We would prefer to use better entropy for data keys, but `/dev/random` is notoriously slow especially
when just starting rocksdb with very little disk/network utilization.

Generating data keys (other than the first one, or when changing encryption ciphers) can be done
in the background so we may be able to use the higher entropy `/dev/random`.
nonces may be safe to keep generating using the lower-entropy `/dev/urandom`.

More research must be done into the use of `/dev/random` in multi-user environment. For example, is it possible
for an attacked to consume `/dev/random` for long enough that key generation is effectively disabled?

### IV makeup and reuse prevention

An important consideration in AES-CTR is making sure we never reuse the same IV for a given key.

The IV has a size of `AES::BlockSize`, or 128 bits. It is made of two parts:
* nonce: 96 bits, randomly generated for each file
* counter: 32 bits, incremented for each block in the file

This imposes two limits:
* maximum file size: `2^32 128-bit blocks == 64GiB`
* probability of nonce re-use after `2^32` files is `2^-32`

These limits should be sufficient for our needs.

### Safety of symmetric key hashes

Given a reasonably safe hashing algorithm, exposing the hash of the store keys should not be an issue.

Indeed, finding collisions in `sha256` is not currently easier than cracking `aes128`. Should better collision
methods be found, this is still not the key itself.

### Memory safety

We need to provide safety for the keys while held in memory.
At the C++ level, we can control two aspects:
* don't swap to disk: using `mlock` (`man mlock(2)`) on memory holding keys, preventing paging out to disk
* don't core dump: using `madvise` with `MADV_DONTDUMP` (see `man madvise(2)` on Linux) to exclude pages from core dumps.

There is no equivalent in Go so the current approach is to avoid loading keys in Go.
This can become problematic if we want to reuse the keys to encrypt log files written in Go.
No good answer presents itself.

# Guide-level explanation

## Terminology

Terminology used in this RFC:
* **data key**: a.k.a Data-encryption-key. Used to encrypt the actual on-disk data. These are generated automatically.
* **store key**: a.k.a. Key-encryption-key. Used to encrypt the set of data keys. Provided by the user.
* **active key**: the key being used to encrypt new data.
* **key rotation**: encrypting data with a new key. Rotation starts when the new key is provided and ends when no data encrypted with the old key remains.
* **plaintext**: unencrypted data.
* **Env**: rocksdb terminology for the layer between rocksdb and the filesystem.
* **Switching Env**: our new Env that can switch between plaintext and encrypted envs.

## User-level explanation

Encryption-at-rest is an optional feature that can be enabled on a per-store basis.

In order to enable encryption on a given store, the user needs two things:
* an enterprise license
* one or more store key(s)

Enabling encryption increases the store version, making downgrade to a binary before encryption impossible.

### Configuration recommendations

We identify a few configuration requirements for users to safely use encryption at rest.

**TODO**: this will need to be fleshed out when writing the docs.

* restricted access to store keys (ideally, only the cockroach user, and read-only access)
* store keys and cockroach data must not be on the same filesystem/disk (including temporary working directories)
* restricted access to all cockroach data
* disable swap
* don't enable core dumps
* reasonable key generation/rotation
* monitoring
* ideally, the store keys are not stored on the machine (use something like `keywhiz`)

### Store keys

The store key is a symmetric key provided by the user. It has the following properties:
* unique for each store
* available only to the cockroach process on the node
* not stored on the same disk as the cockroach data

Store keys are stored in raw format in files (one file per key).
eg: to generate a 128-bit key: `openssl rand 16 > store.key`

Specifying store keys is done through the `--enterprise-encryption` flag. There are two key fields in this flag:
* `key`: path to the active store key, or `plain` for plaintext (default).
* `old_key`: path to the previous store key, or `plain` for plaintext (default).

When a new `key` is specified, we must tell cockroach what the previous active key was through `old_key`.

### Data keys

Data keys are automatically generated by cockroach. They are stored in the data directory and
encrypted with the active store key. Data keys are used to encrypt the actual files inside the data
directory.

This two-level approach allows easy rotation of store keys and provides safer encryption of large amounts of
data. To rotate the store key, all we need to do is re-encrypt the file containing the data keys, leaving
the bulk of the data as is.

Data keys are generated and rotated by cockroach.
There are two parameters controlling how data keys behave:
* encryption cipher: the cipher in use for data encryption. The cipher is currently `AES CTR` with the same key
size as the store key.
* rotation period: the time before a new key is generated and used. Default value: 1 week. This can be set through a flag.

### User control of encryption

#### Recommended production configuration

The need for encryption entails a few recommended changes in production configuration:
* disable swap/core dumps: we want to avoid any data hitting disk unencrypted, this includes memory being swapped out.
* run on architectures that support the [AES-NI instruction set](https://en.wikipedia.org/wiki/AES_instruction_set).
* have a separate area (encrypted or in-memory partition, fuse-filesystem, etc...) to store the store-level keys.

#### Flag changes for the cockroach binary

We add a new flag for CCL binaries. It must be specified for each store we wish encrypted:
```
--enterprise-encryption=path=<path to store>,key=<path to key file>,old_key=<path to old key>,rotation_period=<duration>
```

The individual fields are:
* `path`: the path to the data directory of the corresponding store. This must match the path specified in `--store`
* `key`: the path to the current encryption key, or `plaintext` if we wish to use plaintext. default: `plaintext`
* `old_key`: the path to the previous encryption key. Only needed if data was already encrypted.
* `rotation_period`: how often data keys should be rotated. default: `1 week`

The flag can be specified multiple times, once for each store.

The encryption flags can specify different encryption states for different stores (eg: one encrypted one plain,
different rotation periods).

#### Enabling encryption on a store

Turning on encryption for a new store or a store currently in plaintext involves the following:

```
# Ensure your key file exists and has valid key data (correct size)
# For example, to generate a key for AES-128:
$ openssl rand 16 > /path/to/cockroach.key
# Specify the enterprise-encryption:
$ cockroach start <regular options> \
    --store=/mnt/data \
    --enterprise-encryption=path=/mnt/data,key=/path/to/cockroach.key
```

The node will generate a 128 bit data key, encrypt the list of data keys with the store key, and use AES128
encryption for all new files.

Examine the logs or node debug pages to see that encryption is now enabled and see its progress.

#### Rotating the store key

Given the previous configuration, we can generate a new store key. We must pass the previous key.

```
# Create a new 128 bit key.
$ openssl rand 16 > /path/to/cockroach.new.key
# Tell cockroach about the new key, and pass the old key (/path/to/cockroach.key)
$ cockroach start <regular options> \
    --store=/mnt/data \
    --enterprise-encryption=path=/mnt/data,key=/path/to/cockroach.new.key,old_key=/path/to/cockroach.key
```

Examine the logs or node debug pages to see that the new key is now in use.
It is now safe to delete the old key file.

#### Disabling encryption

We can switch an encrypted store back plaintext. This is done by using the special value `plaintext` in the
`key` field of the encryption flag. We need to specify the previous encryption key.

```
# Instead of a key file, use "plaintext" as the argument.
# Pass the old key to allow decrypting existing data.
$ cockroach start <regular options> \
    --store=/mnt/data \
    --enterprise-encryption=path=/mnt/data,key=plain,old_key=/path/to/cockroach.new.keys
```

Examine the logs or node debug pages to see that the store encryption status is now plaintext. It is now safe to delete the old key file.

Examine logs and debug pages to see progress of data encryption. This may take some time.

## Contributor impact

The biggest impact of this change on contributors is the fact that all data on a given store must be encrypted.

There are three main categories:
* using the store rocksdb instance: encryption is done automatically
* using a separate rocksdb instance: encryption settings **must** be given to the new instance. Care must be taken to ensure that users know not to place store keys on the same disks as the rocksdb directory
* using anything other than rocksdb: logs (written at the Go level) are marked out of scope for this document. However, any raw data written to disk should use the same encryption settings as the store

# Reference-level explanation

## Detailed design

### Store version

We introduce a new [store version](https://github.com/cockroachdb/cockroach/blob/master/pkg/storage/engine/version.go#L27) to mark switching to stores supporting encryption.

Stores are currently using `versionBeta20160331`. If no encryption flags are specified, we remain at this
version until a "reasonable" time (one or two minor stable releases) has passed.

Specifying the `--enterprise-encryption` flag increases the version to `versionSwitchingEnv`. Downgrades to
binaries that do not support this version is not possible.

### Switching Env

Rocksdb performs filesystem-level operations through an [`Env`](https://github.com/facebook/rocksdb/blob/master/include/rocksdb/env.h).

This layer can be used to provide different behavior for a number of reasons. For example:
* posix support: the default `Env`
* in-memory support: for testing or in-memory databases
* hdfs: for HDFS-backed rocksdb instances
* encryption: for file-level encryption with encryption settings stored in a 4KB data prefix
* wrapper: can override specific methods, the rest are passed through to a `base env`

We leverage the `Env` layer to implement the following behavior:
* stores at `versionBeta20160331` continue to use the default `Env`
* stores at `versionSwitchingEnv` use the switching env
* plaintext files under version `versionSwitchingEnv` use a default `Env`
* encrypted files under version `versionSwitchingEnv` use an `EncryptedEnv`

```
versionBeta20160331:    DefaultEnv

versionSwitchingEnv:    SwitchingEnv: Encrypted? no  -----> DefaultEnv
                                                 yes -----> EncryptedEnv
```

The state of a file (plaintext or encrypted) is stored in a file registry. This records the list of all
encrypted files by filename and is persisted to disk in a file named `COCKROACHDB_REGISTRY`.

For every file being operated on, the switching env must lookup its existing encryption state in the registry or the
desired encryption state for new files. If the file is plaintext, pass the operation down to the `DefaultEnv`.
If the file is encrypted, pass the operation down to the `EncryptedEnv`. For a new file, we must successfully
persist its state in the registry before proceeding with the operation.

Most `SwitchingEnv` methods will perform something like the following:
```
OpOnFile(filename)
  // Determine whether the file uses encryption (existing files) or encryption is desired (new files)
  if !registry.HasFile(filename)
    useEncryption = lookup desired encryption (from --enterprise-encryption flag)
    add filename to registry
    persist registry to disk. Error out on failure.
  else
    useEncryption = get file encryption state from registry

  // Perform the operation through the appropriate Env.
  if useEncryption
    EncryptedEnv->OpOnFile(filename)
  else
    DefaultEnv->OpOnFile(filename)
```

The registry may accumulate non-existent entries if writes fail after addition or removal fails after deletes.
It will also gather entries that are never deleted by rocksdb (eg: archives). We can clean these up
by adding a periodic [garbage collection](#garbage-collection-of-registry-entries).

### COCKROACHDB_REGISTRY

The registry is a new file containing encryption status information for files written through rocksdb.
This is similar to rocksdb's `MANIFEST`. We intentionally do not call it manifest to avoid confusion.

It is stored in the base rocksdb directory for the store and written using a `write/close/rename` method.
It is always operated on through the `DefaultEnv`.

Encrypted files are always present in the registry. Plaintext files are not registered as we cannot guarantee
their presence when operating on an existing store.

`Env` operations on files will use the registry in different ways:
* existing file: lookup its encryption state in the registry, assume plaintext if missing
* existing file if it exists, otherwise new file: lookup its encryption state in the registry. If missing, stat the file through the `DefaultEnv`. If it does not exist, see "create a new file"
* create a new file: lookup the desired encryption state. If encrypted, persist it in the registry

The registry is a serialized protocol buffer:
```
enum EncryptionRegistryVersion {
  // The only version so far.
  Base = 0;
}

message EncryptionRegistry {
  // version is currently always Base.
  int version = 1;
  repeated EncryptedFile files = 2;
}

enum EncryptionType {
  // No encryption applied, not used for the registry.
  Plaintext = 0;
  // AES in counter mode.
  AES_CTR = 1;
}

message EncryptedFile {
  Filename string = 1;
  // The type of encryption applied.
  EncryptionType type = 2;

  // Encryption fields. This may move to a separate AES-CTR message.
  // ID (hash) of the key in use, if any.
  optional bytes key_id = 3;
  // Initialization vector, of size 96 bits (12 bytes) for AES.
  optional bytes nonce = 4;
  // Counter, allowing 2^32 blocks per file, so 64GiB.
  optional uint32 counter = 5;
}
```

The registry contains all information needed to find the encryption key used for a given file and encrypt/decrypt it.

### Encrypted Env

Rocksdb has an `EncryptedEnv` introduced in [PR 2424](https://github.com/facebook/rocksdb/pull/2424).
It adds a 4KiB data block at the beginning of each file with a nonce and possible encrypted extra information.

We opt to use a slightly modified (mostly simplified) version of this encrypted env because:
* `EncryptedEnv` does not support multiple keys
* the data prefix is not needed, all encryption fields can be stored in the registry

We will use a modified version of the existing `EncryptedEnv` without data prefix.

The encrypted env uses a `CipherStream` for each file, with the cipher stream containing the necessary
information to perform encryption and decryption (cipher algorithm, key, nonce, and counter).

It also holds a reference to a key manager which can provide the active key and any older keys held.

Two instances of the encrypted env are in use:
* store encryption env: uses store keys, used to manipulate the data keys file
* data encryption env: uses data keys, used to manipulate all other files

### Key levels

We introduce two levels of encryption with their corresponding keys:
* data keys:
	* used to encrypt the data itself
	* automatically generated and rotated
	* stored in the `COCKROACHDB_DATA_KEYS` file
	* encrypted using the store keys, or plaintext when encryption is disabled
* store keys:
	* used to encrypt the list of data keys
	* provided by the user
	* should be stored on a separate disk
	* should only be accessible to the cockroach process

### Key status

We have three distinct status for keys:
* active: key is being used for all new data
* in-use: key is still needed to read some data but is not being used for new data
* inactive: there is no remaining data encrypted with this key

### Store keys files

Store keys consist of exactly two keys: the active key, and the previous key.

They are stored in separate files containing the raw key data (no encoding).

Specifying the keys in use is done through the encryption flag fields:
* `key`: path to the active key, or `plaintext` for plaintext. If not specified, `plaintext` is the default.
* `old_key`: path to the previous key, or `plaintext` for plaintext. If not specified, `plaintext` is the default.

The size of the raw key in the file dictates the cipher variant to use. Keys can be 16, 24, or 32 bytes long
corresponding to AES-128, AES-192, AES-256 respectively.

Key files are opened in read-only mode by cockroach.

### Key Manager

The key manager is responsible for holding all keys used in encryption.
It is used by the encrypted env and provides the following interfaces:
* `GetActiveKey`: returns the currently active key
* `GetKey(key hash)`: returns the key matching the key hash, if any

We identify two types of key managers:

#### Store Key Manager

The store key manager holds the current and previous store keys as specified through the `--enterprise-encryption`
flag.

Since the keys are externally provided, there is no concept of key rotation.

#### Data Key Manager

The data key manager holds the dynamically-generated data keys.

Keys are persisted to the `COCKROACHDB_DATA_KEYS` file using the `write/close/rename` method and encrypted
through an encrypted env using the store key manager.

The manager periodically generates a new data key (see [Rotating data keys](#rotating-data-keys)), keeps
the previously-active key in the list of existing keys, and marks the new key as active.

Keys must be successfully persisted to the `COCKROACHDB_DATA_KEYS` file before use.

### Rotating store keys

Rotating the store keys consists of specifying:
* `key` points to a new key file, or `plaintext` to switch to plaintext.
* `old_key` points to the key file previously used.

Upon starting (or other signal), cockroach decrypts the data keys file and re-encrypts it with the new key.
If rotation is done through a flag (as opposed to other signal), this is done before starting rocksdb.

An ID is computed for each key by taking the hash (`sha-256`) of the raw key. This key ID is stored in plaintext
to indicate which store key is used to decode the data keys file.

Any changes in active store key (actual key, key size) triggers a data key rotation.

### Data keys file format

The data keys file is an encoded protocol buffer:

```
message DataKeysRegistry {
  // Ordering does not matter.
  repeated DataKey data_keys = 1;
  repeated StoreKey store_keys = 2;
}

// EncryptionType is shared with the registry EncryptionType.
enum EncryptionType {
  // No encryption applied.
  Plaintext = 0;
  // AES in counter mode.
  AES_CTR = 1;
}

// Information about the store key, but not the key itself.
message StoreKey {
  // The ID (hash) of this key.
  optional bytes key_id = 1;
  // Whether this is the active (latest key).
  optional bool active = 2;
  // First time this key was seen (in seconds since epoch).
  optional int32 creation_time = 3;
}

// Actual data keys and related information.
message DataKey {
  // The ID (hash) of this key.
  optional bytes key_id = 1;
  // Whether this is the active (latest) key.
  optional bool active = 2;
  // EncryptionType is the type of encryption (aka: cipher) used with this key.
  EncryptionType encryption_type = 3;
  // Creation time is the time at which the key was created (in seconds since epoch).
  optional int32 creation_time = 4;
  // Key is the raw key.
  optional bytes key = 5;
  // Was exposed is true if we ever wrote the data keys file in plaintext.
  optional bool was_exposed = 6;
  // ID of the active store key at creation time.
  optional bytes creator_store_key_id = 7;
}
```

The `store_keys` field is needed to keep track of store key ages and statuses. We only need to keep the
active key but may keep previous keys for history. It does **not** store the actual key, only key hash.

The `data_keys` field contains all in-use (data encrypted with those keys is still live) keys and all information
needed to determine ciphers, ages, related store keys, etc...

`was_exposed` indicates whether the key was even written to disk as plaintext (encryption was disabled at the
store level). This will be surfaced in encryption status reports. Data encrypted by an exposed key is securely
as bad as `plaintext`.

`creator_store_key_id` is the ID of the active store key when this key was created. This enables two things:
* check the active data key's `create_store_key_id` against the active store key. Mismatch triggers rotation
* force re-encryption of all files encrypted up to some store key

### Generating data keys

To generate a new data key, we look up the following:
* current active key
* current timestamp
* desired cipher (eg: `AES128`)
* current store key ID

If the cipher is other than `plaintext`, we generate a key of the desired length using the pseudorandom `CryptoPP::OS_GenerateRandomBlock(blocking=false`) (see [Random number generator](#random-number-generator) for alternatives).

We then generate the following new key entry:
* **key_id**: the hash (`sha256`) of the raw key
* **creation_time**: current time
* **encryption_type**: as specified
* **key**: raw key data
* **create_store_key_id**: the ID of the active store key
* **was_exposed**: true if the current store encryption type is `plaintext`

### Rotating data keys

Rotation is the act of using a new key as the active encryption key. This can be due to:
* a new cipher is desired (including turning encryption on and off)
* a different key size is desired
* the store key was rotated
* rotation is needed (time based, amount of data/number of files using the current key)

When a new key has been generated (see above), we build a temporary list of data keys (using the existing
data keys and the new key).
If the current store key encryption type is `plaintext`, set `was_exposed = true` for all data keys.

We write the file with encryption to `COCKROACHDB_DATA_KEYS`. Upon successful write, we trigger a data key file reload.

We use a `write/close/rename` method to ensure correct file contents.

Key generation is done inline at startup (we may as well wait for the new key before proceeding), but in the
background for automated changes while the system is already running.

### Reporting encryption status

We need to report basic information about the current status of encryption.

At the very least, we should have:
* log entries
* debug page entries per store

With the following information:
* user-requested encryption settings
* active store key ID and cipher
* active data key ID and cipher
* fraction of live data per key ID and cipher

We can report the following encryption status:
* `plaintext`: plaintext data
* `AES-<size>`: encrypted with AES (one entry for each key size)
* `AES-<size> EXPOSED`: encrypted, but data key was exposed at some point

Active key IDs and ciphers are known at all times. We need to log them when they change
(indicating successful key rotation) and propagate the information to the Go layer.

Fraction of data encoded is a bit trickier. We need to:
1. find all files in use
1. lookup their encryption status in the registry (key ID and cipher)
1. determine file sizes
1. log a summary
1. report back to the go layer

We can find the list of all in-use files the same way rocksdb's backup does, by calling:
* `rocksdb::GetLiveFiles`: retrieve the list of all files in the database
* `rocksdb::GetSortedWalFiles`: retrieve the sorted list of all wal files

### Other uses of local disk

**Note: logs encryption is currently [Out of scope](#out-of-scope)**

All existing uses of local disk to process data must apply the desired encryption status.

Data tied to a specific store should use the store's rocksdb instance for encryption.
Data not necessarily tied to a store should be encrypted if any of the stores on the node is encrypted.

We identify some existing uses of local disk:
TODO(mberhault, mjibson, dan): make sure we don't miss anything.

1. temporary work space for dist SQL: written through a temporary instance of rocksdb. This data does not need
to be used by another rocksdb instance and does not survive node restart. We propose to use dynamically-generated
keys to encrypt the temporary rocksdb instance.
1. sideloading for restore. Local SSTables are generated using an in-memory rocksdb instance then written in go
to local disk. We must change this to either be written directly by rocksdb, or move encryption to Go. The former
is probably preferable.

In addition to making sure we cover all existing use cases, we should:
1. document that any other directories must **NOT** reside on the same disk as any keys used
1. reduce the number of entry points into rocksdb to make it harder to miss encryption setup

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

Actual code for changes proposed here will be broken into CCL and non-CCL code:
* non-CCL: switching env, modified encrypted env
* CCL: key manager(s), ciphers

## Drawbacks

Implementing encryption-at-rest as proposed has a few drawbacks (in no particular order):

### Directs us towards rocksdb-level encryption

While rocksdb-level encryption does not force us to keep encryption-at-rest at this level,
it strongly discourages us from implementing it elsewhere.

This means that more fine-grained encryption (eg: per column) will need to fit within this
model or will require encryption in a completely different part of the system.

### Lack of correctness testing of rocksdb encryption layer

The rocksdb `env_encryption` functionality is barely tested and has no known open-source uses.
This raises serious concerns about the correctness of the proposed approach.

We can improve testing of this functionality at the rocksdb level as well as within cockroach.
A testing plan must be developped and implemented to provide some assurances of correctness.

### Complexity of configuration and monitoring

Proper use of encryption-at-rest requires a reasonable amount of user education, including
* proper configuration of the system (see [Configuration recommendations](#configuration-recommendations))
* proper monitoring of encryption status

A lot of this falls onto proper documentation and admin UI components, but some are choices made here
(flag specification, logged information, surfaced encryption status).

### No strong license enforcement

The current proposal takes a reactive approach to license enforcement: we show warnings in multiple places
if encryption was enabled without an enterprise license.

This is unlike our other enterprise features which simply cannot be used without a license.

There is some discussion of possible ways to solve this in
[Enterprise feature gating](#enterprise-feature-gating), but this is left as future improvements.

### Non-live rocksdb files will rot

Any files not included in rocksdb's "Live files" will still be encrypted. However, due to not being rewritten,
they will become inaccessible as soon as the key is rotated out and GCed.

While we do not currently make use of backups, we have in the past and may again.

### CCL code location

The enterprise-related functionality should live in CCL directories as much as possible (`pkg/ccl` for go code,
`c-deps/libroach/ccl` for C++ code).

However, a lot of integration is needed. Some (but far from all) examples include:
* new flag on the `start` command
* additional fields on the `StoreSpec`
* changes to store version logic
* different objects (`Env`) for `DBImpl` construction
* encryption status reporting in node debug pages

This makes hook-based integration of CCL functionality tricky.

Making less code CCL would simplify this. But enterprise enforcement must be taken into account.

## Rationale and Alternatives

There are a few alternatives available in the major aspects of this design as well as in
specific areas. We address them all here (in no particular order):

### Filesystem encryption

This is [Out of scope](#out-of-scope)

Filesystem encryption can be used without requiring coordination with cockroach or rocksdb.
While this may be an option in some environments, DBAs do not always have sufficient
privileges to use this or may not be willing to.

Filesystem encryption can still be used with cockroach independently of at-rest-encryption.
This can be a reasonable solution for non-enterprise users.

Should we choose this alternative, this entire RFC can be ignored.

### Fine-grained encryption

This is [Out of scope](#out-of-scope)

The solution proposed here allows encryption to be enabled or not for individual rocksdb instances.
This may not be sufficient for fine-grained encryption.

Database and table-level encryption can be accomplished by integrating store encryption status with
zone configs, allowing the placement of certain databases/tables on encrypted disks. This approach is
rather heavy-handed and may not be suitable for all cases of database/table-level encryption.

However, this may not be sufficient for more fine-grained encryption (eg: per column).
It's not clear how encryption for individual keys/values would work.

### Single level of keys

**We have settled on a two-level key structure**

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

### Relationship between store and data keys

**We have settled on tied cipher/key-size specification. This can be changed easily.**

The current proposal uses the same cipher and key size for store and data keys.

Pros:
* more user friendly: only have to specify one cipher
* less chance of mistake when switching encryption on/off

Cons:
* it's not possible to specify a different cipher for store keys

### Directly using the data prefix format

The previous version of this RFC proposed using the `rocksdb::EncryptedEnv` for all files, with encryption state
(plaintext or encrypted) and encryption fields stored in the 4KiB data prefix.

The main issues of that solution are:
* cannot switch existing stores to the data prefix format, requiring new stores for encryption support
* overhead of the encrypted env for plaintext files
* lack of support for multiple keys in the existing data prefix format requiring heaving modification

## Future improvements

We break down future improvements in multiple categories:
* v1.0: may be not done as part of the initial implementation. Must be done for the first
stable release.
* future: possible additions to come after first stable release.

The features are listed in no particular order.

### v1.0: a.k.a. MVP

#### Instruction set support

Crypto++ can determine support for SSE2 and AES-NI at runtime and fall back to software implementation when
not supported.

There are a few things we can do:
* ensure out builds properly enable instruction-set detection
* surface a warning when running in software mode
* properly document instruction set requirements for optimal performance

#### Forcing re-encryption

We need to find a way to force re-encryption when we want to remove an old key.
While rocksdb regularly creates new files, we may need to force rewrite for less-frequently
updated files. Other files (such as `MANIFEST`, `OPTIONS`, `CURRENT`, `IDENTITY`, etc...) may need
a different method to rewrite.

Compaction (of the entire key space, or specific ranges determined through live file metadata) may provide
the bulk of the needed functionality.
However, some files (especially with no updates) will not be rewritten.

Some possible solutions to investigate:
* there is rumor of being able to mark sstables as "dirty"
* patches to rocksdb to force rotation even if nothing has changed (may be the safest)
* "poking" at the files to add changes (may be impossible to do properly)
* level of indirection in the encryption layer while a file is being rewritten

Part of forcing re-encryption includes:
* when to do it automatically (eg: age-based. maybe after half the active key lifetime)
* how to do it manually (user requests quick re-encryption)
* specifying what to re-encrypt (eg: all data keys up to ID 5)

#### Garbage collection of old data keys

We would prefer not to keep old data keys forever, but we need to be certain that a key is no longer in use
before deleting it. How feasible this is depends on the accuracy of our encryption status reporting.

If we choose to ignore non-live files, garbage collection should be reasonably safe.

#### Garbage collection of registry entries

All encrypted files are stored in the registry. Live rocksdb files will automatically be removed as they are
deleted, but any other files will remain forever if not deleted through rocksdb.

We may want to periodically stats all files in our registry and deleted the entries for nonexistent files.

#### Performance impact

The performance impact needs to be measured for a variety of workloads and for all supported ciphers.
This is needed to provide some guidance to users.

Guidance on key rotation period would also be helpful. This is dependent on the rocksdb churn, so will depend
on the specific workload. We may want to add metrics about data churn to our encryption status reporting.

#### Propagating encrypted status

We may want to automatically mark a store as "encrypted" and make this status available to zone configuration,
allowing database/table placement to specify encryption status.

When to mark a store as "encrypted" is not clear. For example: can we mark it as encrypted just because encryption
is enabled, or should we wait until encryption usage is at 100%?

If we use the existing store attributes for this marker, we may need to add the concept of "reserved" attributes.

#### Encryption-related metrics

We can export high-level metrics about at-rest-encryption through prometheus.
This can include:
* encryption status (enabled/disabled/not-possible-on-this-store)
* amount of encrypted data per key ID
* amount of data per cipher (or plaintext)
* age of in-use keys

#### Reloading store keys

The current proposal only reloads store keys at node start time.
We can avoid restarts by triggering a refresh of the store key file when receiving a signal (eg: `SIGHUP`) or other
conditions (periodic refresh, admin UI endpoint, filesystem polling, etc...)

#### Tooling

At the very least, we want `cockroach debug` tools to continue working correctly with encrypted files.

We should examine which rocksdb-provided tools may need modification as well, possibly involving patches
to rocksdb.

### Possible future additions

#### Safe file deletion

We may want to delete old files in a less recoverable way (some filesystems allow un-delete).
On SSDs, a single overwrite pass may be sufficient. We do not propose to handle safe deletion
on hard drives.

#### Support for additional block ciphers

Crypto++ supports multiple block ciphers. It should be reasonably easy to add support for
other ciphers.

#### Authenticated encryption for data integrity

We can switch to authenticated encryption (eg: Galois Counter Mode, or others) to allow integrity
verification of files on disk.

Implementing authenticated encryption would require additional changes to the raw storage format
to store the final authentication tag.

#### Add sanity checks

We could perform a few checks to ensure data security, such as:
* detect if keys are on the same disk as the store
* detect if keys have loose permissions
* detect if swap is enabled

#### Enterprise feature gating

The current proposal does not gate encryption on a valid license due to the fact that we cannot check the license
when initialising the node.

A possible solution to explore is detection when the node joins a cluster. eg:
* always allow store encryption
* when a node joins, communicate its encryption status and refuse the join if no enterprise license exists
* on bootstrap, an encrypted store will only allow SQL operations on the system tables (to set the license)
* the license can be passed through `init`

This would still cause issues when removing the license (or errors loading/validating the license).

Less drastic actions may be possible.
