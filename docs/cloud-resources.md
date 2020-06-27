# Cloud Resources

This document attempts to catalog the cloud resources created by or required by
scripts in this repository. At the time of writing, 16 August 2017, this
document was not nearly comprehensive; please do not take it as such.

## Test Fixtures

Our acceptance tests push quite a bit of data around. For a rough sense of
scale, the biggest allocator tests create clusters with hundreds of gigabytes of
data, and the biggest backup/restore tests push around several terabytes of
data. This is far too much data to store in a VM image and far too much data to
generate on demand, so we stash "test fixtures" in cloud blob storage.

At the moment, all our test fixtures live in [Azure Blob
Storage][azure-blob-storage], Azure's equivalent of Amazon S3. The object
hierarchy looks like this:

* **roachfixtures{region}/**
  * **backups/** — the output of `BACKUP... TO 'azure://roachfixtures/backups/FOO'`,
                   used to test `RESTORE` without manually running a backup.
    * **2tb/**
    * **tpch{1,5,10,100}/**
  * **store-dumps/** — gzipped tarballs of raw stores (i.e., `cockroach-data`
                       directories), used to test allocator rebalancing and
                       backups without manually inserting gigabytes of data.
    * **1node-17gb-841ranges/** - source: `RESTORE` of `tpch10`
    * **1node-113gb-9595ranges/** - source: `tpch100 IMPORT`
    * **3nodes-17gb-841ranges/** - source: `RESTORE` of `tpch10`
    * **6nodes-67gb-9588ranges/** - source: `RESTORE` of `tpch100`
    * **10nodes-2tb-50000ranges/**
  * **csvs/** — huge CSVs used to test distributed CSV import (`IMPORT...`).

*PLEA(benesch):* Please keep the above list up to date if you add additional
objects to the storage account. It's very difficult to track down an object's
origin story.

Note that egress bandwidth is *expensive*. Every gigabyte of outbound traffic
costs 8¢, so one 2TB restore costs approximately $160. Data transfer within a
region, like from a storage account in the `eastus` region to a VM in the
`eastus` region, is not considered outbound traffic, however, and so is free.

Ideally, we'd limit ourselves to one region and frolic in free bandwidth
forever. In practice, of course, things are never simple. The `eastus` region
doesn't support newer VMs, and we want to test backup/restore on both old and
new VMs. So we duplicate the `roachfixtures` storage accounts in each region we
spin up acceptance tests. At the moment, we have the following storage accounts:

* **`roachfixtureseastus`** — missing new VMs
* **`roachfixtureswestus`** — has new VMs

### Syncing `roachfixtures{region}` Buckets

By far the fastest way to interact with Azure Blob Storage is the
[`azcopy`][azcopy] command. It's a bit of a pain to install, but the Azure CLIs
(`az` and `azure`) don't attempt to parallelize operations and won't come close
to saturating your network bandwidth.

Here's a sample invocation to sync data from `eastus` to `westus`:

```shell
for container in $(az storage container list --account-name roachfixtureseastus -o tsv --query '[*].name')
do
  azcopy --recursive --exclude-older --sync-copy \
    --source "https://roachfixtureseastus.blob.core.windows.net/$container" \
    --destination "https://roachfixtureswestus.blob.core.windows.net/$container" \
  --source-key "$source_key" --dest-key "$dest_key"
done
```

Since egress is expensive and ingress is free, be sure to run this on an
azworker located in the source region—`eastus` in this case.

You can fetch the source and destination access keys from the Azure Portal or
with the following Azure CLI 2.0 command:

```shell
az storage account keys list -g fixtures -n roachfixtures{region} -o tsv --query '[0].value'
```

TODO(benesch): install `azcopy` on azworkers.

TODO(benesch): set up a TeamCity build to sync fixtures buckets automatically.

## Ephemeral Storage

Backup acceptance tests need a cloud storage account to use as the backup
destination. These backups don't need to last beyond the end of each acceptance
test, and so the files are periodically cleaned up to avoid paying for
unnecessary storage.

TODO(benesch): automate cleaning out ephemeral data. Right now, the process is
entirely manual.

To avoid accidentally deleting fixture data—which can take several *days* to
regenerate—the ephemeral data is stored in separate storage accounts from the
backup data. Currently, we maintain the following storage accounts:

* **`roachephemeraleastus`**
* **`roachephemeralwestus`**

Some acceptance tests follow a backup to ephemeral storage with a restore from
that same ephemeral storage, triggering both ingress and egress bandwidth. To
avoid paying for this egress bandwidth, we colocate storage accounts with the
VMs running the acceptance tests, as we do for test fixtures.

[azcopy]: https://docs.microsoft.com/en-us/azure/storage/storage-use-azcopy-linux
[azure-blob-storage]: https://docs.microsoft.com/en-us/azure/storage/storage-introduction#blob-storage
