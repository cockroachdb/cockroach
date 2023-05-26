## Description

The **roachprod-microbench** command is useful for orchestrating microbenchmarks on a roachprod cluster for analysis. 

It operates on a portable test binaries archive that has been built with the `dev test-binaries` tooling.
Compressed and uncompressed `tar` archives are supported, since the `dev` tooling can produce both.

It should be noted that the archive has to adhere to a specific layout and contains `run.sh` scripts for each package, which facilitates execution.
This tool only supports binaries archives produced in this format by the `dev test-binaries` tooling.

As part of the orchestration, test binaries are copied to the target cluster and extracted.
Once the cluster is ready, the tool will first probe for all available microbenchmarks and then execute the microbenchmarks according to any flags that have been set.

## Usage

For additional information on usage, run the **roachprod-microbench** help command:

```bash
./roachprod-microbench --help
```
