# Phone Bill Benchmark


## Requirements

* Java `>= 11`
  * The `JAVA_HOME` environment variable should be set.


## How to Build

```sh
cd src
./gradlew distTar
```
Upon successful execution, `phone-bill.tar.gz` will be generated in the `src/build/distributions` directory.


## Installation

To install, extract `phone-bill.tar.gz` in any directory of your choice:

```sh
tar xf phone-bill.tar.gz
```

After extracting, a new directory named phone-bill will be created. We will refer to this directory as $PHONE_BILL in the following sections.


## Usage

This section provides instructions on how to execute the Phone Bill Benchmark.

### Prerequisites

Ensure `tsurugidb` is running on the server where the benchmark is to be executed.

### Creating Tables

```sh
$PHONE_BILL/bin/create_table.sh $PHONE_BILL/conf/batch-only
```


### Generating Test Data

```sh
$PHONE_BILL/bin/create_test_data.sh $PHONE_BILL/conf/batch-only
```


###  Running the Benchmark

```sh
$PHONE_BILL/bin/execute.sh $PHONE_BILL/conf/batch-only
```

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)




