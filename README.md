# cva-enduro-workflows

**cva-enduro-workflows** provides two Enduro child workflows for the City of
Vancouver Archives: a preprocessing child workflow and a postbatch child
workflow. The worker binary starts one Temporal worker that registers both child
workflows.

- [Configuration](#configuration)
- [Local environment](#local-environment)
- [Makefile](#makefile)

## Configuration

The worker needs to share the filesystem with Enduro's a3m or Archivematica
workers, connect to the same Temporal server, and be related to Enduro with the
correct namespace, task queue and workflow names.

### Worker configuration

An example configuration for the worker binary:

```toml
debug = false
verbosity = 0

[ingestBucket]
endpoint = "http://minio.enduro-sdps:9000"
pathStyle = true
accessKey = "minio"
secretKey = "minio123"
region = "us-west-1"
bucket = "enduro-ingest"

[temporal]
address = "temporal-frontend.enduro-sdps:7233"
namespace = "default"

[worker]
maxConcurrentSessions = 1
taskQueue = "cva-enduro"

[preprocessing]
workflowName = "preprocessing"
sharedPath = "/home/enduro/shared"

[preprocessing.bagCreate]
checksumAlgorithm = "sha512"

[postbatch]
workflowName = "batch-csv"
```

### Enduro

The child workflow sections for Enduro's configuration:

```toml
[[childWorkflows]]
type = "preprocessing"
namespace = "default"
taskQueue = "cva-enduro"
workflowName = "preprocessing"
extract = true
sharedPath = "/home/enduro/shared"

[[childWorkflows]]
type = "postbatch"
namespace = "default"
taskQueue = "cva-enduro"
workflowName = "batch-csv"
```

## Local environment

This project provides child workflows for the Enduro development environment.
The supported development workflow is to run `tilt up` from the Enduro
repository and load this repository through Enduro's `CHILD_WORKFLOW_PATHS`
mechanism.

Bring up the Enduro environment by following the [Enduro development manual].

### Set up

The specific requirements for `cva-enduro-workflows` are:

- clone this repository as a sibling of the Enduro repository
- configure `CHILD_WORKFLOW_PATHS=../cva-enduro-workflows`
- configure `MOUNT_PREPROCESSING_VOLUME=true`
- run `tilt up` from the Enduro repository

All other development workflow details, including `.tilt.env`, live updates,
starting, stopping, and clearing the environment, are documented in Enduro.
This repository can also provide local overrides through its own `.tilt.env`
file, including settings such as `TRIGGER_MODE_AUTO`.

### Requirements for development

While we run the services inside a Kubernetes cluster we recommend installing
Go and other tools locally to ease the development process.

- [Go] (1.26+)
- GNU [Make] and [GCC]

## Makefile

The Makefile provides developer utility scripts via command line `make` tasks.
Running `make` with no arguments (or `make help`) prints the help message.
Dependencies are downloaded automatically.

### Debug mode

The debug mode produces more output, including the commands executed. E.g.:

```shell
$ make env DBG_MAKEFILE=1
Makefile:10: ***** starting Makefile for goal(s) "env"
Makefile:11: ***** Fri 10 Nov 2023 11:16:16 AM CET
go env
GO111MODULE=''
GOARCH='amd64'
...
```

## Available activities

The activities documented below belong to both the preprocessing child workflow
(see [preprocessing.go]) and the post-batch child workflow (see [postbatch.go]).

### Create AtoM CSV file

Creates a CSV metadata file for all the SIPs in a batch. The CSV file can be
imported into AtoM to create an archival description for each of ingested SIPs.

**Steps**

- Create a batch CSV file in the internal ingest bucket, with a "reports/"
  prefix
- Loop through the SIPs in the batch and for each one do the following:
  - Parse the required metadata from the SIPs ContainerMetadata.xml file
  - Write a row to the CSV file for the SIP, in AtoM information object CSV
    import format

**Success criteria**

- CSV file is successfully created with all required metadata
- CSV file is stored in designated bucket
- CSV file can be uploaded to AtoM without error

### Other activities

The preprocessing child workflow (see the [preprocessing.go] file) also uses a
number of other more general Enduro temporal activites, including:

- `bagcreate`
- `bucketdelete`
- `bucketupload`
- `xmlvalidate`

[Enduro development manual]: https://enduro.readthedocs.io/dev-manual/devel/
[go]: https://go.dev/doc/install
[make]: https://www.gnu.org/software/make/
[gcc]: https://gcc.gnu.org/
[preprocessing.go]: (https://github.com/artefactual-sdps/cva-enduro-workflows/blob/main/internal/workflows/preprocessing.go)
[postbatch.go]: (https://github.com/artefactual-sdps/cva-enduro-workflows/blob/main/internal/workflows/postbatch.go)
