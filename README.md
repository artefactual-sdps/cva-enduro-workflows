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

[Enduro development manual]: https://enduro.readthedocs.io/dev-manual/devel/
[go]: https://go.dev/doc/install
[make]: https://www.gnu.org/software/make/
[gcc]: https://gcc.gnu.org/
