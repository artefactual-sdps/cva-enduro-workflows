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

### Preprocessing workflow

The preprocessing workflow moves the ContainerMetadata.xml to the ingest bucket
in preparation for the postbatch workflow and bags the SIP before sending it to
Archivematica.

This workflow is registered as `preprocessing`.

1. Check for batch ID and upload ContainerMetadata.xml.
   - Check for a batch ID. If there isn't one, skip this step, because a single
     SIP doesn't get a batch CSV.
   - Read `ContainerMetadata.xml`. This is the metadata export from VanDocs.
   - Upload `ContainerMetadata.xml` to the Enduro ingest bucket as
     `<SIP-UUID>_ContainerMetadata.xml`. The postbatch workflow reads it from
     the ingest bucket.
   - This shows up in the Enduro UI as the task "Upload ContainerMetadata.xml".

2. Bag the SIP.
   - Run the `bagcreate` activity on the SIP directory, using sha512 checksums
     by default. This turns the SIP into a BagIt bag.
   - This shows up as the task "Bag SIP".

### Postbatch workflow

The postbatch workflow generates a AtoM-compliant CSV file that lists each SIP
in the batch. When uploaded to AtoM, the CSV will result in one information
object for each SIP. The postbatch workflow runs once per batch.

This workflow is registered as `batch-csv`.

1. Create the AtoM CSV - `create-csv-activity`, with a 10-minute timeout.
   - Create a CSV file in the ingest bucket at `reports/batch_<UUID>.csv`. If
     the batch has a custom identifier, the name is
     `reports/batch_<identifier>_<UUID>. csv` instead.
   - Create a header row in the CSV, using the standard AtoM RAD information
     object CSV import format.
   - The activity then adds information to the CSV for each SIP:
     - If the SIP has no AIP ID, it gets skipped (this means that the
       preservation workflow didn't produce an AIP.)
     - If the SIP does have an AIP ID, the activity reads
       `<SIP-UUID>_ContainerMetadata.xml` (created in the preprocessing
       workflow) from the Enduro ingest bucket and parses the contents as
       shown in the table below.
     - This process is repeated for each SIP

2. Clean up.
   - For each SIP in the batch, delete `<SIP-UUID>_ContainerMetadata.xml` from
     the ingest bucket using `bucketdelete`, with a 1-minute timeout per file. 

#### Metadata mapping

| CSV column | Source |
|---|---|
| `legacyId` | The SIP's position in the batch (1, 2, 3…) |
| `qubitParentSlug` | `Classification`, with `PD-`, `VPD-` or `VPL-` in front if `OPR` starts with that code |
| `acquisition` | "VanDocs transfer: " followed by `Consignment` |
| `eventTypes` / `eventDates` / `eventStartDates` / `eventEndDates` / `eventActors` | Up to two events, separated by pipes. **Creation** runs from `DateRegistered` to `DateClosed`. **Recordkeeping** has `HomeLocation` as the actor. An empty value becomes `NULL`. |
| `identifier` | "F" followed by the part of `RecordNumber` after the "/" (for example, `01-1000-30/0000007` becomes `F0000007`) |
| `alternativeIdentifiers` / `alternativeIdentifierLabels` | The AIP UUID (labelled "AIP UUID") and, if present, `RecordNumber` (labelled "VanDocs container record number") |
| `title` | `TitleFreeTextPart` |
| `extentAndMedium` | "N digital documents", where N is Enduro's file count |
| `radGeneralMaterialDesignation` | Always "Multiple media" |
| `levelOfDescription` | Always "File" |
| `culture` | Always "en" |
| `publicationStatus` | Always "draft" |
| `accessConditions` | Always: "This file has not been reviewed for potential FOIPPA restrictions. Access is pending review and may be delayed. See archivist for details." |

### Other activities

The preprocessing child workflow (see the [preprocessing.go] file) also uses a
number of other more general Enduro temporal activites, including:

- `bagcreate`
- `bucketdelete`
- `bucketupload`

[Enduro development manual]: https://enduro.readthedocs.io/dev-manual/devel/
[go]: https://go.dev/doc/install
[make]: https://www.gnu.org/software/make/
[gcc]: https://gcc.gnu.org/
[preprocessing.go]: (https://github.com/artefactual-sdps/cva-enduro-workflows/blob/main/internal/workflows/preprocessing.go)
[postbatch.go]: (https://github.com/artefactual-sdps/cva-enduro-workflows/blob/main/internal/workflows/postbatch.go)
