package workflows

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/artefactual-sdps/enduro/pkg/childwf"
	"github.com/artefactual-sdps/temporal-activities/bagcreate"
	"github.com/artefactual-sdps/temporal-activities/bucketupload"
	"github.com/google/uuid"
	temporalsdk_workflow "go.temporal.io/sdk/workflow"

	"github.com/artefactual-sdps/cva-enduro-workflows/internal/config"
)

type Preprocesssing struct {
	cfg config.PreprocessingConfig
}

func NewPreprocessing(cfg config.PreprocessingConfig) *Preprocesssing {
	return &Preprocesssing{cfg: cfg}
}

func (w *Preprocesssing) Execute(
	ctx temporalsdk_workflow.Context,
	params *childwf.PreprocessingParams,
) (*childwf.PreprocessingResult, error) {
	var (
		result childwf.PreprocessingResult
		err    error
	)

	logger := temporalsdk_workflow.GetLogger(ctx)
	logger.Debug("Preprocessing workflow running!", "params", params)

	// Upload the ContainerMetadata.xml file only if this SIP is part of a
	// batch; single SIPs don't write a Batch CSV file, so the metadata is
	// not needed.
	if params.BatchID != uuid.Nil {
		uploadTask := result.NewTask(temporalsdk_workflow.Now(ctx), "Upload ContainerMetadata.xml")

		err = w.uploadContainerMDFile(ctx, params)
		if err != nil {
			logger.Error("Task failed with error", "task", uploadTask.Name, "error", err)
			result.SystemError(
				temporalsdk_workflow.Now(ctx),
				uploadTask,
				"An error occurred when uploading the ContainerMetadata.xml file to the Enduro ingest bucket. Please try again, or ask a system administrator to investigate.",
			)
			return &result, nil
		}
		uploadTask.Succeed(
			temporalsdk_workflow.Now(ctx),
			"ContainerMetadata.xml file uploaded to the Enduro ingest bucket",
		)
	}

	// Bag the SIP for Enduro processing.
	bagTask := result.NewTask(temporalsdk_workflow.Now(ctx), "Bag SIP")

	var createBag bagcreate.Result
	err = temporalsdk_workflow.ExecuteActivity(
		withFilesysOpts(ctx, 10*time.Minute),
		bagcreate.Name,
		&bagcreate.Params{
			SourcePath: filepath.Join(w.cfg.SharedPath, params.RelativePath),
		},
	).Get(ctx, &createBag)
	if err != nil {
		logger.Error("Task failed with error", "task", bagTask.Name, "error", err)
		result.SystemError(
			temporalsdk_workflow.Now(ctx),
			bagTask,
			"An error occurred when bagging the SIP. Please try again, or ask a system administrator to investigate.",
		)
		return &result, nil
	}
	bagTask.Succeed(temporalsdk_workflow.Now(ctx), "SIP has been bagged")

	result.RelativePath = params.RelativePath

	return &result, nil
}

// uploadContainerMDFile uploads the ContainerMetadata.xml file from the SIP to
// the Enduro ingest bucket so it can be read by the postbatch workflow after
// preservation processing. The key of the uploaded file is set to
// "<SIPID>_ContainerMetadata.xml" to make it unique.
func (w *Preprocesssing) uploadContainerMDFile(
	ctx temporalsdk_workflow.Context,
	params *childwf.PreprocessingParams,
) error {
	path := filepath.Join(
		w.cfg.SharedPath,
		params.RelativePath,
		"metadata",
		"submissionDocumentation",
		"ContainerMetadata.xml",
	)
	key := fmt.Sprintf("%s_ContainerMetadata.xml", params.SIPID)

	fsCtx := withFilesysOpts(ctx, 10*time.Minute)
	var res bucketupload.Result
	err := temporalsdk_workflow.ExecuteActivity(
		fsCtx,
		bucketupload.Name,
		&bucketupload.Params{
			Path:       path,
			Key:        key,
			BufferSize: 100_000_000,
		},
	).Get(fsCtx, &res)
	if err != nil {
		return fmt.Errorf("upload ContainerMetadata.xml file: %w", err)
	}

	return nil
}
