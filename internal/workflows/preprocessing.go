package workflows

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/artefactual-sdps/temporal-activities/bagcreate"
	"github.com/artefactual-sdps/temporal-activities/bucketupload"
	"github.com/google/uuid"
	temporalsdk_workflow "go.temporal.io/sdk/workflow"

	"github.com/artefactual-sdps/cva-enduro-workflows/internal/config"
	"github.com/artefactual-sdps/cva-enduro-workflows/internal/enums"
	"github.com/artefactual-sdps/cva-enduro-workflows/internal/tasks"
)

type (
	Preprocesssing struct {
		cfg config.PreprocessingConfig
	}
	PreprocessingRequest struct {
		RelativePath string
		SIPID        uuid.UUID
	}
	PreprocessingResult struct {
		Outcome           Outcome
		RelativePath      string
		PreservationTasks []*tasks.Task
	}
)

func NewPreprocessing(cfg config.PreprocessingConfig) *Preprocesssing {
	return &Preprocesssing{cfg: cfg}
}

func (r *PreprocessingResult) completeTask(ctx temporalsdk_workflow.Context, task *tasks.Task, message string) {
	task.Complete(
		temporalsdk_workflow.Now(ctx),
		enums.TaskOutcomeSuccess,
		message,
	)
	r.PreservationTasks = append(r.PreservationTasks, task)
}

func (r *PreprocessingResult) systemError(
	ctx temporalsdk_workflow.Context,
	task *tasks.Task,
	msg string,
	err error,
) (*PreprocessingResult, error) {
	r.Outcome = OutcomeSystemError

	logger := temporalsdk_workflow.GetLogger(ctx)
	logger.Error("Task failed with error", "task", task.Name, "error", err)

	task.Complete(
		temporalsdk_workflow.Now(ctx),
		enums.TaskOutcomeSystemFailure,
		fmt.Sprintf("System error: %v", msg),
	)

	r.PreservationTasks = append(r.PreservationTasks, task)

	return r, nil
}

func (w *Preprocesssing) Execute(
	ctx temporalsdk_workflow.Context,
	params *PreprocessingRequest,
) (*PreprocessingResult, error) {
	var (
		result PreprocessingResult
		err    error
	)

	logger := temporalsdk_workflow.GetLogger(ctx)
	logger.Debug("Preprocessing workflow running!", "params", params)

	uploadTask := tasks.New(temporalsdk_workflow.Now(ctx), "Upload ContainerMetadata.xml")

	err = w.uploadContainerMDFile(ctx, params)
	if err != nil {
		return result.systemError(
			ctx,
			uploadTask,
			"An error occurred when uploading the ContainerMetadata.xml file to the Enduro ingest bucket. Please try again, or ask a system administrator to investigate.",
			err,
		)
	}
	result.completeTask(ctx, uploadTask, "ContainerMetadata.xml file uploaded to the Enduro ingest bucket")

	// Bag the SIP for Enduro processing.
	bagTask := tasks.New(temporalsdk_workflow.Now(ctx), "Bag SIP")

	var createBag bagcreate.Result
	err = temporalsdk_workflow.ExecuteActivity(
		withFilesysOpts(ctx, 10*time.Minute),
		bagcreate.Name,
		&bagcreate.Params{
			SourcePath: filepath.Join(w.cfg.SharedPath, params.RelativePath),
		},
	).Get(ctx, &createBag)
	if err != nil {
		return result.systemError(
			ctx,
			bagTask,
			"An error occurred when bagging the SIP. Please try again, or ask a system administrator to investigate.",
			err,
		)
	}
	result.completeTask(ctx, bagTask, "SIP has been bagged")

	result.Outcome = OutcomeSuccess
	result.RelativePath = params.RelativePath

	return &result, nil
}

// uploadContainerMDFile uploads the ContainerMetadata.xml file from the SIP to
// the Enduro ingest bucket so it can be read by the postbatch workflow after
// preservation processing. The key of the uploaded file is set to
// "<SIPID>_ContainerMetadata.xml" to make it unique.
func (w *Preprocesssing) uploadContainerMDFile(
	ctx temporalsdk_workflow.Context,
	params *PreprocessingRequest,
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
