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
)

type (
	Preprocesssing struct {
		cfg config.Config
	}
	PreprocessingRequest struct {
		RelativePath string
		SIPID        uuid.UUID
	}
	PreprocessingResult struct {
		Outcome      Outcome
		RelativePath string
	}
)

func NewPreprocessing(cfg config.Config) *Preprocesssing {
	return &Preprocesssing{cfg: cfg}
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

	uploadResult, err := w.uploadContainerMDFile(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("preprocessing: upload ContainerMetadata.xml file: %w", err)
	}

	logger.Debug(
		"Uploaded ContainerMetadata.xml file to ingest bucket",
		"key", uploadResult.Key,
	)

	// Bag the SIP for Enduro processing.
	var createBag bagcreate.Result
	err = temporalsdk_workflow.ExecuteActivity(
		withFilesysOpts(ctx, 10*time.Minute),
		bagcreate.Name,
		&bagcreate.Params{
			SourcePath: filepath.Join(w.cfg.Preprocessing.SharedPath, params.RelativePath),
		},
	).Get(ctx, &createBag)
	if err != nil {
		return nil, fmt.Errorf("preprocessing: bag SIP: %w", err)
	}

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
) (*bucketupload.Result, error) {
	path := filepath.Join(
		w.cfg.Preprocessing.SharedPath,
		params.RelativePath,
		"metadata",
		"submissionDocumentation",
		"ContainerMetadata.xml",
	)
	key := fmt.Sprintf("%s_ContainerMetadata.xml", params.SIPID)

	fsCtx := withFilesysOpts(ctx, 10*time.Minute)
	var result bucketupload.Result
	err := temporalsdk_workflow.ExecuteActivity(
		fsCtx,
		bucketupload.Name,
		&bucketupload.Params{
			Path:       path,
			Key:        key,
			BufferSize: 100_000_000,
		},
	).Get(fsCtx, &result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}
