package workflows

import (
	"fmt"
	"time"

	"github.com/artefactual-sdps/temporal-activities/bucketdownload"
	temporalsdk_workflow "go.temporal.io/sdk/workflow"

	"github.com/artefactual-sdps/cva-enduro-workflows/internal/activities"
	"github.com/artefactual-sdps/cva-enduro-workflows/internal/config"
	"github.com/artefactual-sdps/cva-enduro-workflows/internal/types"
)

type (
	Postbatch struct {
		cfg config.Config
	}
	PostbatchRequest struct {
		Batch *types.Batch
		SIPs  []*types.SIP
	}
	PostbatchResult struct {
		Outcome      Outcome
		RelativePath string
	}
)

func NewPostbatch(cfg config.Config) *Postbatch {
	return &Postbatch{cfg: cfg}
}

func (w *Postbatch) Execute(
	ctx temporalsdk_workflow.Context,
	params *PostbatchRequest,
) (*PostbatchResult, error) {
	var (
		result PostbatchResult
		err    error
	)

	logger := temporalsdk_workflow.GetLogger(ctx)
	logger.Debug("Postbatch workflow running!", "params", params)

	// Download the ContainerMetadata.xml files for the batch from the ingest
	// bucket.
	for _, sip := range params.SIPs {
		key := fmt.Sprintf("%s_ContainerMetadata.xml", sip.UUID)
		fsCtx := withFilesysOpts(ctx, 10*time.Minute)
		var dlResult bucketdownload.Result
		err = temporalsdk_workflow.ExecuteActivity(
			fsCtx,
			bucketdownload.Name,
			&bucketdownload.Params{
				DirPath: w.cfg.Postbatch.ProcessingDir,
				Key:     key,
			},
		).Get(fsCtx, &dlResult)
		if err != nil {
			return nil, fmt.Errorf("download ContainerMetadata.xml for SIP %s: %w", sip.UUID, err)
		}

		logger.Debug(
			"Downloaded ContainerMetadata.xml from ingest bucket",
			"sip", sip.UUID,
			"key", key,
			"path", dlResult.FilePath,
		)
	}

	// Create an AtoM CSV file for all the SIPs in the batch.
	fsCtx := withFilesysOpts(ctx, 10*time.Minute)
	var csvResult activities.CreateCSVResult
	err = temporalsdk_workflow.ExecuteActivity(
		fsCtx,
		activities.CreateCSVName,
		activities.CreateCSVParams{
			Batch: params.Batch,
			SIPs:  params.SIPs,
		},
	).Get(fsCtx, &csvResult)
	if err != nil {
		return nil, fmt.Errorf("create CSV: %w", err)
	}

	result.Outcome = OutcomeSuccess
	result.RelativePath = csvResult.Key

	return &result, nil
}
