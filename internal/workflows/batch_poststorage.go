package workflows

import (
	"fmt"
	"time"

	temporalsdk_temporal "go.temporal.io/sdk/temporal"
	temporalsdk_workflow "go.temporal.io/sdk/workflow"

	"github.com/artefactual-sdps/cva-enduro-workflows/internal/activities"
	"github.com/artefactual-sdps/cva-enduro-workflows/internal/config"
	"github.com/artefactual-sdps/cva-enduro-workflows/internal/types"
)

type Outcome int

const (
	OutcomeSuccess Outcome = iota
	OutcomeSystemError
	OutcomeContentError
)

type (
	BatchPoststorage struct {
		cfg config.Config
	}
	BatchPoststorageRequest struct {
		Batch *types.Batch
		SIPs  []*types.SIP
	}
	BatchPoststorageResult struct {
		Outcome      Outcome
		RelativePath string
	}
)

func NewBatchPoststorage(cfg config.Config) *BatchPoststorage {
	return &BatchPoststorage{cfg: cfg}
}

func (w *BatchPoststorage) Execute(
	ctx temporalsdk_workflow.Context,
	params *BatchPoststorageRequest,
) (*BatchPoststorageResult, error) {
	var (
		result BatchPoststorageResult
		err    error
	)

	logger := temporalsdk_workflow.GetLogger(ctx)
	logger.Debug("Batch post-storage workflow running!", "params", params)

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
		return nil, fmt.Errorf("Create CSV: %w", err)
	}

	result.Outcome = OutcomeSuccess
	result.RelativePath = csvResult.Key

	return &result, nil
}

func withFilesysOpts(ctx temporalsdk_workflow.Context, d time.Duration) temporalsdk_workflow.Context {
	return temporalsdk_workflow.WithActivityOptions(ctx, temporalsdk_workflow.ActivityOptions{
		ScheduleToCloseTimeout: d,
		RetryPolicy: &temporalsdk_temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	})
}
