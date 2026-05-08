package workflows

import (
	"fmt"
	"time"

	"github.com/artefactual-sdps/enduro/pkg/childwf"
	"github.com/artefactual-sdps/temporal-activities/bucketdelete"
	temporalsdk_workflow "go.temporal.io/sdk/workflow"

	"github.com/artefactual-sdps/cva-enduro-workflows/internal/activities"
	"github.com/artefactual-sdps/cva-enduro-workflows/internal/config"
)

type Postbatch struct {
	cfg config.PostbatchConfig
}

func NewPostbatch(cfg config.PostbatchConfig) *Postbatch {
	return &Postbatch{cfg: cfg}
}

func (w *Postbatch) Execute(
	ctx temporalsdk_workflow.Context,
	params *childwf.PostbatchParams,
) (*childwf.PostbatchResult, error) {
	logger := temporalsdk_workflow.GetLogger(ctx)
	logger.Debug("Postbatch workflow running!", "params", params)

	// Create an AtoM CSV file for all the SIPs in the batch.
	fsCtx := withFilesysOpts(ctx, 10*time.Minute)
	var csvResult activities.CreateCSVResult
	err := temporalsdk_workflow.ExecuteActivity(
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

	// Delete the ContainerMetadata.xml file for each SIP in the batch.
	for _, sip := range params.SIPs {
		key := fmt.Sprintf("%s_ContainerMetadata.xml", sip.UUID)
		fsCtx := withFilesysOpts(ctx, 1*time.Minute)
		err = temporalsdk_workflow.ExecuteActivity(
			fsCtx,
			bucketdelete.Name,
			bucketdelete.Params{
				Key: key,
			},
		).Get(fsCtx, nil)
		if err != nil {
			return nil, fmt.Errorf("delete %s from ingest bucket: %v", key, err)
		}
	}

	return &childwf.PostbatchResult{}, nil
}
