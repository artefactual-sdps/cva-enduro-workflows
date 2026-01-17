package workflow

import (
	"github.com/artefactual-sdps/cva-enduro-workflows/internal/types"
	temporalsdk_workflow "go.temporal.io/sdk/workflow"
)

type Outcome int

const (
	OutcomeSuccess Outcome = iota
	OutcomeSystemError
	OutcomeContentError
)

type BatchPoststorageWorkflowParams struct {
	SIPs []types.SIP `json:"sips"`
}

type BatchPoststorageWorkflowResult struct {
	Outcome      Outcome
	RelativePath string
}

type BatchPoststorageWorkflow struct {
	sharedPath string
}

func NewBatchPoststorageWorkflow(sharedPath string) *BatchPoststorageWorkflow {
	return &BatchPoststorageWorkflow{
		sharedPath: sharedPath,
	}
}

func (w *BatchPoststorageWorkflow) Execute(
	ctx temporalsdk_workflow.Context,
	params *BatchPoststorageWorkflowParams,
) (*BatchPoststorageWorkflowResult, error) {
	var (
		result BatchPoststorageWorkflowResult
		e      error
	)

	logger := temporalsdk_workflow.GetLogger(ctx)
	logger.Debug("Batch post-storage workflow running!", "params", params)

	return &result, e
}
