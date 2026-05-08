package workflows_test

import (
	"fmt"
	"testing"

	"github.com/artefactual-sdps/enduro/pkg/childwf"
	"github.com/artefactual-sdps/temporal-activities/bucketdelete"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.artefactual.dev/tools/bucket"
	"go.artefactual.dev/tools/ref"
	temporalsdk_activity "go.temporal.io/sdk/activity"
	temporalsdk_testsuite "go.temporal.io/sdk/testsuite"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/memblob"

	"github.com/artefactual-sdps/cva-enduro-workflows/internal/activities"
	"github.com/artefactual-sdps/cva-enduro-workflows/internal/config"
	"github.com/artefactual-sdps/cva-enduro-workflows/internal/workflows"
)

type PostbatchTestSuite struct {
	suite.Suite
	temporalsdk_testsuite.WorkflowTestSuite

	env *temporalsdk_testsuite.TestWorkflowEnvironment

	// Each test registers the workflow with a different name to avoid
	// duplicates.
	workflow *workflows.Postbatch

	// bucket is a blobs.Bucket used for reports in tests.
	bucket *blob.Bucket
}

func TestPostbatch(t *testing.T) {
	suite.Run(t, new(PostbatchTestSuite))
}

func (s *PostbatchTestSuite) SetupWorkflowTest(cfg config.Config) {
	s.env = s.NewTestWorkflowEnvironment()

	b, err := bucket.NewWithConfig(s.T().Context(), cfg.IngestBucket)
	s.Require().NoError(err)
	s.bucket = b

	s.env.RegisterActivityWithOptions(
		activities.NewCreateCSV(s.bucket).Execute,
		temporalsdk_activity.RegisterOptions{Name: activities.CreateCSVName},
	)

	s.env.RegisterActivityWithOptions(
		bucketdelete.New(s.bucket).Execute,
		temporalsdk_activity.RegisterOptions{Name: bucketdelete.Name},
	)

	s.workflow = workflows.NewPostbatch(cfg.Postbatch)
}

func (s *PostbatchTestSuite) TearDownTest() {
	s.bucket.Close()
}

func (s *PostbatchTestSuite) TestHappyPath() {
	batch := &childwf.PostbatchBatch{
		UUID:      uuid.MustParse("8fdfaea1-06ed-4cf6-8bdf-d15d80420f35"),
		SIPSCount: 1,
	}
	sip := &childwf.PostbatchSIP{
		UUID:  uuid.MustParse("22222222-3333-4444-5555-666666666666"),
		Name:  "Test SIP",
		AIPID: ref.New(uuid.MustParse("11111111-2222-3333-4444-555555555555")),
	}

	s.SetupWorkflowTest(config.Config{
		IngestBucket: &bucket.Config{URL: "mem://"},
	})

	s.env.OnActivity(
		activities.CreateCSVName,
		mock.AnythingOfType("*context.timerCtx"),
		&activities.CreateCSVParams{
			Batch: batch,
			SIPs:  []*childwf.PostbatchSIP{sip},
		},
	).Return(
		&activities.CreateCSVResult{
			Key: fmt.Sprintf("batch_%s.csv", batch.UUID),
		},
		nil,
	)

	s.env.OnActivity(
		bucketdelete.Name,
		mock.AnythingOfType("*context.timerCtx"),
		&bucketdelete.Params{
			Key: fmt.Sprintf("%s_ContainerMetadata.xml", sip.UUID),
		},
	).Return(nil, nil)

	s.env.ExecuteWorkflow(s.workflow.Execute, &childwf.PostbatchParams{
		Batch: batch,
		SIPs:  []*childwf.PostbatchSIP{sip},
	})

	s.True(s.env.IsWorkflowCompleted())

	var result childwf.PostbatchResult
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal(childwf.OutcomeSuccess, result.Outcome)
}
