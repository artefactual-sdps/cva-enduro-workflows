package workflows_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/artefactual-sdps/temporal-activities/bagcreate"
	"github.com/artefactual-sdps/temporal-activities/bucketupload"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.artefactual.dev/tools/bucket"
	temporalsdk_activity "go.temporal.io/sdk/activity"
	temporalsdk_testsuite "go.temporal.io/sdk/testsuite"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/memblob"

	"github.com/artefactual-sdps/cva-enduro-workflows/internal/config"
	"github.com/artefactual-sdps/cva-enduro-workflows/internal/workflows"
)

type PreprocessingTestSuite struct {
	suite.Suite
	temporalsdk_testsuite.WorkflowTestSuite

	env *temporalsdk_testsuite.TestWorkflowEnvironment

	// Each test registers the workflow with a different name to avoid
	// duplicates.
	workflow *workflows.Preprocesssing

	// bucket is a blobs.Bucket used for reports in tests.
	bucket *blob.Bucket
}

func TestPreprocessing(t *testing.T) {
	suite.Run(t, new(PreprocessingTestSuite))
}

func (s *PreprocessingTestSuite) SetupWorkflowTest(cfg config.Config) {
	s.env = s.NewTestWorkflowEnvironment()

	b, err := bucket.NewWithConfig(s.T().Context(), cfg.IngestBucket)
	s.Require().NoError(err)
	s.bucket = b

	s.env.RegisterActivityWithOptions(
		bucketupload.New(s.bucket).Execute,
		temporalsdk_activity.RegisterOptions{Name: bucketupload.Name},
	)

	s.env.RegisterActivityWithOptions(
		bagcreate.New(cfg.Preprocessing.BagCreate).Execute,
		temporalsdk_activity.RegisterOptions{Name: bagcreate.Name},
	)

	s.workflow = workflows.NewPreprocessing(cfg)
}

func (s *PreprocessingTestSuite) TearDownTest() {
	s.bucket.Close()
}

func (s *PreprocessingTestSuite) TestHappyPath() {
	sharedPath := s.T().TempDir()
	relativePath := "SIP-01234"
	sipID := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")
	key := fmt.Sprintf("%s_ContainerMetadata.xml", sipID)

	if err := createSIP(sharedPath, relativePath); err != nil {
		s.FailNow("Unable to create SIP for test", "error", err)
	}

	s.SetupWorkflowTest(config.Config{
		IngestBucket: &bucket.Config{URL: "mem://"},
		Preprocessing: config.PreprocessingConfig{
			WorkflowName: "preprocessing-test",
			SharedPath:   sharedPath,
			BagCreate: bagcreate.Config{
				ChecksumAlgorithm: "sha512",
			},
		},
	})

	s.env.OnActivity(
		bucketupload.Name,
		mock.AnythingOfType("*context.timerCtx"),
		&bucketupload.Params{
			Path: filepath.Join(
				sharedPath,
				relativePath,
				"metadata",
				"submissionDocumentation",
				"ContainerMetadata.xml",
			),
			Key:        key,
			BufferSize: 100_000_000,
		},
	).Return(
		&bucketupload.Result{Key: key}, nil,
	)

	s.env.OnActivity(
		bagcreate.Name,
		mock.AnythingOfType("*context.timerCtx"),
		&bagcreate.Params{
			SourcePath: filepath.Join(sharedPath, relativePath),
		},
	).Return(
		&bagcreate.Result{}, nil,
	)

	s.env.ExecuteWorkflow(s.workflow.Execute, &workflows.PreprocessingRequest{
		RelativePath: relativePath,
		SIPID:        sipID,
	})

	s.True(s.env.IsWorkflowCompleted())

	var result workflows.PreprocessingResult
	s.NoError(s.env.GetWorkflowResult(&result))
	s.Equal(workflows.OutcomeSuccess, result.Outcome)
	s.Equal(relativePath, result.RelativePath)
}

func createSIP(sharedPath, relativePath string) error {
	sipPath := filepath.Join(sharedPath, relativePath)
	if err := os.MkdirAll(sipPath, 0o755); err != nil {
		return fmt.Errorf("create SIP directory: %w", err)
	}

	return nil
}
