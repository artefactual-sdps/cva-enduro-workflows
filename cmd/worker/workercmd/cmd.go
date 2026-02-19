package workercmd

import (
	"context"
	"fmt"

	"github.com/artefactual-sdps/temporal-activities/bagcreate"
	"github.com/artefactual-sdps/temporal-activities/bucketupload"
	"github.com/go-logr/logr"
	"go.artefactual.dev/tools/bucket"
	"go.artefactual.dev/tools/temporal"
	temporalsdk_activity "go.temporal.io/sdk/activity"
	temporalsdk_client "go.temporal.io/sdk/client"
	temporalsdk_interceptor "go.temporal.io/sdk/interceptor"
	temporalsdk_worker "go.temporal.io/sdk/worker"
	temporalsdk_workflow "go.temporal.io/sdk/workflow"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"

	"github.com/artefactual-sdps/cva-enduro-workflows/internal/activities"
	"github.com/artefactual-sdps/cva-enduro-workflows/internal/config"
	"github.com/artefactual-sdps/cva-enduro-workflows/internal/workflows"
)

const Name = "cva-enduro-worker"

type Main struct {
	logger         logr.Logger
	cfg            config.Config
	ingestBucket   *blob.Bucket
	temporalWorker temporalsdk_worker.Worker
	temporalClient temporalsdk_client.Client
}

func NewMain(logger logr.Logger, cfg config.Config) *Main {
	return &Main{
		logger: logger,
		cfg:    cfg,
	}
}

func (m *Main) Run(ctx context.Context) error {
	c, err := temporalsdk_client.Dial(temporalsdk_client.Options{
		HostPort:  m.cfg.Temporal.Address,
		Namespace: m.cfg.Temporal.Namespace,
		Logger:    temporal.Logger(m.logger.WithName("temporal")),
	})
	if err != nil {
		m.logger.Error(err, "Unable to create Temporal client.")
		return err
	}
	m.temporalClient = c

	w := temporalsdk_worker.New(m.temporalClient, m.cfg.Worker.TaskQueue, temporalsdk_worker.Options{
		EnableSessionWorker:               true,
		MaxConcurrentSessionExecutionSize: m.cfg.Worker.MaxConcurrentSessions,
		Interceptors: []temporalsdk_interceptor.WorkerInterceptor{
			temporal.NewLoggerInterceptor(m.logger.WithName("worker")),
		},
	})
	m.temporalWorker = w

	b, err := bucket.NewWithConfig(ctx, m.cfg.IngestBucket)
	if err != nil {
		m.logger.Error(err, "Failed to open ingest bucket.")
		return err
	}
	m.ingestBucket = b

	m.registerPreprocessingWorkflow()
	m.registerPostbatchWorkflow()

	if err := w.Start(); err != nil {
		m.logger.Error(err, "Worker failed to start.")
		return err
	}

	return nil
}

func (m *Main) Close() error {
	if m.temporalWorker != nil {
		m.temporalWorker.Stop()
	}

	if m.temporalClient != nil {
		m.temporalClient.Close()
	}

	if m.ingestBucket != nil {
		if err := m.ingestBucket.Close(); err != nil {
			return fmt.Errorf("close ingest bucket: %w", err)
		}
	}

	return nil
}

func (m *Main) registerPreprocessingWorkflow() {
	m.temporalWorker.RegisterWorkflowWithOptions(
		workflows.NewPreprocessing(m.cfg).Execute,
		temporalsdk_workflow.RegisterOptions{Name: m.cfg.Preprocessing.WorkflowName},
	)

	m.temporalWorker.RegisterActivityWithOptions(
		bucketupload.New(m.ingestBucket).Execute,
		temporalsdk_activity.RegisterOptions{Name: bucketupload.Name},
	)

	m.temporalWorker.RegisterActivityWithOptions(
		bagcreate.New(m.cfg.Preprocessing.BagCreate).Execute,
		temporalsdk_activity.RegisterOptions{Name: bagcreate.Name},
	)
}

func (m *Main) registerPostbatchWorkflow() {
	m.temporalWorker.RegisterWorkflowWithOptions(
		workflows.NewPostbatch(m.cfg).Execute,
		temporalsdk_workflow.RegisterOptions{Name: m.cfg.Postbatch.WorkflowName},
	)

	m.temporalWorker.RegisterActivityWithOptions(
		activities.NewCreateCSV(m.ingestBucket).Execute,
		temporalsdk_activity.RegisterOptions{Name: activities.CreateCSVName},
	)
}
