package config_test

import (
	"testing"

	"github.com/artefactual-sdps/temporal-activities/bagcreate"
	"go.artefactual.dev/tools/bucket"
	"gotest.tools/v3/assert"
	"gotest.tools/v3/fs"

	"github.com/artefactual-sdps/cva-enduro-workflows/internal/config"
)

const testConfig = `# Config
debug = true
verbosity = 2
[ingestBucket]
endpoint = "http://minio.enduro-sdps:9000"
pathStyle = true
accessKey = "minio"
secretKey = "minio123"
region = "us-west-1"
bucket = "enduro-ingest"
[temporal]
address = "temporal.enduro-sdps:7233"
namespace = "default"
[worker]
maxConcurrentSessions = 1
taskQueue = "cva-enduro"
[preprocessing]
workflowName = "preprocessing"
sharedPath = "/home/enduro/shared"
[preprocessing.bagCreate]
checksumAlgorithm = "sha256"
[postbatch]
workflowName = "postbatch"
`

func TestConfig(t *testing.T) {
	t.Parallel()

	type test struct {
		name            string
		configFile      string
		toml            string
		wantFound       bool
		wantCfg         config.Config
		wantErr         string
		wantErrContains string
	}

	for _, tc := range []test{
		{
			name:       "Loads configuration from a TOML file",
			configFile: "cva-enduro.toml",
			toml:       testConfig,
			wantFound:  true,
			wantCfg: config.Config{
				Debug:     true,
				Verbosity: 2,
				Temporal: config.TemporalConfig{
					Address:   "temporal.enduro-sdps:7233",
					Namespace: "default",
				},
				Worker: config.WorkerConfig{
					MaxConcurrentSessions: 1,
					TaskQueue:             "cva-enduro",
				},
				Preprocessing: config.PreprocessingConfig{
					WorkflowName: "preprocessing",
					SharedPath:   "/home/enduro/shared",
					BagCreate: bagcreate.Config{
						ChecksumAlgorithm: "sha256",
					},
				},
				Postbatch: config.PostbatchConfig{WorkflowName: "postbatch"},
				IngestBucket: &bucket.Config{
					Endpoint:  "http://minio.enduro-sdps:9000",
					PathStyle: true,
					AccessKey: "minio",
					SecretKey: "minio123",
					Region:    "us-west-1",
					Bucket:    "enduro-ingest",
				},
			},
		},
		{
			name:       "Errors when configuration values are not valid",
			configFile: "cva-enduro.toml",
			toml: `# override default values to trigger validation errors
[temporal]
namespace = ""
`,
			wantFound: true,
			wantErr: `invalid configuration
Temporal.Address: missing required value
Temporal.Namespace: missing required value
Worker.TaskQueue: missing required value
Preprocessing.WorkflowName: missing required value
Preprocessing.SharedPath: missing required value
Postbatch.WorkflowName: missing required value`,
		},
		{
			name:       "Errors when MaxConcurrentSessions is less than 1",
			configFile: "cva-enduro.toml",
			toml: `# Config
[ingestBucket]
url = "file:///home/enduro/reports"
[temporal]
address = "temporal.enduro-sdps:7233"
namespace = "default"
[worker]
maxConcurrentSessions = -1
taskQueue = "cva-enduro"
[preprocessing]
workflowName = "preprocessing"
sharedPath = "/home/enduro/shared"
[preprocessing.bagCreate]
checksumAlgorithm = "sha256"
[postbatch]
workflowName = "postbatch"
`,
			wantFound: true,
			wantErr: `invalid configuration
Worker.MaxConcurrentSessions: -1 is less than the minimum value (1)`,
		},
		{
			name:       "Errors when TOML is invalid",
			configFile: "cva-enduro.toml",
			toml:       "bad TOML",
			wantFound:  true,
			wantErr:    "failed to read configuration file: While parsing config: toml: expected character =",
		},
		{
			name:            "Errors when no config file is found in the default paths",
			wantFound:       false,
			wantErrContains: "Config File \"cva-enduro.toml\" Not Found in \"[",
		},
		{
			name:            "Errors when the given configFile is not found",
			configFile:      "missing.toml",
			wantFound:       false,
			wantErrContains: "configuration file not found: ",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tmpDir := fs.NewDir(t, "cva-enduro", fs.WithFile("cva-enduro.toml", tc.toml))

			configFile := ""
			if tc.configFile != "" {
				configFile = tmpDir.Join(tc.configFile)
			}

			var c config.Config
			found, configFileUsed, err := config.Read(&c, configFile)
			if tc.wantErr != "" {
				assert.Equal(t, found, tc.wantFound)
				assert.Error(t, err, tc.wantErr)
				return
			}
			if tc.wantErrContains != "" {
				assert.Equal(t, found, tc.wantFound)
				assert.ErrorContains(t, err, tc.wantErr)
				return
			}

			assert.NilError(t, err)
			assert.Equal(t, found, true)
			assert.Equal(t, configFileUsed, configFile)
			assert.DeepEqual(t, c, tc.wantCfg)
		})
	}
}
