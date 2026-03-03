package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/artefactual-sdps/temporal-activities/bagcreate"
	"github.com/spf13/viper"
	"go.artefactual.dev/tools/bucket"
)

type Config struct {
	// Debug toggles human readable logs or JSON logs (default).
	Debug bool

	// Verbosity sets the verbosity level of log messages, with 0 (default)
	// logging only critical messages and each higher number increasing the
	// number of messages logged.
	Verbosity int

	// Temporal configures the Temporal client.
	Temporal TemporalConfig

	// Worker configures the Temporal worker.
	Worker WorkerConfig

	// Preprocessing configures the preprocessing workflow.
	Preprocessing PreprocessingConfig

	// Postbatch configures the postbatch workflow.
	Postbatch PostbatchConfig

	// IngestBucket configuration.
	IngestBucket *bucket.Config
}

func (c Config) Validate() error {
	return errors.Join(
		c.Temporal.Validate(),
		c.Worker.Validate(),
		c.Preprocessing.Validate(),
		c.Postbatch.Validate(),
	)
}

type TemporalConfig struct {
	// Address is the Temporal server host and port (required).
	Address string

	// Namespace is the Temporal client namespace (default: "default").
	Namespace string
}

func (c TemporalConfig) Validate() error {
	var errs error

	if c.Address == "" {
		errs = errors.Join(errs, errRequired("Temporal.Address"))
	}
	if c.Namespace == "" {
		errs = errors.Join(errs, errRequired("Temporal.Namespace"))
	}

	return errs
}

type WorkerConfig struct {
	// MaxConcurrentSessions limits the number of workflow sessions the worker
	// can handle simultaneously (default: 1).
	MaxConcurrentSessions int

	// TaskQueue is the Temporal task queue from which the worker will pull
	// tasks (default: "cva-enduro").
	TaskQueue string
}

func (c WorkerConfig) Validate() error {
	var errs error

	if c.TaskQueue == "" {
		errs = errors.Join(errs, errRequired("Worker.TaskQueue"))
	}

	// Verify that MaxConcurrentSessions is >= 1.
	if c.MaxConcurrentSessions < 1 {
		errs = errors.Join(errs, fmt.Errorf(
			"Worker.MaxConcurrentSessions: %d is less than the minimum value (1)",
			c.MaxConcurrentSessions,
		))
	}

	return errs
}

type PreprocessingConfig struct {
	// WorkflowName is the preprocessing Temporal workflow name (required).
	WorkflowName string

	// BagCreate configures the bagcreate activity used in the preprocessing
	// workflow.
	BagCreate bagcreate.Config

	// SharedPath is the shared directory where Enduro puts SIPs for
	// preprocessing (required).
	SharedPath string
}

func (c PreprocessingConfig) Validate() error {
	var errs error

	if c.WorkflowName == "" {
		errs = errors.Join(errs, errRequired("Preprocessing.WorkflowName"))
	}
	if c.SharedPath == "" {
		errs = errors.Join(errs, errRequired("Preprocessing.SharedPath"))
	}

	errs = errors.Join(errs, c.BagCreate.Validate())

	return errs
}

type PostbatchConfig struct {
	// WorkflowName is the postbatch Temporal workflow name (required).
	WorkflowName string

	// ProcessingDir is the local directory where the postbatch workflow
	// downloads files from the ingest bucket for processing (required).
	ProcessingDir string
}

func (c PostbatchConfig) Validate() error {
	var errs error

	if c.WorkflowName == "" {
		errs = errors.Join(errs, errRequired("Postbatch.WorkflowName"))
	}
	if c.ProcessingDir == "" {
		errs = errors.Join(errs, errRequired("Postbatch.ProcessingDir"))
	}

	return errs
}

func Read(config *Config, configFile string) (found bool, configFileUsed string, err error) {
	v := viper.New()

	v.AddConfigPath(".")
	v.AddConfigPath("$HOME/.config/")
	v.AddConfigPath("/etc")
	v.SetConfigName("cva-enduro")
	v.SetEnvPrefix("CVA_ENDURO")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Defaults.
	v.SetDefault("Temporal.Namespace", "default")
	v.SetDefault("Worker.MaxConcurrentSessions", 1)
	v.SetDefault("Preprocessing.BagCreate.ChecksumAlgorithm", "sha512")

	if configFile != "" {
		// Viper will not return a viper.ConfigFileNotFoundError error when
		// SetConfigFile() is passed a path to a file that doesn't exist, so we
		// need to check ourselves.
		if _, err := os.Stat(configFile); errors.Is(err, os.ErrNotExist) {
			return false, "", fmt.Errorf("configuration file not found: %s", configFile)
		}

		v.SetConfigFile(configFile)
	}

	if err = v.ReadInConfig(); err != nil {
		switch err.(type) {
		case viper.ConfigFileNotFoundError:
			return false, "", err
		default:
			return true, "", fmt.Errorf("failed to read configuration file: %w", err)
		}
	}

	err = v.Unmarshal(config)
	if err != nil {
		return true, "", fmt.Errorf("failed to unmarshal configuration: %w", err)
	}

	if err := config.Validate(); err != nil {
		return true, "", errors.Join(errors.New("invalid configuration"), err)
	}

	return true, v.ConfigFileUsed(), nil
}

func errRequired(name string) error {
	return fmt.Errorf("%s: missing required value", name)
}
