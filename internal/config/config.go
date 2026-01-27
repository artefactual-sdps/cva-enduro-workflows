package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
	"go.artefactual.dev/tools/bucket"
)

type ConfigValidator interface {
	Validate() error
}

type Config struct {
	// Debug toggles human readable logs or JSON logs (default).
	Debug bool

	// Verbosity sets the verbosity level of log messages, with 0 (default)
	// logging only critical messages and each higher number increasing the
	// number of messages logged.
	Verbosity int

	// IngestBucket configuration.
	IngestBucket *bucket.Config

	// Temporal configures the Temporal server address and workflow information.
	Temporal Temporal

	// Worker configures the Temporal worker.
	Worker WorkerConfig
}

type Temporal struct {
	// Address is the Temporal server host and port (default: "localhost:7233").
	Address string

	// Namespace is the Temporal namespace of the worker (default: "default").
	Namespace string

	// TaskQueue is the Temporal task queue from which the worker will pull
	// tasks (required).
	TaskQueue string

	// WorkflowName is the Temporal workflow name (required).
	WorkflowName string
}

type WorkerConfig struct {
	// MaxConcurrentSessions limits the number of workflow sessions the worker
	// can handle simultaneously (default: 1).
	MaxConcurrentSessions int
}

func (c Config) Validate() error {
	var errs error

	// Verify that the required fields have values.
	if c.IngestBucket == nil {
		errs = errors.Join(errs, errRequired("IngestBucket"))
	}
	if c.Temporal.TaskQueue == "" {
		errs = errors.Join(errs, errRequired("Temporal.TaskQueue"))
	}
	if c.Temporal.WorkflowName == "" {
		errs = errors.Join(errs, errRequired("Temporal.WorkflowName"))
	}

	// Verify that MaxConcurrentSessions is >= 1.
	if c.Worker.MaxConcurrentSessions < 1 {
		errs = errors.Join(errs, fmt.Errorf(
			"Worker.MaxConcurrentSessions: %d is less than the minimum value (1)",
			c.Worker.MaxConcurrentSessions,
		))
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
	v.SetDefault("Worker.MaxConcurrentSessions", 1)

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
		return true, "", errors.Join(errors.New("invalid configuration:"), err)
	}

	return true, v.ConfigFileUsed(), nil
}

func errRequired(name string) error {
	return fmt.Errorf("%s: missing required value", name)
}
