package types

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
)

// SIP represents a SIP.
type SIP struct {
	ID     int
	UUID   uuid.UUID
	Name   string
	AIPID  uuid.NullUUID // Nullable.
	Status string

	// It defaults to CURRENT_TIMESTAMP(6) so populated as soon as possible.
	CreatedAt time.Time

	// Nullable, populated as soon as processing starts.
	StartedAt sql.NullTime

	// Nullable, populated as soon as ingest completes.
	CompletedAt sql.NullTime

	// Set if there is a failure in workflow, it can be empty.
	FailedAs string

	// Object key from the failed SIP/PIP in the internal bucket.
	FailedKey string

	// Uploader is the user that uploaded the SIP.
	Uploader *User

	// Batch is the batch this SIP belongs to.
	Batch *Batch
}
