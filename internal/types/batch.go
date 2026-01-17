package types

import (
	"time"

	"github.com/google/uuid"
)

// Batch represents a Batch.
type Batch struct {
	ID          int
	UUID        uuid.UUID
	Identifier  string
	Status      string
	SIPSCount   int
	CreatedAt   time.Time
	StartedAt   time.Time
	CompletedAt time.Time

	// Uploader is the user that uploaded the Batch.
	Uploader *User
}
