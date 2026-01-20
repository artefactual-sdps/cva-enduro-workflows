package types

import (
	"github.com/google/uuid"
)

type Batch struct {
	UUID      uuid.UUID
	SIPSCount int
}
