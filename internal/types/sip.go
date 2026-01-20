package types

import (
	"github.com/google/uuid"
)

type SIP struct {
	UUID  uuid.UUID
	Name  string
	AIPID *uuid.UUID // Nullable.
}
