package types

import (
	"fmt"
	"time"

	"github.com/artefactual-sdps/cva-enduro-workflows/internal/enums"
)

const eventDateFmt = "2006-01-02"

type Event struct {
	Type  enums.EventType
	Start time.Time
	End   time.Time
	Actor string
}

// An event is considered zero-value if Start, End, and Actor are empty/zero.
func (e Event) IsZero() bool {
	return e.Start.IsZero() && e.End.IsZero() && e.Actor == ""
}

// FormatDates returns a string representation of the event's Start and End
// years separated by a hyphen (e.g. "2001-2015").
//
// If both dates are zero, an empty string is returned.
// If the Start date is zero, only the End date is returned (e.g. "2015").
// If the End date is zero, the Start date is returned followed by a hyphen and
// four spaces (e.g. "2001-    ").
// If the Start and End years are equal, only the Start year is returned
// (e.g. "2001").
func (e Event) FormatDates() string {
	const f = "2006"
	switch {
	case e.Start.IsZero() && e.End.IsZero():
		return ""
	case e.Start.IsZero():
		return e.End.Format(f)
	case e.End.IsZero():
		return fmt.Sprintf("%s-    ", e.Start.Format(f))
	case e.Start.Format(f) == e.End.Format(f):
		return e.Start.Format(f)
	default:
		return fmt.Sprintf("%s-%s", e.Start.Format(f), e.End.Format(f))
	}
}

// FormatStart returns a string representation of the event's Start time
// formatted according to the provided layout. If Start is zero, an empty
// string is returned.
func (e Event) FormatStart() string {
	if e.Start.IsZero() {
		return ""
	}
	return e.Start.Format(eventDateFmt)
}

// FormatEnd returns a string representation of the event's End time formatted
// according to the provided layout. If End is zero, an empty string is
// returned.
func (e Event) FormatEnd() string {
	if e.End.IsZero() {
		return ""
	}
	return e.End.Format(eventDateFmt)
}

func (e Event) GetType() string {
	return string(e.Type)
}

func (e Event) GetActor() string {
	return e.Actor
}
