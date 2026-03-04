package types_test

import (
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/artefactual-sdps/cva-enduro-workflows/internal/enums"
	"github.com/artefactual-sdps/cva-enduro-workflows/internal/types"
)

var (
	t1 = time.Date(2020, 3, 15, 0, 0, 0, 0, time.UTC)
	t2 = time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC)
)

func TestEvent_IsZero(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		event types.Event
		want  bool
	}{
		{
			name:  "returns true for zero-value Event",
			event: types.Event{},
			want:  true,
		},
		{
			name:  "returns true when only Type is set",
			event: types.Event{Type: enums.EventTypeCreation},
			want:  true,
		},
		{
			name:  "returns false when Start is set",
			event: types.Event{Start: t1},
			want:  false,
		},
		{
			name:  "returns false when End is set",
			event: types.Event{End: t2},
			want:  false,
		},
		{
			name:  "returns false when Actor is set",
			event: types.Event{Actor: "Jane Doe"},
			want:  false,
		},

		{
			name: "returns false when all fields are set",
			event: types.Event{
				Type:  enums.EventTypeRecordkeeping,
				Start: t1,
				End:   t2,
				Actor: "Jane Doe",
			},
			want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.event.IsZero())
		})
	}
}

func TestEvent_FormatDates(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		event types.Event
		want  string
	}{
		{
			name:  "returns an empty string when both times are zero",
			event: types.Event{},
			want:  "",
		},
		{
			name:  "returns a year range when both Start and End are set",
			event: types.Event{Start: t1, End: t2},
			want:  "2020-2024",
		},
		{
			name:  "returns a single year when Start and End are the same",
			event: types.Event{Start: t2, End: t2},
			want:  "2024",
		},
		{
			name:  "returns the Start year followed by hyphen and spaces when EndTime is zero",
			event: types.Event{Start: t1},
			want:  "2020-    ",
		},
		{
			name:  "returns the End year when only EndTime is set",
			event: types.Event{End: t2},
			want:  "2024",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.event.FormatDates())
		})
	}
}

func TestEvent_FormatStartDate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		event *types.Event
		want  string
	}{
		{
			name:  "returns empty string when StartTime is zero",
			event: &types.Event{},
			want:  "",
		},
		{
			name:  "returns formatted date when StartTime is set",
			event: &types.Event{Start: t1},
			want:  "2020-03-15",
		},
		{
			name:  "ignores EndTime",
			event: &types.Event{End: t2},
			want:  "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.event.FormatStart())
		})
	}
}

func TestEvent_FormatEndDate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		event *types.Event
		want  string
	}{
		{
			name:  "returns empty string when EndTime is zero",
			event: &types.Event{},
			want:  "",
		},
		{
			name:  "returns formatted date when EndTime is set",
			event: &types.Event{End: t2},
			want:  "2024-11-01",
		},
		{
			name:  "ignores StartTime",
			event: &types.Event{Start: t1},
			want:  "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.event.FormatEnd())
		})
	}
}
