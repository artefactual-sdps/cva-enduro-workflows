package types_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"gotest.tools/v3/assert"

	"github.com/artefactual-sdps/cva-enduro-workflows/internal/enums"
	"github.com/artefactual-sdps/cva-enduro-workflows/internal/types"
)

func TestAcquisition(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		md   *types.ContainerMD
		want string
	}{
		{
			name: "returns empty string when Consignment is empty",
			md: &types.ContainerMD{
				Container: types.ContainerMDRecord{
					Consignment: "",
				},
			},
			want: "",
		},
		{
			name: "returns formatted string when Consignment is set",
			md: &types.ContainerMD{
				Container: types.ContainerMDRecord{
					Consignment: "900036",
				},
			},
			want: "VanDocs transfer: 900036",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.md.Acquisition())
		})
	}
}

func TestAlternativeIdentifiers(t *testing.T) {
	t.Parallel()

	aipID := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")

	for _, tc := range []struct {
		name                string
		md                  types.ContainerMD
		aipID               uuid.UUID
		wantIDs, wantLabels []string
	}{
		{
			name:       "returns AIP ID and label when ContainerMD is empty",
			aipID:      aipID,
			wantIDs:    []string{"123e4567-e89b-12d3-a456-426614174000"},
			wantLabels: []string{"AIP UUID"},
		},
		{
			name: "returns an empty UUID and RecordNumber when AIP ID is nil",
			md: types.ContainerMD{
				Container: types.ContainerMDRecord{
					RecordNumber: "01-1000-30/0000007",
				},
			},
			aipID: uuid.Nil,
			wantIDs: []string{
				"00000000-0000-0000-0000-000000000000",
				"01-1000-30/0000007",
			},
			wantLabels: []string{
				"AIP UUID",
				"VanDocs container record number",
			},
		},
		{
			name: "returns both AIP ID and RecordNumber when both are set",
			md: types.ContainerMD{
				Container: types.ContainerMDRecord{
					RecordNumber: "01-1000-30/0000007",
				},
			},
			aipID: aipID,
			wantIDs: []string{
				"123e4567-e89b-12d3-a456-426614174000",
				"01-1000-30/0000007",
			},
			wantLabels: []string{
				"AIP UUID",
				"VanDocs container record number",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotIDs, gotLabels := tc.md.AlternativeIdentifiers(tc.aipID)
			assert.DeepEqual(t, tc.wantIDs, gotIDs)
			assert.DeepEqual(t, tc.wantLabels, gotLabels)
		})
	}
}

func TestCreationDate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		md   types.ContainerMD
		want types.Event
	}{
		{
			name: "returns creation event with no times when ContainerMD is empty",
			md:   types.ContainerMD{},
			want: types.Event{
				Type: enums.EventTypeCreation,
			},
		},
		{
			name: "returns a creation event",
			md: types.ContainerMD{
				Container: types.ContainerMDRecord{
					DateRegistered: time.Date(2020, 3, 15, 0, 0, 0, 0, time.UTC),
					DateClosed:     time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC),
				},
			},
			want: types.Event{
				Type:  enums.EventTypeCreation,
				Start: time.Date(2020, 3, 15, 0, 0, 0, 0, time.UTC),
				End:   time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.DeepEqual(t, tc.want, tc.md.CreationEvent())
		})
	}
}

func TestIdentifier(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		md   types.ContainerMD
		want string
	}{
		{
			name: "returns empty string when RecordNumber is empty",
			md: types.ContainerMD{
				Container: types.ContainerMDRecord{
					RecordNumber: "",
				},
			},
			want: "",
		},
		{
			name: "returns empty string when RecordNumber does not contain a forward slash",
			md: types.ContainerMD{
				Container: types.ContainerMDRecord{
					RecordNumber: "01-1000-30-0000007",
				},
			},
			want: "",
		},
		{
			name: "returns identifier with F prefix when RecordNumber is valid",
			md: types.ContainerMD{
				Container: types.ContainerMDRecord{
					RecordNumber: "01-1000-30/0000007",
				},
			},
			want: "F0000007",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.md.Identifier())
		})
	}
}

func TestQubitParentSlug(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		md   types.ContainerMD
		want string
	}{
		{
			name: "returns empty string when classification is empty",
			md: types.ContainerMD{
				Container: types.ContainerMDRecord{
					Classification: "",
					OPR:            "VPL - Vancouver Public Library",
				},
			},
			want: "",
		},
		{
			name: "returns classification when OPR has no matching prefix",
			md: types.ContainerMD{
				Container: types.ContainerMDRecord{
					Classification: "01-5000-12",
					OPR:            "COV - Office of Custody (OPR)",
				},
			},
			want: "01-5000-12",
		},
		{
			name: "prepends PD when OPR starts with PD",
			md: types.ContainerMD{
				Container: types.ContainerMDRecord{
					Classification: "01-5000-12",
					OPR:            "PD - Planning Department",
				},
			},
			want: "PD-01-5000-12",
		},
		{
			name: "prepends VPD when OPR starts with VPD",
			md: types.ContainerMD{
				Container: types.ContainerMDRecord{
					Classification: "01-5000-12",
					OPR:            "VPD - Vancouver Police Department",
				},
			},
			want: "VPD-01-5000-12",
		},
		{
			name: "prepends VPL when OPR starts with VPL",
			md: types.ContainerMD{
				Container: types.ContainerMDRecord{
					Classification: "01-5000-12",
					OPR:            "VPL - Vancouver Public Library",
				},
			},
			want: "VPL-01-5000-12",
		},
		{
			name: "returns classification when OPR is empty",
			md: types.ContainerMD{
				Container: types.ContainerMDRecord{
					Classification: "01-5000-12",
					OPR:            "",
				},
			},
			want: "01-5000-12",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.md.QubitParentSlug())
		})
	}
}

func TestTitle(t *testing.T) {
	t.Parallel()

	t.Run("Returns title", func(t *testing.T) {
		t.Parallel()

		md := types.ContainerMD{
			Container: types.ContainerMDRecord{
				TitleFreeTextPart: "Test Title",
			},
		}
		assert.Equal(t, "Test Title", md.Title())
	})
}
