package activities_test

import (
	"io"
	"testing"

	"github.com/google/uuid"
	"go.artefactual.dev/tools/bucket"
	_ "gocloud.dev/blob/fileblob"
	"gotest.tools/v3/assert"

	"github.com/artefactual-sdps/cva-enduro-workflows/internal/activities"
	"github.com/artefactual-sdps/cva-enduro-workflows/internal/types"
)

func TestCreateCSV_Execute(t *testing.T) {
	t.Parallel()

	type test struct {
		name        string
		bucketCfg   *bucket.Config
		params      *activities.CreateCSVParams
		expectedKey string
		want        string
		wantErr     string
	}

	batchID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	aipID1 := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	aipID2 := uuid.MustParse("22222222-3333-4444-5555-666666666666")

	for _, tc := range []test{
		{
			name:      "writes CSV with two SIPs",
			bucketCfg: &bucket.Config{URL: "file:///" + t.TempDir()},
			params: &activities.CreateCSVParams{
				Batch: &types.Batch{UUID: batchID},
				SIPs: []*types.SIP{
					{
						Name:  "Test SIP 1",
						AIPID: &aipID1,
					},
					{
						Name:  "Test SIP 2",
						AIPID: &aipID2,
					},
				},
			},
			expectedKey: "reports/batch_33333333-3333-3333-3333-333333333333.csv",
			want: `title,alternativeIdentifiers,alternativeIdentifierLabels,radGeneralMaterialDesignation,levelOfDescription,culture,publicationStatus,accessRestriction
Test SIP 1,11111111-2222-3333-4444-555555555555,AIP UUID,Multiple media,File,en,draft,This file has not been reviewed for potential FOIPPA restrictions. Access is pending review and may be delayed. See archivist for details.
Test SIP 2,22222222-3333-4444-5555-666666666666,AIP UUID,Multiple media,File,en,draft,This file has not been reviewed for potential FOIPPA restrictions. Access is pending review and may be delayed. See archivist for details.
`,
		},
		{
			name:      "no SIPs provided",
			bucketCfg: &bucket.Config{URL: "file:///" + t.TempDir()},
			params: &activities.CreateCSVParams{
				Batch: &types.Batch{UUID: batchID},
			},
			wantErr: "create CSV: no SIPs provided",
		},
		{
			name:      "errors if SIP name is missing",
			bucketCfg: &bucket.Config{URL: "file:///" + t.TempDir()},
			params: &activities.CreateCSVParams{
				Batch: &types.Batch{UUID: batchID},
				SIPs: []*types.SIP{
					{
						AIPID: &aipID1,
					},
				},
			},
			wantErr: "create CSV: SIP 1: missing name",
		},
		{
			name:      "skips SIP if AIP ID is missing",
			bucketCfg: &bucket.Config{URL: "file:///" + t.TempDir()},
			params: &activities.CreateCSVParams{
				Batch: &types.Batch{
					UUID: uuid.MustParse("33333333-3333-3333-3333-333333333333"),
				},
				SIPs: []*types.SIP{
					{
						Name: "Test SIP 1",
					},
				},
			},
			expectedKey: "reports/batch_33333333-3333-3333-3333-333333333333.csv",
			want: `title,alternativeIdentifiers,alternativeIdentifierLabels,radGeneralMaterialDesignation,levelOfDescription,culture,publicationStatus,accessRestriction
`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b, err := bucket.NewWithConfig(t.Context(), tc.bucketCfg)
			assert.NilError(t, err, "failed to open bucket")
			defer b.Close()

			act := activities.NewCreateCSV(b)
			res, err := act.Execute(t.Context(), tc.params)

			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
				return
			}

			assert.NilError(t, err)
			assert.Equal(t, tc.expectedKey, res.Key)

			r, err := b.NewReader(t.Context(), res.Key, nil)
			assert.NilError(t, err)
			defer r.Close()

			got, err := io.ReadAll(r)
			assert.NilError(t, err)
			assert.Equal(t, tc.want, string(got))
		})
	}
}
