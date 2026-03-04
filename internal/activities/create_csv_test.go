package activities_test

import (
	"context"
	"io"
	"testing"

	"github.com/google/uuid"
	"go.artefactual.dev/tools/bucket"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	"gotest.tools/v3/assert"

	"github.com/artefactual-sdps/cva-enduro-workflows/internal/activities"
	"github.com/artefactual-sdps/cva-enduro-workflows/internal/types"
)

// containerMDXMLParams holds the fields used by sipContainerMetadataXML.
type containerMDXMLParams struct {
	consignment       string
	recordNumber      string
	titleFreeTextPart string
	homeLocation      string
	dateRegistered    string
	dateClosed        string
}

// sipContainerMetadataXML returns a ContainerMetadata.xml for the given params.
// Date fields (dateRegistered, dateClosed) are omitted when empty so that the
// XML decoder does not fail trying to parse an empty string as time.Time.
func sipContainerMetadataXML(p containerMDXMLParams) string {
	var dateRegistered, dateClosed string
	if p.dateRegistered != "" {
		dateRegistered = "    <DateRegistered>" + p.dateRegistered + "</DateRegistered>\n"
	}
	if p.dateClosed != "" {
		dateClosed = "    <DateClosed>" + p.dateClosed + "</DateClosed>\n"
	}
	return `<ContainerMetadata xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <Container>
    <Classification>01-5000-12</Classification>
    <Consignment>` + p.consignment + `</Consignment>
    <HomeLocation>` + p.homeLocation + `</HomeLocation>
    <OPR>COV - Office of Custody (OPR)</OPR>
    <RecordNumber>` + p.recordNumber + `</RecordNumber>
    <TitleFreeTextPart>` + p.titleFreeTextPart + `</TitleFreeTextPart>
` + dateRegistered + dateClosed + `  </Container>
</ContainerMetadata>`
}

// seedContainerMetadataXML uploads XML content for a single SIP UUID into b.
func seedContainerMetadataXML(t *testing.T, b *blob.Bucket, sipUUID uuid.UUID, xmlContent string) {
	t.Helper()
	key := sipUUID.String() + "_ContainerMetadata.xml"
	err := b.WriteAll(context.Background(), key, []byte(xmlContent), nil)
	assert.NilError(t, err, "seed ContainerMetadata.xml for SIP %s", sipUUID)
}

const (
	csvHeader = "qubitParentSlug," +
		"acquisition," +
		"eventTypes," +
		"eventDates," +
		"eventStartDates," +
		"eventEndDates," +
		"eventActors," +
		"identifier," +
		"alternativeIdentifiers," +
		"alternativeIdentifierLabels," +
		"title," +
		"radGeneralMaterialDesignation," +
		"levelOfDescription," +
		"culture," +
		"publicationStatus," +
		"accessConditions"

	accessConditionsValue = "This file has not been reviewed for potential" +
		" FOIPPA restrictions. Access is pending review and may be delayed." +
		" See archivist for details."
)

func TestCreateCSV_Execute(t *testing.T) {
	t.Parallel()

	type test struct {
		name        string
		bucketCfg   *bucket.Config
		params      *activities.CreateCSVParams
		setup       func(t *testing.T, b *blob.Bucket)
		expectedKey string
		want        string
		wantErr     string
	}

	batchID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	sipID1 := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	sipID2 := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	aipID1 := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	aipID2 := uuid.MustParse("22222222-3333-4444-5555-666666666666")

	for _, tc := range []test{
		{
			name:      "writes CSV with two SIPs",
			bucketCfg: &bucket.Config{URL: "file:///" + t.TempDir()},
			params: &activities.CreateCSVParams{
				Batch: &types.Batch{UUID: batchID},
				SIPs: []*types.SIP{
					{UUID: sipID1, Name: "Test SIP 1", AIPID: &aipID1},
					{UUID: sipID2, Name: "Test SIP 2", AIPID: &aipID2},
				},
			},
			setup: func(t *testing.T, b *blob.Bucket) {
				t.Helper()
				seedContainerMetadataXML(t, b, sipID1, sipContainerMetadataXML(containerMDXMLParams{
					consignment:       "900036",
					recordNumber:      "01-5000-12/2009-01",
					titleFreeTextPart: "Test Title 1",
					homeLocation:      "Finance and Supply Chain Management (FSC)",
					dateRegistered:    "2009-01-15T00:00:00Z",
					dateClosed:        "2012-06-30T00:00:00Z",
				}))
				seedContainerMetadataXML(t, b, sipID2, sipContainerMetadataXML(containerMDXMLParams{
					consignment:       "900037",
					recordNumber:      "01-5000-12/2010-02",
					titleFreeTextPart: "Test Title 2",
					homeLocation:      "Engineering Services (ENG)",
					dateRegistered:    "2010-02-01T00:00:00Z",
					dateClosed:        "2015-03-31T00:00:00Z",
				}))
			},
			expectedKey: "reports/batch_33333333-3333-3333-3333-333333333333.csv",
			want: csvHeader +
				"\n" +
				"01-5000-12," +
				"VanDocs transfer: 900036," +
				"Creation|Recordkeeping," +
				"2009-2012|NULL," +
				"2009-01-15|NULL," +
				"2012-06-30|NULL," +
				"NULL|Finance and Supply Chain Management (FSC)," +
				"F2009-01," +
				"11111111-2222-3333-4444-555555555555|01-5000-12/2009-01," +
				"AIP UUID|VanDocs container record number," +
				"Test Title 1," +
				"Multiple media," +
				"File," +
				"en," +
				"draft," +
				accessConditionsValue +
				"\n" +
				"01-5000-12,VanDocs transfer: 900037," +
				"Creation|Recordkeeping," +
				"2010-2015|NULL," +
				"2010-02-01|NULL," +
				"2015-03-31|NULL," +
				"NULL|Engineering Services (ENG)," +
				"F2010-02," +
				"22222222-3333-4444-5555-666666666666|01-5000-12/2010-02," +
				"AIP UUID|VanDocs container record number," +
				"Test Title 2," +
				"Multiple media," +
				"File," +
				"en," +
				"draft," +
				accessConditionsValue +
				"\n",
		},
		{
			name:      "writes CSV with only recordkeeping event when creation dates are zero",
			bucketCfg: &bucket.Config{URL: "file:///" + t.TempDir()},
			params: &activities.CreateCSVParams{
				Batch: &types.Batch{UUID: batchID},
				SIPs: []*types.SIP{
					{UUID: sipID1, Name: "Test SIP 1", AIPID: &aipID1},
				},
			},
			setup: func(t *testing.T, b *blob.Bucket) {
				t.Helper()
				seedContainerMetadataXML(t, b, sipID1, sipContainerMetadataXML(containerMDXMLParams{
					consignment:       "900036",
					recordNumber:      "01-5000-12/2009-01",
					titleFreeTextPart: "Test Title 1",
					homeLocation:      "Finance and Supply Chain Management (FSC)",
				}))
			},
			expectedKey: "reports/batch_33333333-3333-3333-3333-333333333333.csv",
			want: csvHeader +
				"\n" +
				"01-5000-12," +
				"VanDocs transfer: 900036," +
				"Recordkeeping," +
				"NULL," +
				"NULL," +
				"NULL," +
				"Finance and Supply Chain Management (FSC)," +
				"F2009-01," +
				"11111111-2222-3333-4444-555555555555|01-5000-12/2009-01," +
				"AIP UUID|VanDocs container record number," +
				"Test Title 1," +
				"Multiple media," +
				"File," +
				"en," +
				"draft," +
				accessConditionsValue +
				"\n",
		},
		{
			name:      "writes CSV with empty event columns when both events are zero",
			bucketCfg: &bucket.Config{URL: "file:///" + t.TempDir()},
			params: &activities.CreateCSVParams{
				Batch: &types.Batch{UUID: batchID},
				SIPs: []*types.SIP{
					{UUID: sipID1, Name: "Test SIP 1", AIPID: &aipID1},
				},
			},
			setup: func(t *testing.T, b *blob.Bucket) {
				t.Helper()
				seedContainerMetadataXML(t, b, sipID1, sipContainerMetadataXML(containerMDXMLParams{
					consignment:       "900036",
					recordNumber:      "01-5000-12/2009-01",
					titleFreeTextPart: "Test Title 1",
				}))
			},
			expectedKey: "reports/batch_33333333-3333-3333-3333-333333333333.csv",
			want: csvHeader +
				"\n" +
				"01-5000-12," +
				"VanDocs transfer: 900036," +
				"," +
				"," +
				"," +
				"," +
				"," +
				"F2009-01," +
				"11111111-2222-3333-4444-555555555555|01-5000-12/2009-01," +
				"AIP UUID|VanDocs container record number," +
				"Test Title 1," +
				"Multiple media," +
				"File," +
				"en," +
				"draft," +
				accessConditionsValue +
				"\n",
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
					{AIPID: &aipID1},
				},
			},
			wantErr: "create CSV: SIP 1: missing name",
		},
		{
			name:      "skips SIP if AIP ID is missing",
			bucketCfg: &bucket.Config{URL: "file:///" + t.TempDir()},
			params: &activities.CreateCSVParams{
				Batch: &types.Batch{UUID: batchID},
				SIPs: []*types.SIP{
					{Name: "Test SIP 1"},
				},
			},
			expectedKey: "reports/batch_33333333-3333-3333-3333-333333333333.csv",
			want:        csvHeader + "\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b, err := bucket.NewWithConfig(t.Context(), tc.bucketCfg)
			assert.NilError(t, err, "failed to open bucket")
			defer b.Close()

			if tc.setup != nil {
				tc.setup(t, b)
			}

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
