package activities

import (
	"context"
	"encoding/csv"
	"fmt"

	"gocloud.dev/blob"

	"github.com/artefactual-sdps/cva-enduro-workflows/internal/types"
)

const (
	CreateCSVName string = "create-csv-activity"

	accessRestriction string = "This file has not been reviewed for potential FOIPPA restrictions. Access is pending review and may be delayed. See archivist for details."
)

// CreateCSV is an activity that creates an AtoM CSV file for the given SIPs.
type (
	CreateCSV struct {
		bucket *blob.Bucket
	}
	CreateCSVParams struct {
		Batch *types.Batch
		SIPs  []*types.SIP
	}
	CreateCSVResult struct {
		Key string
	}
)

// NewCreateCSV creates a new CreateCSV.
func NewCreateCSV(b *blob.Bucket) *CreateCSV {
	return &CreateCSV{
		bucket: b,
	}
}

func (a *CreateCSV) Execute(ctx context.Context, params *CreateCSVParams) (*CreateCSVResult, error) {
	if len(params.SIPs) == 0 {
		return nil, fmt.Errorf("create CSV: no SIPs provided")
	}

	key := fmt.Sprintf("batch_%s.csv", params.Batch.UUID)

	bw, err := a.bucket.NewWriter(ctx, key, nil)
	if err != nil {
		return nil, fmt.Errorf("create CSV: new writer: %w", err)
	}
	defer bw.Close()

	cw := csv.NewWriter(bw)

	// Write header.
	err = cw.Write([]string{
		"title",
		"alternativeIdentifiers",
		"alternativeIdentifierLabels",
		"radGeneralMaterialDesignation",
		"levelOfDescription",
		"culture",
		"publicationStatus",
		"accessRestriction",
	})
	if err != nil {
		return nil, fmt.Errorf("create CSV: write header: %w", err)
	}

	for i, sip := range params.SIPs {
		if sip.Name == "" {
			return nil, fmt.Errorf("create CSV: SIP %d: missing name", i+1)
		}
		if sip.AIPID == nil {
			continue
		}

		err := cw.Write([]string{
			sip.Name,           // title
			sip.AIPID.String(), // alternativeIdentifiers
			"AIP UUID",         // alternativeIdentifierLabels
			"Multiple media",   // radGeneralMaterialDesignation
			"File",             // levelOfDescription
			"en",               // culture
			"draft",            // publicationStatus
			accessRestriction,  // accessRestriction
		})
		if err != nil {
			return nil, fmt.Errorf("create CSV: write row %d: %w", i+1, err)
		}
	}

	cw.Flush()
	if err := cw.Error(); err != nil {
		return nil, fmt.Errorf("create CSV: flush writer: %w", err)
	}

	return &CreateCSVResult{Key: key}, nil
}
