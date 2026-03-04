package activities

import (
	"context"
	"encoding/csv"
	"encoding/xml"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"gocloud.dev/blob"

	"github.com/artefactual-sdps/cva-enduro-workflows/internal/types"
)

const (
	CreateCSVName string = "create-csv-activity"

	accessConditions string = "This file has not been reviewed for potential FOIPPA restrictions. Access is pending review and may be delayed. See archivist for details."
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
	if params.Batch == nil {
		return nil, fmt.Errorf("create CSV: missing batch")
	}

	key := fmt.Sprintf("reports/batch_%s.csv", params.Batch.UUID)

	bw, err := a.bucket.NewWriter(ctx, key, nil)
	if err != nil {
		return nil, fmt.Errorf("create CSV: new writer: %w", err)
	}
	defer bw.Close()

	cw := csv.NewWriter(bw)

	// Write header.
	err = cw.Write([]string{
		"qubitParentSlug",
		"acquisition",
		"eventTypes",
		"eventDates",
		"eventStartDates",
		"eventEndDates",
		"eventActors",
		"identifier",
		"alternativeIdentifiers",
		"alternativeIdentifierLabels",
		"title",
		"radGeneralMaterialDesignation",
		"levelOfDescription",
		"culture",
		"publicationStatus",
		"accessConditions",
	})
	if err != nil {
		return nil, fmt.Errorf("create CSV: write header: %w", err)
	}

	// TODO: Add an "extentAndMedium" column with the total number of files
	// in each SIP.

	for i, sip := range params.SIPs {
		if sip.Name == "" {
			return nil, fmt.Errorf("create CSV: SIP %d: missing name", i+1)
		}
		if sip.AIPID == nil || *sip.AIPID == uuid.Nil {
			continue
		}

		md, err := a.parseContainerMetadata(ctx, sip.UUID.String())
		if err != nil {
			return nil, fmt.Errorf("create CSV: parse container metadata: %w", err)
		}

		// Alternative identifiers and their labels are split over two columns,
		// so extract them as separate strings here.
		altIDs, altLabels := md.AlternativeIdentifiers(*sip.AIPID)

		// Add Creation and Recordkeeping events.
		events := make([]types.Event, 0, 2)
		if e := md.CreationEvent(); !e.IsZero() {
			events = append(events, e)
		}
		if e := md.RecordkeepingEvent(); !e.IsZero() {
			events = append(events, e)
		}

		err = cw.Write([]string{
			md.QubitParentSlug(),                          // qubitParentSlug
			md.Acquisition(),                              // acquisition
			joinWithPipe(events, types.Event.GetType),     // eventTypes
			joinWithPipe(events, types.Event.FormatDates), // eventDates
			joinWithPipe(events, types.Event.FormatStart), // eventStartDates
			joinWithPipe(events, types.Event.FormatEnd),   // eventEndDates
			joinWithPipe(events, types.Event.GetActor),    // eventActors
			md.Identifier(),                               // identifier
			strings.Join(altIDs, "|"),                     // alternativeIdentifiers
			strings.Join(altLabels, "|"),                  // alternativeIdentifierLabels
			md.Title(),                                    // title
			"Multiple media",                              // radGeneralMaterialDesignation
			"File",                                        // levelOfDescription
			"en",                                          // culture
			"draft",                                       // publicationStatus
			accessConditions,                              // accessConditions
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

// parseContainerMetadata reads and parses the ContainerMetadata.xml file for
// the given SIP.
func (a *CreateCSV) parseContainerMetadata(ctx context.Context, sipUUID string) (*types.ContainerMD, error) {
	key := fmt.Sprintf("%s_ContainerMetadata.xml", sipUUID)

	r, err := a.bucket.NewReader(ctx, key, nil)
	if err != nil {
		return nil, fmt.Errorf("parse container metadata: new reader: %w", err)
	}
	defer r.Close()

	// decode the "ContainerMetadata.xml" XML data.
	var md types.ContainerMD
	if err := xml.NewDecoder(r).Decode(&md); err != nil {
		return nil, fmt.Errorf("parse container metadata: decode XML: %w", err)
	}
	return &md, nil
}

// joinWithPipe applies the provided function to each event and joins the
// returned strings with a pipe separator. If the function returns an empty
// string, "NULL" is used instead.
func joinWithPipe(events []types.Event, f func(types.Event) string) string {
	vals := make([]string, len(events))
	for i, e := range events {
		v := f(e)
		if v == "" {
			v = "NULL"
		}
		vals[i] = v
	}
	return strings.Join(vals, "|")
}
