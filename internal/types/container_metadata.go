package types

import (
	"encoding/xml"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/artefactual-sdps/cva-enduro-workflows/internal/enums"
)

// ContainerMD represents the parsed contents of a ContainerMetadata.xml file
// produced by the VanDocs records management system. The XML structure wraps
// all fields in a single <Container> child of the root <ContainerMetadata>
// element.
type ContainerMD struct {
	XMLName   xml.Name          `xml:"ContainerMetadata"`
	Container ContainerMDRecord `xml:"Container"`
}

// ContainerMDRecord holds the individual metadata fields for a VanDocs
// container.
type ContainerMDRecord struct {
	AccessControl               string    `xml:"AccessControl"`
	AccessionNumber             int       `xml:"AccessionNumber"`
	AllVersions                 string    `xml:"AllVersions"`
	Assignee                    string    `xml:"Assignee"`
	AssigneeDate                time.Time `xml:"AssigneeDate"`
	Classification              string    `xml:"Classification"`
	Consignment                 string    `xml:"Consignment"`
	ContainedRecords            string    `xml:"ContainedRecords"`
	Creator                     string    `xml:"Creator"`
	DateClosed                  time.Time `xml:"DateClosed"`
	DateCreated                 time.Time `xml:"DateCreated"`
	DateDeclaredAsFinal         time.Time `xml:"DateDeclaredAsFinal"`
	DateDueforDestruction       time.Time `xml:"DateDueforDestruction"`
	DateDueforInactive          time.Time `xml:"DateDueforInactive"`
	DateDueforPermanentArchival time.Time `xml:"DateDueforPermanentArchival"`
	DateInactive                time.Time `xml:"DateInactive"`
	DateLastUpdated             time.Time `xml:"DateLastUpdated"`
	DateRegistered              time.Time `xml:"DateRegistered"`
	DatePublished               time.Time `xml:"DatePublished"`
	Department                  string    `xml:"Department"`
	Disposition                 string    `xml:"Disposition"`
	ExpandedNumber              string    `xml:"ExpandedNumber"`
	ExternalID                  string    `xml:"ExternalID"`
	FullClassificationNumber    string    `xml:"FullClassificationNumber"`
	HomeLocation                string    `xml:"HomeLocation"`
	IsContainer                 bool      `xml:"IsContainer"`
	IsElectronic                bool      `xml:"IsElectronic"`
	HasHolds                    bool      `xml:"HasHolds"`
	Notes                       string    `xml:"Notes"`
	OPR                         string    `xml:"OPR"`
	Owner                       string    `xml:"Owner"`
	OwnerLocationType           string    `xml:"OwnerLocationType"`
	PaperFolderExists           bool      `xml:"PaperFolderExists"`
	PersonalInformationBank     bool      `xml:"PersonalInformationBank"`
	RecordClass                 string    `xml:"RecordClass"`
	RecordNumber                string    `xml:"RecordNumber"`
	RecordType                  string    `xml:"RecordType"`
	RelatedRecords              string    `xml:"RelatedRecords"`
	RetentionSchedule           string    `xml:"RetentionSchedule"`
	Security                    string    `xml:"Security"`
	Title                       string    `xml:"Title"`
	TitleFreeTextPart           string    `xml:"TitleFreeTextPart"`
	TitleStructuredPart         string    `xml:"TitleStructuredPart"`
	UniqueIdentifier            int64     `xml:"UniqueIdentifier"`
}

// Acquisition maps the Consignment field to the acquisition column of the Batch
// CSV. If Consignment is empty, an empty string is returned.
func (md ContainerMD) Acquisition() string {
	if md.Container.Consignment == "" {
		return ""
	}

	return fmt.Sprintf("VanDocs transfer: %s", md.Container.Consignment)
}

// AlternativeIdentifiers maps the RecordNumber field and the AIP ID to the
// alternativeIdentifiers and alternativeIdentifierLabels columns of the Batch
// CSV.
func (md ContainerMD) AlternativeIdentifiers(aipID uuid.UUID) (ids, labels []string) {
	ids = append(ids, aipID.String())
	labels = append(labels, "AIP UUID")

	if md.Container.RecordNumber != "" {
		ids = append(ids, md.Container.RecordNumber)
		labels = append(labels, "VanDocs container record number")
	}

	return ids, labels
}

// CreationEvent returns a creation `Event` based on the DateRegistered
// and DateClosed metadata fields. If both dates are zero, an empty Event is
// returned.
func (md ContainerMD) CreationEvent() Event {
	return Event{
		Type:  enums.EventTypeCreation,
		Start: md.Container.DateRegistered,
		End:   md.Container.DateClosed,
	}
}

// Identifier maps the RecordNumber field to the identifier CSVcolumn.
//
// The identifier is constructed from the part of RecordNumber after the forward
// slash, prepended with an "F". E.g. RecordNumber "01-1000-30/0000007" yields
// the identifier "F0000007".
//
// If RecordNumber is empty or does not contain a forward slash, an empty string
// is returned.
func (md ContainerMD) Identifier() string {
	if md.Container.RecordNumber == "" {
		return ""
	}

	// RecordNumber is in the format "01-1000-30/0000007" and we want the part
	// after the forward slash.
	parts := strings.Split(md.Container.RecordNumber, "/")
	if len(parts) < 2 {
		return ""
	}

	// Prepend "F" to the identifier.
	return fmt.Sprintf("F%s", parts[1])
}

// RecordkeepingEvent returns an recordkeeping Event based on the HomeLocation
// field. If HomeLocation is empty, an empty Event is returned.
func (md ContainerMD) RecordkeepingEvent() Event {
	if md.Container.HomeLocation == "" {
		return Event{}
	}

	return Event{
		Type:  enums.EventTypeRecordkeeping,
		Actor: md.Container.HomeLocation,
	}
}

// Title maps the TitleFreeTextPart field to the title column.
func (md ContainerMD) Title() string {
	return md.Container.TitleFreeTextPart
}

// QubitParentSlug maps the Classification field to the qubitParentSlug column.
//
// If Classification is empty, an empty string is returned.
// If the OPR field begins with "PD", "VPD", or "VPL", the corresponding code is
// prepended to the Classification value to differentiate these external SIPs
// from SIPs created internally by CVA.
func (md ContainerMD) QubitParentSlug() string {
	if md.Container.Classification == "" {
		return ""
	}

	qubitParentSlug := md.Container.Classification
	for _, code := range []string{"PD", "VPD", "VPL"} {
		if strings.HasPrefix(md.Container.OPR, code) {
			qubitParentSlug = fmt.Sprintf("%s-%s", code, qubitParentSlug)
			break
		}
	}

	return qubitParentSlug
}
