// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

package gbif

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// SpAnswer is the answer for the species request.
type spAnswer struct {
	Offset, Limit int64
	EndOfRecords  bool
	Results       []*Species
}

// Species stores the taxonomic information stored in GBIF.
type Species struct {
	Key, NubKey, AcceptedKey int64  // ID
	CanonicalName            string // name
	BasionymKey              int64  // ID of the basionym
	Authorship               string // author
	Rank                     string // taxon rank
	TaxonomicStatus          string // status
	DatasetKey               string // source
	ParentKey                int64  // parent
	PublishedIn              string // reference

	//parent keys
	KingdomKey int64
	PhylumKey  int64
	ClassKey   int64
	OrderKey   int64
	FamilyKey  int64
	GenusKey   int64

	Kingdom string
	Phylum  string
	Class   string
	Order   string
	Family  string
	Genus   string
}

// SpeciesID return a Species from a GBIF species ID.
func SpeciesID(id string) (*Species, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, errors.New("gbif: species: search an empty ID")
	}

	var err error
	for r := 0; r < Retry; r++ {
		req := newRequest("species/" + id)
		select {
		case err = <-req.err:
			continue
		case a := <-req.ans:
			d := json.NewDecoder(a.Body)
			sp := &Species{}
			err = d.Decode(sp)
			a.Body.Close()
			if err != nil {
				continue
			}
			return sp, nil
		}
	}
	if err == nil {
		return nil, fmt.Errorf("gbif: species: no answer after %d retries", Retry)
	}
	return nil, fmt.Errorf("gbif: species: %v", err)
}
