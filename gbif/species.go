// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

package gbif

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
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
	ScientificName           string // full name (with authorship)
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
	SpeciesKey int64

	Kingdom string
	Phylum  string
	Class   string
	Order   string
	Family  string
	Genus   string
	Species string
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

// TaxonName returns a list of taxons with a given name.
func TaxonName(name string) ([]*Species, error) {
	name = strings.Join(strings.Fields(name), " ")
	if name == "" {
		return nil, errors.New("gbif: taxonomy: search an empty taxon")
	}
	request := "species?"
	param := url.Values{}
	param.Add("name", name)
	ls, err := taxonList(request, param)
	if err != nil {
		return nil, fmt.Errorf("taxonomy: gbif: taxon: %v", err)
	}
	return ls, nil
}

// Children returns an list with the children of a given taxon ID.
func Children(id int64) ([]*Species, error) {
	request := "species/" + strconv.FormatInt(id, 10) + "/children?"
	param := url.Values{}
	param.Add("offset", "0")
	ls, err := taxonList(request, param)
	if err != nil {
		return nil, fmt.Errorf("taxonomy: gbif: taxon: %v", err)
	}
	return ls, nil
}

// Synonym returns a slice of synonyms of a taxon ID.
func Synonym(id int64) ([]*Species, error) {
	request := "species/" + strconv.FormatInt(id, 10) + "/synonyms?"
	param := url.Values{}
	param.Add("offset", "0")
	ls, err := taxonList(request, param)
	if err != nil {
		return nil, fmt.Errorf("taxonomy: gbif: taxon: %v", err)
	}
	return ls, nil
}

func taxonList(request string, param url.Values) ([]*Species, error) {
	var ls []*Species
	var err error
	end := false
	for off := int64(0); !end; {
		if off > 0 {
			param.Set("offset", strconv.FormatInt(off, 10))
		}
		retryErr := true
		for r := 0; r < Retry; r++ {
			req := newRequest(request + param.Encode())
			select {
			case err = <-req.err:
				continue
			case a := <-req.ans:
				d := json.NewDecoder(a.Body)
				resp := &spAnswer{}
				err = d.Decode(resp)
				a.Body.Close()
				if err != nil {
					continue
				}
				for _, sp := range resp.Results {
					if sp.Key != sp.NubKey {
						continue
					}
					ls = append(ls, sp)
				}
				if resp.EndOfRecords {
					// end retry loop
					end = true
					r = Retry
					retryErr = false
					continue
				}
				off += resp.Limit
				r = Retry
				retryErr = false
			}
		}
		if retryErr {
			if err == nil {
				return nil, fmt.Errorf("no answer after %d retries", Retry)
			}
			return nil, err
		}
	}
	return ls, nil
}
