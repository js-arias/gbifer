// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

// Package taxonomy implements a basic taxonomy
// to be used with GBIF occurrence data.
package taxonomy

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/js-arias/gbifer/gbif"
	"github.com/js-arias/gbifer/tsv"
)

// Rank is a linnean rank.
// Ranks are arranged in a way that an inclusive rank in the taxonomy
// is always smaller than more exclusive ranks.
// Then it is possible to use the form:
//
//	if rank < taxonomy.Genus {
//		// do something
//	}
type Rank uint

// Valid taxonomic ranks.
const (
	Unranked Rank = iota
	Kingdom
	Phylum
	Class
	Order
	Family
	Genus
	Species
)

// ranks holds a list of the accepted rank names.
var ranks = []string{
	Unranked: "unranked",
	Kingdom:  "kingdom",
	Phylum:   "phylum",
	Class:    "class",
	Order:    "order",
	Family:   "family",
	Genus:    "genus",
	Species:  "species",
}

// GetRank returns a rank value from a string.
func GetRank(s string) Rank {
	s = strings.ToLower(s)
	for i, r := range ranks {
		if r == s {
			return Rank(i)
		}
	}
	return Unranked
}

// String returns the rank string of a rank.
func (r Rank) String() string {
	i := int(r)
	if i >= len(ranks) {
		return ranks[Unranked]
	}
	return ranks[i]
}

// A Taxon stores the taxon information.
type Taxon struct {
	Name   string // taxon name
	Author string // author of the name
	ID     int64  // ID of the taxon
	Rank   Rank   // taxon rank
	Status string // taxon status
	Parent int64  // ID of the parent taxon
}

type taxon struct {
	data     Taxon
	children []*taxon
}

// A Taxonomy stores taxon IDs
type Taxonomy struct {
	ids  map[int64]*taxon
	root []*taxon // list parent-less of taxa
	tmp  []*taxon // temporal list of taxons
}

// NewTaxonomy creates a new empty taxonomy.
func NewTaxonomy() *Taxonomy {
	return &Taxonomy{ids: make(map[int64]*taxon)}
}

var headerCols = []string{
	"name",
	"author",
	"taxonKey",
	"rank",
	"status",
	"parent",
}

// Read reads a taxonomy from a TSV-encoded file.
func Read(r io.Reader) (*Taxonomy, error) {
	tab := tsv.NewReader(r)
	tab.Comma = '\t'

	header, err := tab.Read()
	if err != nil {
		return nil, fmt.Errorf("when reading taxonomy header: %v", err)
	}
	fields := make(map[string]int)
	for i, h := range header {
		h = strings.ToLower(h)
		fields[h] = i
	}
	for _, h := range headerCols {
		h = strings.ToLower(h)
		if _, ok := fields[h]; !ok {
			return nil, fmt.Errorf("when reading taxonomy header: expecting %q field", h)
		}
	}

	tx := NewTaxonomy()
	for {
		row, err := tab.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		ln, _ := tab.FieldPos(0)
		if err != nil {
			return nil, fmt.Errorf("taxonomy: row %d: %v", ln, err)
		}

		name := Canon(row[fields["name"]])
		if name == "" {
			continue
		}
		id, err := strconv.ParseInt(row[fields["taxonkey"]], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("taxonomy: row %d: %q: %v", ln, "taxonKey", err)
		}
		if _, ok := tx.ids[id]; ok {
			continue
		}

		var parent int64
		if p := row[fields["parent"]]; p != "" {
			parent, err = strconv.ParseInt(p, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("taxonomy: row %d: %q: %v", ln, "parent", err)
			}
		}

		data := Taxon{
			Name:   Canon(row[fields["name"]]),
			Author: strings.Join(strings.Fields(row[fields["author"]]), " "),
			ID:     id,
			Rank:   GetRank(row[fields["rank"]]),
			Status: strings.ToLower(strings.TrimSpace(row[fields["status"]])),
			Parent: parent,
		}
		tax := &taxon{data: data}
		tx.tmp = append(tx.tmp, tax)
		tx.ids[id] = tax
	}

	tx.Stage()
	return tx, nil
}

// AddFromGBIF add a taxon from a GBIF ID,
// as well as all the parents up to the given rank.
//
// Taxa will be added in a temporal space.
// To formally add the taxa to the taxonomy
// use the Stage method.
//
// It requires an internet connection.
func (tx *Taxonomy) AddFromGBIF(id int64, maxRank Rank) error {
	for {
		if id == 0 {
			return nil
		}
		if _, ok := tx.ids[id]; ok {
			return nil
		}

		sp, err := gbif.SpeciesID(strconv.FormatInt(id, 10))
		if err != nil {
			return err
		}

		data := Taxon{
			Name:   sp.CanonicalName,
			Author: sp.Authorship,
			ID:     id,
			Rank:   GetRank(sp.Rank),
			Status: strings.ToLower(sp.TaxonomicStatus),
		}
		if data.Name == "" {
			data.Name = sp.Species
		}

		// ignore BOLD "species"
		if data.Name == "" && strings.HasPrefix(sp.ScientificName, "BOLD:") {
			return nil
		}

		tax := &taxon{data: data}

		if data.Status == "accepted" && data.Rank != Unranked && data.Rank <= maxRank {
			if _, ok := tx.ids[sp.ParentKey]; ok {
				tax.data.Parent = sp.ParentKey
			}
			tx.tmp = append(tx.tmp, tax)
			tx.ids[id] = tax
			return nil
		}

		if sp.AcceptedKey != 0 {
			tax.data.Parent = sp.AcceptedKey
		} else if sp.ParentKey != 0 {
			tax.data.Parent = sp.ParentKey
		} else {
			tax.data.Parent = sp.BasionymKey
		}

		tx.tmp = append(tx.tmp, tax)
		tx.ids[id] = tax
		id = tax.data.Parent
	}
}

// An ErrAmbiguous is the error produced when searching for a name
// that has multiple possible resolutions in GBIF.
type ErrAmbiguous struct {
	Name string

	// The found IDs
	IDs []int64

	Err error
}

func (e *ErrAmbiguous) Error() string {
	return fmt.Sprintf("%v: name %q: found %d IDs", e.Err, e.Name, len(e.IDs))
}

func (e *ErrAmbiguous) Unwrap() error { return e.Err }

var errAmbiguous = errors.New("ambiguous taxon name")

// AddNameFromGBIF search for a taxon name in GBIF
// as well as all the parents up to the given rank.
//
// If multiple taxons with the indicated name were found
// it will look for a single accepted name.
// If there are multiple accepted names,
// or all the names are synonyms,
// then it will return an ErrAmbiguous error.
//
// Taxa will be added in a temporal space.
// To formally add the taxa to the taxonomy
// use the Stage method.
//
// It requires an internet connection.
func (tx *Taxonomy) AddNameFromGBIF(name string, maxRank Rank) error {
	ls, err := gbif.TaxonName(name)
	if err != nil {
		return err
	}
	if len(ls) == 0 {
		return nil
	}

	sp := ls[0]
	// ambiguous name,
	// search for any accepted name.
	if len(ls) > 1 {
		v := -1
		for i, sp := range ls {
			if sp.TaxonomicStatus != "ACCEPTED" {
				continue
			}
			if v >= 0 {
				v = -1
				break
			}
			v = i
		}
		if v < 0 {
			ids := make([]int64, 0, len(ls))
			for _, sp := range ls {
				ids = append(ids, sp.NubKey)
			}
			return &ErrAmbiguous{
				Name: name,
				IDs:  ids,
				Err:  errAmbiguous,
			}
		}
		sp = ls[v]
	}

	data := Taxon{
		Name:   sp.CanonicalName,
		Author: sp.Authorship,
		ID:     sp.NubKey,
		Rank:   GetRank(sp.Rank),
		Status: strings.ToLower(sp.TaxonomicStatus),
	}
	if data.Name == "" {
		data.Name = sp.Species
	}

	// ignore BOLD "species"
	if data.Name == "" && strings.HasPrefix(sp.ScientificName, "BOLD:") {
		return nil
	}

	tax := &taxon{data: data}

	if data.Status == "accepted" && data.Rank != Unranked && data.Rank <= maxRank {
		if _, ok := tx.ids[sp.ParentKey]; ok {
			tax.data.Parent = sp.ParentKey
		}
		tx.tmp = append(tx.tmp, tax)
		tx.ids[data.ID] = tax
		return nil
	}

	var pID int64
	if sp.AcceptedKey != 0 {
		pID = sp.AcceptedKey
	} else if sp.ParentKey != 0 {
		pID = sp.ParentKey
	} else {
		pID = sp.BasionymKey
	}
	if err := tx.AddFromGBIF(pID, maxRank); err != nil {
		return err
	}
	if _, ok := tx.ids[pID]; ok {
		tax.data.Parent = pID
	}
	tx.tmp = append(tx.tmp, tax)
	tx.ids[data.ID] = tax
	return nil
}

// MinRank returns the most inclusive rank
// found in the taxonomy.
func (tx *Taxonomy) MinRank() Rank {
	minRank := Unranked
	for _, tax := range tx.root {
		r := tax.minRank()
		if minRank == Unranked {
			minRank = r
		}
		if r == Unranked {
			continue
		}
		if r < minRank {
			minRank = r
		}
	}
	return minRank
}

func (tax *taxon) minRank() Rank {
	if tax.data.Rank != Unranked {
		return tax.data.Rank
	}

	minRank := Unranked
	for _, c := range tax.children {
		r := c.minRank()
		if minRank == Unranked {
			minRank = r
		}
		if r == Unranked {
			continue
		}
		if r < minRank {
			minRank = r
		}
	}
	return minRank
}

// Stage add the taxa in the temporal space
// to the taxonomy,
func (tx *Taxonomy) Stage() {
	if tx.tmp == nil {
		return
	}

	for _, tax := range tx.tmp {
		if tax.data.Parent == 0 {
			tx.root = append(tx.root, tax)
			continue
		}
		p := tx.ids[tax.data.Parent]
		p.children = append(p.children, tax)
	}
	tx.tmp = nil

	// sort
	slices.SortFunc(tx.root, func(a, b *taxon) int {
		if c := cmp.Compare(a.data.Name, b.data.Name); c != 0 {
			return c
		}
		return cmp.Compare(a.data.ID, b.data.ID)
	})
	for _, tax := range tx.ids {
		slices.SortFunc(tax.children, func(a, b *taxon) int {
			if a.data.Status != b.data.Status {
				if a.data.Status == "accepted" {
					return -1
				}
				if b.data.Status == "accepted" {
					return 1
				}
			}
			if c := cmp.Compare(a.data.Name, b.data.Name); c != 0 {
				return c
			}
			return cmp.Compare(a.data.ID, b.data.ID)
		})
	}
}

// Write writes a taxonomy into a TSV table.
func (tx *Taxonomy) Write(w io.Writer) error {
	// write data
	out := tsv.NewWriter(w)
	out.Comma = '\t'
	out.UseCRLF = true

	if err := out.Write(headerCols); err != nil {
		return fmt.Errorf("when writing taxonomy: %v", err)
	}
	for _, tax := range tx.root {
		if err := tax.write(out); err != nil {
			return err
		}
	}

	out.Flush()
	if err := out.Error(); err != nil {
		return fmt.Errorf("when writing taxonomy: %v", err)
	}

	return nil
}

func (tax *taxon) write(w *tsv.Writer) error {
	parent := ""
	if tax.data.Parent != 0 {
		parent = strconv.FormatInt(tax.data.Parent, 10)
	}
	row := []string{
		tax.data.Name,
		tax.data.Author,
		strconv.FormatInt(tax.data.ID, 10),
		tax.data.Rank.String(),
		tax.data.Status,
		parent,
	}
	if err := w.Write(row); err != nil {
		return fmt.Errorf("when writing taxonomy: %v", err)
	}

	for _, c := range tax.children {
		if err := c.write(w); err != nil {
			return err
		}
	}
	return nil
}

// Canon transforms a name into its canonical form.
func Canon(name string) string {
	name = strings.Join(strings.Fields(name), " ")
	if name == "" {
		return ""
	}
	name = strings.ToLower(name)
	r, n := utf8.DecodeRuneInString(name)
	return string(unicode.ToTitle(r)) + name[n:]
}
