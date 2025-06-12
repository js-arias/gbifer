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
	children map[int64]*taxon
}

// A Taxonomy stores taxon IDs
type Taxonomy struct {
	ids   map[int64]*taxon
	root  map[int64]*taxon          // list parent-less of taxa
	names map[string]map[int64]bool // map of taxon names to IDs
}

// NewTaxonomy creates a new empty taxonomy.
func NewTaxonomy() *Taxonomy {
	return &Taxonomy{
		ids:   make(map[int64]*taxon),
		root:  make(map[int64]*taxon),
		names: make(map[string]map[int64]bool),
	}
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
		tax := &taxon{
			data:     data,
			children: make(map[int64]*taxon),
		}
		tx.ids[id] = tax

		byName, ok := tx.names[data.Name]
		if !ok {
			byName = make(map[int64]bool)
			tx.names[data.Name] = byName
		}
		byName[id] = true

		if tax.data.Parent != 0 {
			p := tx.ids[tax.data.Parent]
			p.children[tax.data.ID] = tax
		} else {
			tx.root[tax.data.ID] = tax
		}
	}
	return tx, nil
}

// Accepted return the accepted taxon from a given ID.
func (tx *Taxonomy) Accepted(id int64) Taxon {
	for {
		tax, ok := tx.ids[id]
		if !ok {
			return Taxon{}
		}
		if tax.data.Status == "accepted" {
			return tax.data
		}
		id = tax.data.Parent
	}
}

// AcceptedAndRanked return the accepted and ranked taxon from a given ID.
func (tx *Taxonomy) AcceptedAndRanked(id int64) Taxon {
	for {
		tax, ok := tx.ids[id]
		if !ok {
			return Taxon{}
		}
		if tax.data.Status == "accepted" && tax.data.Rank != Unranked {
			return tax.data
		}
		id = tax.data.Parent
	}
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
	var ls []*gbif.Species
	for {
		if id == 0 {
			break
		}
		if _, ok := tx.ids[id]; ok {
			break
		}

		sp, err := gbif.SpeciesID(strconv.FormatInt(id, 10))
		if err != nil {
			return err
		}

		ls = append([]*gbif.Species{sp}, ls...)
		status := strings.ToLower(sp.TaxonomicStatus)
		r := GetRank(sp.Rank)
		if status == "accepted" && r != Unranked && r <= maxRank {
			break
		}

		var pID int64
		if sp.AcceptedKey != 0 {
			pID = sp.AcceptedKey
		} else if sp.ParentKey != 0 {
			pID = sp.ParentKey
		} else {
			pID = sp.BasionymKey
		}
		id = pID
	}

	for _, sp := range ls {
		tx.AddSpecies(sp)
	}
	return nil
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
	name = Canon(name)
	if name == "" {
		return nil
	}

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

	status := strings.ToLower(sp.TaxonomicStatus)
	r := GetRank(sp.Rank)
	if status == "accepted" && r != Unranked && r <= maxRank {
		tx.AddSpecies(sp)
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

	tx.AddSpecies(sp)
	return nil
}

// AddSpecies add a GBIF Species type from an external source.
func (tx *Taxonomy) AddSpecies(sp *gbif.Species) {
	if _, ok := tx.ids[sp.NubKey]; ok {
		return
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

	// Ignore taxons with empty names
	if data.Name == "" {
		return
	}

	// Ignore taxons with empty IDs
	if sp.NubKey == 0 {
		data.ID = sp.Key
	}
	if data.ID == 0 {
		return
	}

	tax := &taxon{
		data:     data,
		children: make(map[int64]*taxon),
	}
	var pID int64
	if sp.AcceptedKey != 0 {
		pID = sp.AcceptedKey
	} else if sp.ParentKey != 0 {
		pID = sp.ParentKey
	} else {
		pID = sp.BasionymKey
	}
	if _, ok := tx.ids[pID]; ok {
		tax.data.Parent = pID
		p := tx.ids[tax.data.Parent]
		p.children[tax.data.ID] = tax
	} else {
		tx.root[tax.data.ID] = tax
	}

	tx.ids[data.ID] = tax
	byName, ok := tx.names[tax.data.Name]
	if !ok {
		byName = make(map[int64]bool)
		tx.names[tax.data.Name] = byName
	}
	byName[data.ID] = true
}

// ByName returns the IDs of all the taxons with a given name.
func (tx *Taxonomy) ByName(name string) []int64 {
	name = Canon(name)
	if name == "" {
		return nil
	}

	ids, ok := tx.names[name]
	if !ok {
		return nil
	}
	if len(ids) == 0 {
		return nil
	}

	v := make([]int64, 0, len(ids))
	for id := range ids {
		v = append(v, id)
	}
	slices.Sort(v)

	return v
}

// Children return the IDs of all children (sub-taxa) of a taxon
// (including synonyms).
func (tx *Taxonomy) Children(id int64) []int64 {
	tax, ok := tx.ids[id]
	if !ok {
		return nil
	}

	ids := tax.allChildren()
	slices.Sort(ids)

	return ids
}

func (tax *taxon) allChildren() []int64 {
	var ids []int64
	for _, c := range tax.children {
		cIDs := c.allChildren()
		ids = append(ids, c.data.ID)
		ids = append(ids, cIDs...)
	}
	return ids
}

// Del removes a taxon
// and all of its descendants from a taxonomy,
func (tx *Taxonomy) Del(id int64) {
	tax, ok := tx.ids[id]
	if !ok {
		return
	}
	tx.delTaxon(tax)

	pID := tax.data.Parent
	p, ok := tx.ids[pID]
	if !ok {
		delete(tx.root, tax.data.ID)
		return
	}
	delete(p.children, tax.data.ID)
}

func (tx *Taxonomy) delTaxon(tax *taxon) {
	for _, c := range tax.children {
		tx.delTaxon(c)
	}
	tax.children = nil
	byName := tx.names[tax.data.Name]
	delete(byName, tax.data.ID)
	if len(byName) == 0 {
		delete(tx.names, tax.data.Name)
	}
	delete(tx.ids, tax.data.ID)
}

// IDs return the ID of all taxons in the taxonomy.
func (tx *Taxonomy) IDs() []int64 {
	ids := make([]int64, 0, len(tx.ids))
	for _, tax := range tx.ids {
		ids = append(ids, tax.data.ID)
	}
	slices.Sort(ids)

	return ids
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

// Parents return the ID of all parents of a taxon.
func (tx *Taxonomy) Parents(id int64) []int64 {
	tax, ok := tx.ids[id]
	if !ok {
		return nil
	}
	var ids []int64
	for i := 0; i < 20; i++ {
		pID := tax.data.Parent
		p, ok := tx.ids[pID]
		if !ok {
			break
		}
		ids = append(ids, pID)
		tax = p
	}
	slices.Sort(ids)
	return ids
}

// Rank returns the first defined rank
// of a taxon,
// or any of its parents.
func (tx *Taxonomy) Rank(id int64) Rank {
	for id != 0 {
		tax, ok := tx.ids[id]
		if !ok {
			return Unranked
		}
		if tax.data.Rank != Unranked {
			return tax.data.Rank
		}
		id = tax.data.Parent
	}
	return Unranked
}

// Taxon returns a taxon with a given ID.
func (tx *Taxonomy) Taxon(id int64) Taxon {
	tax, ok := tx.ids[id]
	if !ok {
		return Taxon{}
	}
	return tax.data
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

	rootChildren := make([]*taxon, 0, len(tx.root))
	for _, tax := range tx.root {
		rootChildren = append(rootChildren, tax)
	}
	slices.SortFunc(rootChildren, func(a, b *taxon) int {
		if a.data.Rank != b.data.Rank {
			if a.data.Rank == Unranked {
				return -1
			}
			if b.data.Rank == Unranked {
				return 1
			}
			return cmp.Compare(a.data.Rank, b.data.Rank)
		}
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

	for _, tax := range rootChildren {
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

	children := make([]*taxon, 0, len(tax.children))
	for _, c := range tax.children {
		children = append(children, c)
	}
	slices.SortFunc(children, func(a, b *taxon) int {
		if a.data.Rank != b.data.Rank {
			if a.data.Rank == Unranked {
				return -1
			}
			if b.data.Rank == Unranked {
				return 1
			}
			return cmp.Compare(a.data.Rank, b.data.Rank)
		}
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
	for _, c := range children {
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
