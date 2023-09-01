// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

// Package match implements a command to match taxa in a taxonomy
// with taxa from a GBIF occurrence table.
package match

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/js-arias/command"
	"github.com/js-arias/gbifer/gbif"
	"github.com/js-arias/gbifer/taxonomy"
	"github.com/js-arias/gbifer/tsv"
)

var Command = &command.Command{
	Usage: "match --file <file> [-i|--input <file>]",
	Short: "match taxons to taxonomy",
	Long: `
Command match reads a taxonomy and a GBIF occurrence table and extracts the
taxa in the occurrence table that match any of the taxons in the taxonomy. The
extraction was only done at the species level.

A taxonomy file is required and must be defined with the flag --file.

By default, it will read the data from the standard input; use the flag
--input, or -i, to select a particular file.

This command requires an internet connection.
	`,
	SetFlags: setFlags,
	Run:      run,
}

var input string
var taxFile string

func setFlags(c *command.Command) {
	c.Flags().StringVar(&input, "input", "", "")
	c.Flags().StringVar(&input, "i", "", "")
	c.Flags().StringVar(&taxFile, "file", "", "")
}

func run(c *command.Command, args []string) (err error) {
	tx, err := readTaxonomy()
	if err != nil {
		return err
	}
	gbif.Open()

	in := c.Stdin()
	if input != "" {
		f, err := os.Open(input)
		if err != nil {
			return err
		}
		defer f.Close()
		in = f
	} else {
		input = "stdin"
	}

	if err := readTable(in, tx); err != nil {
		return err
	}
	tx.Stage()

	var f *os.File
	f, err = os.Create(taxFile)
	if err != nil {
		return err
	}
	defer func() {
		e := f.Close()
		if e != nil && err == nil {
			err = e
		}
	}()

	if err := tx.Write(f); err != nil {
		return fmt.Errorf("when writing to %q: %v", taxFile, err)
	}

	return nil
}

func readTaxonomy() (*taxonomy.Taxonomy, error) {
	f, err := os.Open(taxFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	tx, err := taxonomy.Read(f)
	if err != nil {
		return nil, fmt.Errorf("on file %q: %v", taxFile, err)
	}
	return tx, nil
}

func readTable(r io.Reader, tx *taxonomy.Taxonomy) error {
	tab := tsv.NewReader(r)
	tab.Comma = '\t'

	header, err := tab.Read()
	if err != nil {
		return fmt.Errorf("when reading %q header: %v", input, err)
	}

	keyCol := -1
	taxCol := -1
	for i, h := range header {
		h = strings.ToLower(h)
		if h == "specieskey" {
			keyCol = i
		}
		if h == "taxonkey" {
			taxCol = i
		}
	}
	if keyCol < 0 && taxCol < 0 {
		return fmt.Errorf("input data %q without %q or %q fields", input, "speciesKey", "taxonKey")
	}

	unMatch := make(map[int64]bool)
	for {
		row, err := tab.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		ln, _ := tab.FieldPos(0)
		if err != nil {
			return fmt.Errorf("table %q: row %d: %v", input, ln, err)
		}

		var key string
		if keyCol >= 0 {
			key = row[keyCol]
			if key == "" {
				continue
			}
		}
		if taxCol >= 0 {
			key = row[taxCol]
		}
		if key == "" {
			continue
		}
		id, err := strconv.ParseInt(key, 10, 64)
		if err != nil {
			return fmt.Errorf("table %q: row %d: %v", input, ln, err)
		}

		ls, err := searchID(id, tx, unMatch)
		if err != nil {
			return err
		}
		for _, sp := range ls {
			tx.AddSpecies(sp)
		}
	}

	return nil
}

func searchID(id int64, tx *taxonomy.Taxonomy, unMatch map[int64]bool) ([]*gbif.Species, error) {
	var ls []*gbif.Species
	for {
		if id == 0 {
			break
		}
		if unMatch[id] {
			break
		}

		if tx.Taxon(id).ID == id {
			return ls, nil
		}

		sp, err := gbif.SpeciesID(strconv.FormatInt(id, 10))
		if err != nil {
			return nil, err
		}

		ls = append([]*gbif.Species{sp}, ls...)

		r := taxonomy.GetRank(sp.Rank)
		if sp.TaxonomicStatus == "ACCEPTED" && r != taxonomy.Unranked && r <= taxonomy.Species {
			break
		}

		if sp.AcceptedKey != 0 {
			id = sp.AcceptedKey
		} else if sp.ParentKey != 0 {
			id = sp.ParentKey
		} else {
			id = sp.BasionymKey
		}
	}

	// mark unmatched IDs
	// so we don't search again
	for _, sp := range ls {
		unMatch[sp.Key] = true
		if sp.NubKey != 0 && sp.Key != sp.NubKey {
			unMatch[sp.Key] = true
		}
	}
	return nil, nil
}
