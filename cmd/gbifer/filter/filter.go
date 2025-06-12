// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

// Package filter implements a command to select rows
// of a GBIF occurrence table
// with several criteria.
package filter

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/js-arias/command"
	"github.com/js-arias/gbifer/taxonomy"
	"github.com/js-arias/gbifer/tsv"
)

var Command = &command.Command{
	Usage: `filter [--tax <file>] [--country <file>]
	[-i|--input <file>] [-o|--output <file>]`,
	Short: "filter occurrence rows",
	Long: `
Command filter reads a GBIF occurrence table from the standard input and
selects rows by different criteria.

If the flag --tax is given with a file, a taxonomy will be read from the file,
and only the records that match the taxonomy will be selected.

With both the options --tax, with a taxonomy, and --country with a country
file, it will select rows that match both the taxonomy and the countries
defined in the country file. A country file should have the following columns:

	- name: to indicate the taxon name, the name should be mapped
	        unambiguously to a valid species in the taxonomy; otherwise,
		it will be ignored.
	- countryCode: an ISO 3166-1 alpha-2 code.

By default, it will read the data from the standard input; use the flag
--input, or -i, to select a particular file.
	
By default, the results will be printed in the standard output; use the flag
--output, or -o, to define an output file.
	`,
	SetFlags: setFlags,
	Run:      run,
}

var input string
var output string
var taxFile string
var countryFile string

func setFlags(c *command.Command) {
	c.Flags().StringVar(&input, "input", "", "")
	c.Flags().StringVar(&input, "i", "", "")
	c.Flags().StringVar(&output, "output", "", "")
	c.Flags().StringVar(&output, "o", "", "")
	c.Flags().StringVar(&taxFile, "tax", "", "")
	c.Flags().StringVar(&countryFile, "country", "", "")
}

func run(c *command.Command, args []string) (err error) {
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
	out := c.Stdout()
	if output != "" {
		var f *os.File
		f, err = os.Create(output)
		if err != nil {
			return err
		}
		defer func() {
			e := f.Close()
			if e != nil && err == nil {
				err = e
			}
		}()
		out = f
	} else {
		output = "stdout"
	}

	if countryFile != "" {
		tx, err := readTaxonomy()
		if err != nil {
			return err
		}

		tc, err := readCountryCodes(c.Stderr(), tx)
		if err != nil {
			return err
		}
		if err := filterCountries(in, out, tx, tc); err != nil {
			return err
		}

		return nil
	}
	if taxFile != "" {
		tx, err := readTaxonomy()
		if err != nil {
			return err
		}

		if err := filterTaxonomy(in, out, tx); err != nil {
			return err
		}
		return nil
	}

	return c.UsageError("expecting filter option")
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

func filterTaxonomy(r io.Reader, w io.Writer, tx *taxonomy.Taxonomy) error {
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

	out := tsv.NewWriter(w)
	out.Comma = '\t'
	out.UseCRLF = true

	// write header
	if err := out.Write(header); err != nil {
		return fmt.Errorf("when writing on %q: %v", output, err)
	}

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
		if tx.Taxon(id).ID == 0 {
			continue
		}
		if rk := tx.Rank(id); rk != taxonomy.Unranked && rk < taxonomy.Species {
			continue
		}

		if err := out.Write(row); err != nil {
			return fmt.Errorf("when writing on %q: %v", output, err)
		}
	}

	out.Flush()
	if err := out.Error(); err != nil {
		return fmt.Errorf("when writing on %q: %v", output, err)
	}
	return nil
}

type taxCountry struct {
	name      string
	id        int64
	countries map[string]bool
}

func readCountryCodes(stderr io.Writer, tx *taxonomy.Taxonomy) (map[int64]*taxCountry, error) {
	if tx == nil {
		return nil, errors.New("country codes require a taxonomy file")
	}

	f, err := os.Open(countryFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	tab := tsv.NewReader(f)
	tab.Comma = '\t'

	header, err := tab.Read()
	if err != nil {
		return nil, fmt.Errorf("country file %q: header: %v", countryFile, err)
	}

	cCol := -1
	taxCol := -1
	for i, h := range header {
		h = strings.ToLower(h)
		if h == "countrycode" {
			cCol = i
		}
		if h == "name" {
			taxCol = i
		}
	}
	if cCol < 0 || taxCol < 0 {
		return nil, fmt.Errorf("country file %q: without %q or %q fields", countryFile, "name", "countryCode")
	}

	cTax := make(map[int64]*taxCountry)
	for {
		row, err := tab.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		ln, _ := tab.FieldPos(0)
		if err != nil {
			return nil, fmt.Errorf("country file %q: row %d: %v", countryFile, ln, err)
		}

		cc := strings.TrimSpace(strings.ToUpper(row[cCol]))
		if len(cc) != 2 {
			return nil, fmt.Errorf("country file %q: row %d: invalid country code %q", countryFile, ln, cc)
		}

		name := row[taxCol]
		ids := tx.ByName(name)
		if len(ids) == 0 {
			continue
		}
		id := tx.AcceptedAndRanked(ids[0]).ID

		// check for ambiguous names
		if len(ids) > 1 {
			var amb []int64
			for _, v := range ids {
				x := tx.AcceptedAndRanked(v).ID
				if x != id {
					amb = append(amb, v)
				}
			}

			if len(amb) > 0 {
				amb = append([]int64{id}, amb...)
				fmt.Fprintf(stderr, "# ambiguous taxon name: %s\n", name)
				for _, id := range ids {
					fmt.Fprintf(stderr, "#\t%d\n", id)
				}
				continue
			}
		}

		tax, ok := cTax[id]
		if !ok {
			tax = &taxCountry{
				name:      taxonomy.Canon(name),
				id:        id,
				countries: make(map[string]bool),
			}
			cTax[id] = tax
		}

		tax.countries[cc] = true
	}
	return cTax, nil
}

func filterCountries(r io.Reader, w io.Writer, tx *taxonomy.Taxonomy, tc map[int64]*taxCountry) error {
	tab := tsv.NewReader(r)
	tab.Comma = '\t'

	header, err := tab.Read()
	if err != nil {
		return fmt.Errorf("when reading %q header: %v", input, err)
	}

	keyCol := -1
	taxCol := -1
	cCol := -1
	for i, h := range header {
		h = strings.ToLower(h)
		if h == "specieskey" {
			keyCol = i
		}
		if h == "taxonkey" {
			taxCol = i
		}
		if h == "countrycode" {
			cCol = i
		}
	}
	if keyCol < 0 || taxCol < 0 || cCol < 0 {
		return fmt.Errorf("input data %q without %q, %q, or %q fields", input, "speciesKey", "taxonKey", "countryCode")
	}

	out := tsv.NewWriter(w)
	out.Comma = '\t'
	out.UseCRLF = true

	// write header
	if err := out.Write(header); err != nil {
		return fmt.Errorf("when writing on %q: %v", output, err)
	}

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
		if tx.Taxon(id).ID == 0 {
			continue
		}
		if rk := tx.Rank(id); rk != taxonomy.Unranked && rk < taxonomy.Species {
			continue
		}

		v := tx.AcceptedAndRanked(id).ID
		if v == 0 {
			continue
		}
		tax, ok := tc[v]
		if !ok {
			continue
		}
		country := strings.TrimSpace(strings.ToUpper(row[cCol]))
		if !tax.countries[country] {
			continue
		}

		if err := out.Write(row); err != nil {
			return fmt.Errorf("when writing on %q: %v", output, err)
		}
	}

	out.Flush()
	if err := out.Error(); err != nil {
		return fmt.Errorf("when writing on %q: %v", output, err)
	}
	return nil
}
