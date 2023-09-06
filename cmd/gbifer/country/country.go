// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

// Command country creates a list of taxa and the countries
// with localities
// from a GBIF occurrence table.
package country

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/js-arias/command"
	"github.com/js-arias/gbifer/taxonomy"
	"github.com/js-arias/gbifer/tsv"
)

var Command = &command.Command{
	Usage: `country [--tax <file>]
	[-i|--input <file>] [-o|--output <file>]`,
	Short: "create a taxon-country table",
	Long: `
Command country reads a GBIF occurrence table from the standard input and
prints a table with the taxon names and countries with specimen records.

A country table has the following columns:

	- name: the taxon name. If a taxonomy is used, the ranked and accepted
	        names will be used.
	- countryCode: an ISO 3166-1 alpha-2 code of the country.
	- country: name of the country

If the flag --tax is given with a file, a taxonomy will be read from the file,
and only the records that match the taxonomy will be selected.

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

func setFlags(c *command.Command) {
	c.Flags().StringVar(&input, "input", "", "")
	c.Flags().StringVar(&input, "i", "", "")
	c.Flags().StringVar(&output, "output", "", "")
	c.Flags().StringVar(&output, "o", "", "")
	c.Flags().StringVar(&taxFile, "tax", "", "")
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

	var tx *taxonomy.Taxonomy
	if taxFile != "" {
		var err error
		tx, err = readTaxonomy()
		if err != nil {
			return err
		}
	}

	tc, err := readTable(in, tx)
	if err != nil {
		return err
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
	if err := writeCountryTable(out, tc); err != nil {
		return err
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

type taxCountry struct {
	name      string
	id        int64
	countries map[string]bool
}

func readTable(r io.Reader, tx *taxonomy.Taxonomy) (map[int64]*taxCountry, error) {
	tab := tsv.NewReader(r)
	tab.Comma = '\t'

	header, err := tab.Read()
	if err != nil {
		return nil, fmt.Errorf("when reading %q header: %v", input, err)
	}

	keyCol := -1
	taxCol := -1
	cCol := -1
	spCol := -1
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
		if h == "species" {
			spCol = i
		}
	}
	if cCol < 0 || (keyCol < 0 && taxCol < 0) {
		return nil, fmt.Errorf("input data %q without %q or %q fields", input, "countryCode", "taxonKey")
	}
	if tx == nil && spCol < 0 {
		return nil, fmt.Errorf("input data %q without %q field", input, "species")
	}

	cTax := make(map[int64]*taxCountry)
	for {
		row, err := tab.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		ln, _ := tab.FieldPos(0)
		if err != nil {
			return nil, fmt.Errorf("table %q: row %d: %v", input, ln, err)
		}

		var key string
		if keyCol >= 0 {
			key = row[keyCol]
			if key == "" {
				continue
			}
		}

		cc := strings.ToUpper(row[cCol])
		if cc == "" {
			continue
		}
		if _, ok := iso3166[cc]; !ok {
			return nil, fmt.Errorf("table %q: row %d: invalid country code: %q", input, ln, cc)
		}

		if tx != nil {
			if taxCol >= 0 {
				key = row[taxCol]
			}
			if key == "" {
				continue
			}

			id, err := strconv.ParseInt(key, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("table %q: row %d: key: %v", input, ln, err)
			}
			tax := tx.AcceptedAndRanked(id)
			if tax.ID == 0 {
				continue
			}
			tc, ok := cTax[tax.ID]
			if !ok {
				tc = &taxCountry{
					name:      tax.Name,
					id:        tax.ID,
					countries: make(map[string]bool),
				}
				cTax[tax.ID] = tc
			}
			tc.countries[cc] = true
			continue
		}

		name := taxonomy.Canon(row[spCol])
		if name == "" {
			continue
		}
		id, err := strconv.ParseInt(key, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("table %q: row %d: key: %v", input, ln, err)
		}

		tc, ok := cTax[id]
		if !ok {
			tc = &taxCountry{
				name:      taxonomy.Canon(name),
				id:        id,
				countries: make(map[string]bool),
			}
			cTax[id] = tc
		}
		tc.countries[cc] = true
	}

	return cTax, nil
}

func writeCountryTable(w io.Writer, cTax map[int64]*taxCountry) error {
	out := tsv.NewWriter(w)
	out.Comma = '\t'
	out.UseCRLF = true

	// write header
	header := []string{
		"name",
		"countryCode",
		"country",
	}
	if err := out.Write(header); err != nil {
		return fmt.Errorf("when writing on %q: %v", output, err)
	}

	ids := make([]int64, 0, len(cTax))
	for id := range cTax {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b int64) int {
		return cmp.Compare(cTax[a].name, cTax[b].name)
	})

	for _, id := range ids {
		tc := cTax[id]

		ccs := make([]string, 0, len(tc.countries))
		for cc := range tc.countries {
			ccs = append(ccs, cc)
		}
		slices.SortFunc(ccs, func(a, b string) int {
			return cmp.Compare(iso3166[a], iso3166[b])
		})

		for _, cc := range ccs {
			row := []string{
				tc.name,
				cc,
				iso3166[cc],
			}
			if err := out.Write(row); err != nil {
				return err
			}
		}
	}

	out.Flush()
	if err := out.Error(); err != nil {
		return fmt.Errorf("when writing on %q: %v", output, err)
	}
	return nil
}
