// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

// Package export implements a command to export
// a GBIF occurrence table
// into a TSV file compatible with the RFC 4180.
package export

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/js-arias/command"
	"github.com/js-arias/gbifer/taxonomy"
	"github.com/js-arias/gbifer/tsv"
)

var Command = &command.Command{
	Usage: `export [-tax <file>]
	[-i|--input <file>] [-o|--output <file>]`,
	Short: "export to TSV RFC 4180 file",
	Long: `
Command export reads a GBIF occurrence table from the standard input and
prints a TSV file compatible with RFC 4180 (using tabs instead of commas).

Once a file is exported, it is no longer compatible with GBIFer, as GBIF
occurrence tables do not follow the quotation rules of RFC 4180. Also, it uses
the column names "latitude" and "longitude" instead of "DecimalLatitude" and
"DecimalLongitude". While no longer compatible with GBIFer, the output file
will preserve GBIF ID fields, so it will be possible to trace the origin of
each occurrence.

By default, it will use the species name from the occurrence file. If the flag
--tax is defined, the indicated file will be used to retrieve the accepted
species name from the taxonomy.

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

	var tx *taxonomy.Taxonomy
	if taxFile != "" {
		var err error
		tx, err = readTaxonomy()
		if err != nil {
			return err
		}
	}

	if err := readTable(in, out, tx); err != nil {
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

var outFields = []string{
	"species",
	"speciesID",
	"latitude",
	"longitude",
	"geoRefUncertainty",
	"gbifID",
	"catalog",
	"occurrenceID",
	"date",
	"country",
	"province",
	"county",
	"locality",
	"taxon",
	"taxonID",
	"dataset",
	"datasetID",
	"publisher",
	"reference",
	"license",
}

func readTable(r io.Reader, w io.Writer, tx *taxonomy.Taxonomy) error {
	tab := tsv.NewReader(r)
	tab.Comma = '\t'

	header, err := tab.Read()
	if err != nil {
		return fmt.Errorf("when reading %q header: %v", input, err)
	}
	fields := make(map[string]int, len(header))
	for i, h := range header {
		h = strings.ToLower(h)
		fields[h] = i
	}

	out := csv.NewWriter(w)
	out.Comma = '\t'
	out.UseCRLF = true

	// write outfield header
	if err := out.Write(outFields); err != nil {
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

		var species, taxon string
		if f, ok := fields["species"]; ok {
			species = taxonomy.Canon(row[f])
			taxon = species
		}

		var taxID, spID int64
		if f, ok := fields["specieskey"]; ok {
			if row[f] == "" {
				continue
			}
			spID, err = strconv.ParseInt(row[f], 10, 64)
			if err != nil {
				return fmt.Errorf("table %q: row %d: field %q: %v", input, ln, "speciesKey", err)
			}
			taxID = spID
			if tx != nil {
				tax := tx.AcceptedAndRanked(spID)
				if tax.ID == 0 {
					continue
				}
				species = tax.Name
				spID = tax.ID
			}
		}
		if spID == 0 {
			continue
		}
		if species == "" {
			continue
		}

		var lat float64
		if f, ok := fields["decimallatitude"]; ok {
			lat, err = strconv.ParseFloat(row[f], 64)
			if err != nil {
				return fmt.Errorf("table %q: row %d: field %q: %v", input, ln, "decimalLatitude", err)
			}
			if lat < -90 || lat > 90 {
				return fmt.Errorf("table %q: row %d: field %q: invalid latitude: %.6f", input, ln, "decimalLatitude", lat)
			}
		}
		var lon float64
		if f, ok := fields["decimallongitude"]; ok {
			lon, err = strconv.ParseFloat(row[f], 64)
			if err != nil {
				return fmt.Errorf("table %q: row %d: field %q: %v", input, ln, "decimalLongitude", err)
			}
			if lon < -180 || lon > 180 {
				return fmt.Errorf("table %q: row %d: field %q: invalid longitude: %.6f", input, ln, "decimalLongitude", lat)
			}
		}
		if lat == 0 || lon == 0 {
			continue
		}

		var geoRefUncertainty int64
		if f, ok := fields["coordinateuncertaintyinmeters"]; ok {
			geoRefUncertainty, err = strconv.ParseInt(row[f], 10, 64)
			if err != nil {
				geoRefUncertainty = 0
			}
		}

		var gbifID string
		if f, ok := fields["gbifid"]; ok {
			gbifID = row[f]
		}

		var institute string
		if f, ok := fields["institutioncode"]; ok {
			institute = row[f]
			if institute == "" {
				if f, ok := fields["ownerinstitutioncode"]; ok {
					institute = row[f]
				}
			}
			if institute == "" {
				if f, ok := fields["institutionid"]; ok {
					institute = row[f]
				}
			}
		}
		var collection string
		if f, ok := fields["collectioncode"]; ok {
			collection = row[f]
			if collection == "" {
				if f, ok := fields["collectionid"]; ok {
					collection = row[f]
				}
			}
		}
		var catNumber string
		if f, ok := fields["catalognumber"]; ok {
			catNumber = row[f]
			if catNumber == "" {
				catNumber = "gbif:" + gbifID
			}
		}
		var catalog = catNumber
		if institute != "" {
			catalog = institute + ":" + collection + ":" + catNumber
		}

		var occurrenceID string
		if f, ok := fields["occurrenceid"]; ok {
			occurrenceID = row[f]
		}

		var date time.Time
		dateOK := false
		if f, ok := fields["eventdate"]; ok {
			date, err = time.Parse("2006-01-02T15:04:05", row[f])
			if err == nil {
				dateOK = true
			}
		}
		if !dateOK {
			var year int
			if f, ok := fields["year"]; ok {
				year, err = strconv.Atoi(row[f])
				if err != nil || year < 1700 {
					year = 1700
				}
			}
			var month int
			if f, ok := fields["month"]; ok {
				month, err = strconv.Atoi(row[f])
				if err != nil || month > 12 {
					month = 1
				}
			}
			var day int
			if f, ok := fields["day"]; ok {
				day, err = strconv.Atoi(row[f])
				if err != nil || day > 31 {
					day = 1
				}
			}
			date = time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
		}

		var country string
		if f, ok := fields["countrycode"]; ok {
			country = row[f]
		}
		var province string
		if f, ok := fields["stateprovince"]; ok {
			province = row[f]
		}
		var county string
		if f, ok := fields["county"]; ok {
			county = row[f]
		}
		var locality string
		if f, ok := fields["verbatimlocality"]; ok {
			locality = row[f]
		}

		if f, ok := fields["scientificname"]; ok {
			taxon = row[f]
		}
		if f, ok := fields["taxonkey"]; ok {
			txID, err := strconv.ParseInt(row[f], 10, 64)
			if err == nil {
				taxID = txID
			}
			if tx != nil {
				tax := tx.Taxon(txID)
				if tax.ID == 0 {
					continue
				}
				taxon = tax.Name
				taxID = tax.ID
			}
		}

		var dataset string
		if f, ok := fields["datasetname"]; ok {
			dataset = row[f]
		}
		var datasetID string
		if f, ok := fields["datasetkey"]; ok {
			datasetID = row[f]
		}
		var publisher string
		if f, ok := fields["publisher"]; ok {
			publisher = row[f]
		}

		var reference string
		if f, ok := fields["bibliographiccitation"]; ok {
			reference = row[f]
		}
		var license string
		if f, ok := fields["license"]; ok {
			license = row[f]
		}

		nr := []string{
			species,
			strconv.FormatInt(spID, 10),
			strconv.FormatFloat(lat, 'f', 7, 64),
			strconv.FormatFloat(lon, 'f', 7, 64),
			strconv.FormatInt(geoRefUncertainty, 10),
			gbifID,
			catalog,
			occurrenceID,
			date.Format(time.RFC3339),
			country,
			province,
			county,
			locality,
			taxon,
			strconv.FormatInt(taxID, 10),
			dataset,
			datasetID,
			publisher,
			reference,
			license,
		}
		if err := out.Write(nr); err != nil {
			return fmt.Errorf("when writing on %q: %v", output, err)
		}
	}

	out.Flush()
	if err := out.Error(); err != nil {
		return fmt.Errorf("when writing on %q: %v", output, err)
	}
	return nil
}
