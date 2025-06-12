// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

// Package filter implements a command to select taxons in a taxonomy
// from a name list.
package filter

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/js-arias/command"
	"github.com/js-arias/gbifer/taxonomy"
)

var Command = &command.Command{
	Usage: `filter [--del]
	[-i|--input] [-o|--output]
	<file> [<file>...]`,
	Short: "filter taxonomy",
	Long: `
Command filter reads one or more files with taxon names and remove all names
in a taxonomy that are not present in the filter files.

One or more filter files should be given as arguments of the command. The
filter files are only a text file with a single name per line. Blank lines or
lines started with sharp symbol ('#') will be ignored.

By default, the read names will be kept in the taxonomy. When a name is read,
its parents and all of its children (including synonyms), will be kept. If the
flag --del is given, then the name and all of its children, will be deleted.

By default, the taxonomy will be read from the standard input. Use the flag
--input, or -i, to select a particular taxonomy file.

By default, the results will be printed in the standard output. Use the flag
--output, or -o, to define an output file.
	`,
	SetFlags: setFlags,
	Run:      run,
}

var delFlag bool
var input string
var output string

func setFlags(c *command.Command) {
	c.Flags().BoolVar(&delFlag, "del", false, "")
	c.Flags().StringVar(&input, "input", "", "")
	c.Flags().StringVar(&input, "i", "", "")
	c.Flags().StringVar(&output, "output", "", "")
	c.Flags().StringVar(&output, "o", "", "")
}

func run(c *command.Command, args []string) (err error) {
	if len(args) == 0 {
		return c.UsageError("expecting filter file argument")
	}

	tx, err := readTaxonomy(c.Stdin())
	ids := make(map[int64]bool)

	for _, a := range args {
		if err := readNames(a, tx, ids); err != nil {
			return err
		}
	}

	toDel := ids
	if !delFlag {
		toDel = make(map[int64]bool)
		for _, id := range tx.IDs() {
			if ids[id] {
				continue
			}
			toDel[id] = true
		}
	}
	delIDs(tx, toDel)

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

	if err := tx.Write(out); err != nil {
		return fmt.Errorf("when writing to %q: %v", output, err)
	}
	return nil
}

func delIDs(tx *taxonomy.Taxonomy, ids map[int64]bool) {
	for id := range ids {
		tx.Del(id)
	}
}

func readTaxonomy(r io.Reader) (*taxonomy.Taxonomy, error) {
	if input != "" {
		f, err := os.Open(input)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		r = f
	} else {
		input = "stdin"
	}

	tx, err := taxonomy.Read(r)
	if err != nil {
		return nil, fmt.Errorf("on file %q: %v", input, err)
	}
	return tx, nil
}

func readNames(name string, tx *taxonomy.Taxonomy, ids map[int64]bool) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for i := 1; ; i++ {
		ln, err := r.ReadString('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		ln = strings.TrimSpace(ln)
		if len(ln) == 0 {
			continue
		}
		if ln[0] == '#' {
			continue
		}

		toAdd := tx.ByName(ln)
		for _, id := range toAdd {
			// The ID is already added
			if _, ok := ids[id]; ok {
				continue
			}
			ids[id] = true
			if delFlag {
				continue
			}

			cIDs := tx.Children(id)
			for _, c := range cIDs {
				ids[c] = true
			}
			pIDs := tx.Parents(id)
			for _, p := range pIDs {
				ids[p] = true
			}
		}
	}
	return nil
}
