// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

package tsv_test

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/js-arias/gbifer/tsv"
)

func TestRead(t *testing.T) {
	tests := map[string]struct {
		input  string
		output [][]string
	}{
		"simple": {
			input:  "a\tb\tc\n",
			output: [][]string{{"a", "b", "c"}},
		},
		"CrLn": {
			input:  "a\tb\r\nc\td\r\n",
			output: [][]string{{"a", "b"}, {"c", "d"}},
		},
		"bare CR": {
			input:  "a\tb\rc\td\r\n",
			output: [][]string{{"a", "b\rc", "d"}},
		},
		"no EOL": {
			input:  "a\tb\tc",
			output: [][]string{{"a", "b", "c"}},
		},
		"blank line": {
			input:  "a\tb\tc\n\nd\te\tf\n\n",
			output: [][]string{{"a", "b", "c"}, {"d", "e", "f"}},
		},
		"bare quotes": {
			input:  `a "word"	"1"2"	a"	"b`,
			output: [][]string{{`a "word"`, `"1"2"`, `a"`, `"b`}},
		},
		"bare double quotes": {
			input:  `a""b	c`,
			output: [][]string{{`a""b`, "c"}},
		},
		"empty fields": {
			input:  "\t\t\n",
			output: [][]string{{"", "", ""}},
		},
		"single field": {
			input:  "abc\r\n",
			output: [][]string{{"abc"}},
		},
		"escaped sequence": {
			input:  `abc\tdef` + "\n",
			output: [][]string{{"abc\tdef"}},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			r := tsv.NewReader(strings.NewReader(test.input))
			var got [][]string
			for {
				row, err := r.Read()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("%s: unexpected error: %q", name, err)
				}
				got = append(got, row)
			}
			if !reflect.DeepEqual(got, test.output) {
				t.Errorf("%s: got %q, want %q", name, got, test.output)
			}
		})
	}
}

func TestReadError(t *testing.T) {
	var tests = map[string]struct {
		input string
		err   error
	}{
		"bad field count": {
			input: "a\tb\tc\nd\te",
			err:   tsv.ErrFieldCount,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			r := tsv.NewReader(strings.NewReader(test.input))
			for {
				_, err := r.Read()
				if errors.Is(err, io.EOF) {
					break
				}
				if err == nil {
					continue
				}
				if !errors.Is(err, test.err) {
					t.Errorf("%s: got error %q, want %q", name, err, test.err)
				}
				return
			}
			t.Errorf("%s: expecting error %q", name, test.err)
		})
	}
}
