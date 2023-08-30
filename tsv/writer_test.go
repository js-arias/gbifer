// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

package tsv_test

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/js-arias/gbifer/tsv"
)

func TestWrite(t *testing.T) {
	tests := map[string]struct {
		input  [][]string
		output string
	}{
		"simple": {
			input:  [][]string{{"abc"}},
			output: "abc\r\n",
		},
		"quotes": {
			input:  [][]string{{`"abc"`}},
			output: `"abc"` + "\r\n",
		},
		"with tab": {
			input:  [][]string{{"abc\tdef"}},
			output: `abc\tdef` + "\r\n",
		},
		"empty": {
			input:  [][]string{{""}},
			output: "\r\n",
		},
		"empty fields": {
			input:  [][]string{{"", ""}},
			output: "\t\r\n",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			w := tsv.NewWriter(&buf)
			for _, r := range test.input {
				if err := w.Write(r); err != nil {
					t.Fatalf("%s: unexpected error: %q", name, err)
				}
			}
			w.Flush()
			if err := w.Error(); err != nil {
				t.Fatalf("%s: unexpected error: %q", name, err)
			}
			got := buf.String()
			if got != test.output {
				t.Errorf("%s: got %q, want %q", name, got, test.output)
			}

			// do not check empty output
			if name == "empty" {
				return
			}

			rr := tsv.NewReader(strings.NewReader(got))
			var rows [][]string
			for {
				rec, err := rr.Read()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("%s-read: unexpected error: %q", name, err)
				}
				rows = append(rows, rec)
			}
			if !reflect.DeepEqual(rows, test.input) {
				t.Errorf("%s-read %q: got %q, want %q", name, got, rows, test.input)
			}
		})
	}
}
