// Copyright © 2023 J. Salvador Arias <jsalarias@gmail.com>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

// Package gbif implements an interface for the GBIF <http://www.gbif.org>
// portal.
package gbif

import (
	"net/http"
	"sync"
	"time"
)

// Retry is the number of times a request will be retried
// before aborted.
var Retry = 5

// Timeout is the timeout of the http request.
var Timeout = 20 * time.Second

// Wait is the waiting time for a new request
// (we don't want to overload the GBIF server!).
var Wait = time.Millisecond * 300

// Buffer is the maximum number of requests in the request queue.
var Buffer = 10

// Open opens GBIF requests.
func Open() {
	once.Do(initReqs)
}

const wsHead = "http://api.gbif.org/v1/"

type request struct {
	req string
	ans chan *http.Response
	err chan error
}

// NewRequest sends a request to the request channel.
func newRequest(req string) request {
	r := request{
		req: wsHead + req,
		ans: make(chan *http.Response),
		err: make(chan error),
	}
	reqChan.cReqs <- r
	return r
}

type reqChanType struct {
	cReqs chan request
}

var once sync.Once

var reqChan *reqChanType

func initReqs() {
	http.DefaultClient.Timeout = Timeout
	reqChan = &reqChanType{cReqs: make(chan request, Buffer)}
	go reqChan.reqs()
}

func (rc *reqChanType) reqs() {
	for r := range rc.cReqs {
		answer, err := http.Get(r.req)
		if err != nil {
			r.err <- err
			continue
		}
		r.ans <- answer

		// we do not want to overload the gbif server.
		time.Sleep(Wait)
	}
}
