package esdump

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/miku/esdump/stringutil"
	"github.com/sethgrid/pester"
)

// SearchResponse is an basic search response with an unparsed source field.
type SearchResponse struct {
	Hits struct {
		Hits []struct {
			Id     string          `json:"_id"`
			Index  string          `json:"_index"`
			Score  float64         `json:"_score"`
			Source json.RawMessage `json:"_source"`
			Type   string          `json:"_type"`
		} `json:"hits"`
		MaxScore float64 `json:"max_score"`
		Total    int64   `json:"total"`
	} `json:"hits"`
	ScrollID string `json:"_scroll_id"`
	Shards   struct {
		Failed     int64 `json:"failed"`
		Skipped    int64 `json:"skipped"`
		Successful int64 `json:"successful"`
		Total      int64 `json:"total"`
	} `json:"_shards"`
	TimedOut bool  `json:"timed_out"`
	Took     int64 `json:"took"`
}

// BasicScroller abstracts iteration over larger result sets via
// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-body.html#request-body-search-scroll.
// Not using the official library since esapi (may) use POST (and other verbs),
// whereas some endpoints disallow anything but GET requests.
type BasicScroller struct {
	Server     string // https://search.elastic.io
	Index      string
	Query      string // query_string query, will be url escaped, so ok to write: '(f:value) OR (g:"hi there")'
	Scroll     string // context timeout, e.g. "5m"
	Size       int    // number of docs per request
	MaxRetries int    // Retry of stranger things, like "unexpected EOF"

	id      string       // will be determined by first request, might change during the scroll
	buf     bytes.Buffer // buffer for response body
	total   int          // docs already received
	err     error
	started time.Time
}

// initialRequest returns a scroll identifier for a given index and query.
func (s *BasicScroller) initialRequest() (id string, err error) {
	s.started = time.Now()
	var (
		link = fmt.Sprintf(`%s/%s/_search?scroll=%s&size=%d`, s.Server, s.Index, s.Scroll, s.Size)
		req  *http.Request
		resp *http.Response
		sr   SearchResponse
	)
	log.Printf("init: %s", link)
	req, err = http.NewRequest("GET", link, strings.NewReader(s.Query))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err = pester.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	s.buf.Reset()
	tee := io.TeeReader(resp.Body, &s.buf)
	if err = json.NewDecoder(tee).Decode(&sr); err != nil {
		return
	}
	s.total += len(sr.Hits.Hits)
	log.Printf("init: %s", stringutil.Trim(sr.ScrollID, 25, "..."))
	return sr.ScrollID, nil
}

// Next fetches the next batch, which is accessible via Bytes or String
// methods. Returns true, if successful, false if stream ended or an error
// occured. The error can be accessed separately.
func (s *BasicScroller) Next() bool {
	if s.err != nil {
		return false
	}
	if s.id == "" {
		s.id, s.err = s.initialRequest()
		return s.err == nil
	}
	var (
		retry = -3
		sleep = 10 * time.Second
		sr    SearchResponse
	)
	// Only wrapped in a loop to escape unexpected EOF, other errors are not
	// retried.
	for {
		if retry == s.MaxRetries {
			s.err = fmt.Errorf("max retries exceeded")
			return false
		}
		var (
			payload = struct {
				Scroll   string `json:"scroll"`
				ScrollID string `json:"scroll_id"`
			}{
				Scroll:   s.Scroll,
				ScrollID: s.id,
			}
			link = fmt.Sprintf("%s/_search/scroll", s.Server)
			buf  bytes.Buffer
			req  *http.Request
			resp *http.Response
		)
		enc := json.NewEncoder(&buf)
		if s.err = enc.Encode(payload); s.err != nil {
			return false
		}
		req, s.err = http.NewRequest("GET", link, &buf)
		if s.err != nil {
			return false
		}
		req.Header.Add("Content-Type", "application/json")
		log.Printf("%s [%d] [...]", req.URL, buf.Len())
		resp, s.err = pester.Do(req)
		if s.err != nil {
			return false
		}
		defer resp.Body.Close()
		s.buf.Reset()
		_, s.err = io.Copy(&s.buf, resp.Body) // we get an occasional "unexpected EOF" here, but why?
		if s.err == nil {
			break
		}
		log.Printf("body was: %s", stringutil.Trim(s.buf.String(), 1024, fmt.Sprintf("... (%d)", s.buf.Len())))
		log.Printf("failed to copy response body: %v (%s)", s.err, link)
		log.Printf("retrying in %s", sleep)
		time.Sleep(sleep)
		retry++
	}
	if s.err = json.Unmarshal(s.buf.Bytes(), &sr); s.err != nil {
		return false
	}
	s.id = sr.ScrollID
	s.total += len(sr.Hits.Hits)
	log.Printf("fetched=%d/%d (%0.2f%%), received=%d",
		s.total, sr.Hits.Total, float64(s.total)/float64(sr.Hits.Total)*100, s.buf.Len())
	log.Println(stringutil.Shorten(s.id, 40))
	if len(sr.Hits.Hits) == 0 && int64(s.total) != sr.Hits.Total {
		log.Printf("warn: partial result")
	}
	return len(sr.Hits.Hits) > 0 && int64(s.total) <= sr.Hits.Total
}

// Bytes returns the current response body.
func (s *BasicScroller) Bytes() []byte {
	return s.buf.Bytes()
}

// String returns current request body as string.
func (s *BasicScroller) String() string {
	return s.buf.String()
}

// Err returns any error.
func (s *BasicScroller) Err() error {
	return s.err
}

// Elapsed returns the elasped time.
func (s *BasicScroller) Elapsed() time.Duration {
	return time.Since(s.started)
}

// Total returns total documents retrieved.
func (s *BasicScroller) Total() int {
	return s.total
}
