// esdump uses the elasticsearch scroll API to stream documents to stdout.
// First written to extract samples from https:/search.fatcat.wiki, but might
// be more generic.
//
// $ esdump -server https://search.fatcat.wiki -index fatcat_release -q 'affiliation:"alberta"' > docs.ndj
//
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/sethgrid/pester"
	log "github.com/sirupsen/logrus"
)

var (
	query   = flag.String("q", "web+archiving", "query to run, empty means match all, example: 'affiliation:\"alberta\"'")
	index   = flag.String("i", "fatcat_release", "index name")
	server  = flag.String("s", "https://search.fatcat.wiki", "elasticsearch server")
	scroll  = flag.String("scroll", "10m", "context timeout")
	size    = flag.Int("size", 1000, "batch size")
	verbose = flag.Bool("verbose", false, "be verbose")

	exampleUsage = `esdump uses the elasticsearch scroll API to stream documents to stdout.
First written to extract samples from https:/search.fatcat.wiki, but might
be more generic.

    $ esdump -server https://search.fatcat.wiki -index fatcat_release -q 'affiliation:"alberta"'

`
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
// Not using the official esapi since esapi uses POST, whereas our public HTTP
// endpoint disallows anything but GET requests.
type BasicScroller struct {
	Server string
	Index  string
	Query  string
	Scroll string // timeout, e.g. "5m"
	Size   int    // number of docs per request

	total    int // docs already received
	scrollID string
	buf      bytes.Buffer
	err      error
}

// getScrollID returns a scroll identifier for a given index and query.
func (s *BasicScroller) getScrollID() (scrollID string, err error) {
	var (
		link = fmt.Sprintf(`%s/%s/_search?scroll=%s&size=%d&q=%s`, s.Server, s.Index, s.Scroll, s.Size, s.Query)
		sr   SearchResponse
	)
	log.Printf("init: %s", link)
	resp, err := pester.Get(link)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return "", err
	}
	log.Printf("init: %s", trim(sr.ScrollID, 25, "..."))
	return sr.ScrollID, nil
}

// Next fetches the next batch, which is accessible via Bytes or String methods.
func (s *BasicScroller) Next() bool {
	if s.err != nil {
		return false
	}
	if s.scrollID == "" {
		s.scrollID, s.err = s.getScrollID()
	}
	if s.err != nil {
		return false
	}
	var payload = struct {
		Scroll   string `json:"scroll"`
		ScrollID string `json:"scroll_id"`
	}{
		Scroll:   s.Scroll,
		ScrollID: s.scrollID,
	}
	var (
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
	req.Header.Add("Content-Type", "application/json")
	if s.err != nil {
		return false
	}
	log.Printf("%s [%d] [...]", req.URL, buf.Len())
	resp, s.err = pester.Do(req)
	if s.err != nil {
		return false
	}
	defer resp.Body.Close()
	s.buf.Reset()
	if _, s.err = io.Copy(&s.buf, resp.Body); s.err != nil {
		log.Printf("body was: %s", trim(s.buf.String(), 1024, fmt.Sprint("... (%d)", s.buf.Len())))
		return false
	}
	var sr SearchResponse
	if s.err = json.Unmarshal(s.buf.Bytes(), &sr); s.err != nil {
		return false
	}
	s.scrollID = sr.ScrollID
	s.total += len(sr.Hits.Hits)
	log.Printf("fetched=%d/%d, received=%d", s.total, sr.Hits.Total, s.buf.Len())
	log.Println(trim(s.scrollID, 30, "..."))
	return len(sr.Hits.Hits) > 0
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

// trim string to length.
func trim(s string, l int, ellipsis string) string {
	if len(s) < l {
		return s
	}
	return s[:l] + ellipsis
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), exampleUsage)
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if !*verbose {
		log.SetOutput(ioutil.Discard)
	}
	ss := &BasicScroller{
		Server: *server,
		Size:   *size,
		Index:  *index,
		Query:  *query,
		Scroll: *scroll,
	}
	for ss.Next() {
		fmt.Println(ss)
	}
	if ss.Err() != nil {
		log.Fatal(ss.Err())
	}
}
