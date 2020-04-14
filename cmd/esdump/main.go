// esdump uses the elasticsearch scroll API to stream documents to stdout.
// First written to extract samples from https:/search.fatcat.wiki, but might
// be more generic. It uses HTTP GET only.
//
// $ esdump -s https://search.fatcat.wiki -i fatcat_release -q 'web archiving'
//
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/sethgrid/pester"
	log "github.com/sirupsen/logrus"
)

var (
	query       = flag.String("q", "web archiving", `query to run, empty means match all, example: 'affiliation:"alberta"'`)
	index       = flag.String("i", "fatcat_release", "index name")
	server      = flag.String("s", "https://search.fatcat.wiki", "elasticsearch server")
	scroll      = flag.String("scroll", "5m", "context timeout")
	size        = flag.Int("size", 1000, "batch size")
	verbose     = flag.Bool("verbose", false, "be verbose")
	showVersion = flag.Bool("v", false, "show version")
	idsFile     = flag.String("ids", "", "a path to a file with one id per line to fetch")

	exampleUsage = `esdump uses the elasticsearch scroll API to stream
documents to stdout. First written to extract samples from
https:/search.fatcat.wiki (a scholarly communications preservation and
discovery project).

    $ esdump -s https://search.fatcat.wiki -i fatcat_release -q 'web archiving'

`
	Version   = "0.1.4"
	Commit    = "dev"
	Buildtime = ""
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
	Server string // https://search.elastic.io
	Index  string
	Query  string // query_string query, will be url escaped, so ok to write: '(f:value) OR (g:"hi there")'
	Scroll string // context timeout, e.g. "5m"
	Size   int    // number of docs per request

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
		link = fmt.Sprintf(`%s/%s/_search?scroll=%s&size=%d&q=%s`, s.Server, s.Index, s.Scroll, s.Size, url.QueryEscape(s.Query))
		resp *http.Response
		sr   SearchResponse
	)
	log.Printf("init: %s", link)
	resp, err = pester.Get(link)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	tee := io.TeeReader(resp.Body, &s.buf)
	if err = json.NewDecoder(tee).Decode(&sr); err != nil {
		return
	}
	s.total += len(sr.Hits.Hits)
	log.Printf("init: %s", trim(sr.ScrollID, 25, "..."))
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
	if s.err != nil {
		return false
	}
	var payload = struct {
		Scroll   string `json:"scroll"`
		ScrollID string `json:"scroll_id"`
	}{
		Scroll:   s.Scroll,
		ScrollID: s.id,
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
		log.Printf("body was: %s", trim(s.buf.String(), 1024, fmt.Sprintf("... (%d)", s.buf.Len())))
		return false
	}
	var sr SearchResponse
	if s.err = json.Unmarshal(s.buf.Bytes(), &sr); s.err != nil {
		return false
	}
	s.id = sr.ScrollID
	s.total += len(sr.Hits.Hits)
	log.Printf("fetched=%d/%d (%0.2f%%), received=%d",
		s.total, sr.Hits.Total, float64(s.total)/float64(sr.Hits.Total)*100, s.buf.Len())
	log.Println(shorten(s.id, 40))
	if len(sr.Hits.Hits) == 0 && int64(s.total) != sr.Hits.Total {
		log.Printf("warn: partial result")
	}
	return len(sr.Hits.Hits) > 0 && int64(s.total) < sr.Hits.Total
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

// identifierDump reads each line (id) from r and will create batched ids
// requests and will write the responses to the given writer.
func identifierDump(r io.Reader, w io.Writer) error {
	var (
		br     = bufio.NewReader(r)
		batch  []string
		quoted []string
		link   string
	)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		line = strings.TrimSpace(line)
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}
		batch = append(batch, line)
		if len(batch)%1000 == 0 {
			queryFunc := func() error {
				for _, id := range batch {
					quoted = append(quoted, fmt.Sprintf("%q", id))
				}
				query := fmt.Sprintf(`{"query": {"ids": {"type": "_doc", "values": [%s]}`, strings.Join(quoted, ", "))
				if *index == "" {
					link = fmt.Sprintf("%s/_search", *server)
				} else {
					link = fmt.Sprintf("%s/%s/_search", *server, *index)
				}
				req, err := http.NewRequest("GET", link, strings.NewReader(query))
				if err != nil {
					return err
				}
				resp, err := pester.Do(req)
				if err != nil {
					return err
				}
				defer resp.Body.Close()
				if _, err := io.Copy(w, resp.Body); err != nil {
					return err
				}
				if _, err := io.WriteString(w, "\n"); err != nil {
					return err
				}
				batch, quoted = batch[:0], quoted[:0]
				return nil
			}
			if err := queryFunc(); err != nil {
				return err
			}
		}
	}
	return nil
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), exampleUsage)
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if *showVersion {
		fmt.Printf("%s %s %s\n", Version, Commit, Buildtime)
		os.Exit(0)
	}
	if !*verbose {
		log.SetOutput(ioutil.Discard)
	}
	switch {
	case *idsFile != "":
		var r io.Reader
		if _, err := os.Stat(*idsFile); os.IsNotExist(err) {
			ids := strings.Join(strings.Fields(*idsFile), "\n")
			r = strings.NewReader(ids)
		} else {
			f, err := os.Open(*idsFile)
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()
			r = f
		}
		bw := bufio.NewWriter(os.Stdout)
		defer bw.Flush()
		if err := identifierDump(r, bw); err != nil {
			log.Fatal(err)
		}
	default:
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
		if *verbose {
			log.Printf("%d docs in %v (%d docs/s)", ss.Total(), ss.Elapsed(), int(float64(ss.Total())/ss.Elapsed().Seconds()))
		}
	}
}

func shorten(s string, l int) string {
	if len(s) < l {
		return s
	}
	k := l / 2
	return s[:k] + " [...] " + s[len(s)-k:] + fmt.Sprintf(" [%d]", len(s))
}

// trim string to length.
func trim(s string, l int, ellipsis string) string {
	if len(s) < l {
		return s
	}
	return s[:l] + ellipsis
}
