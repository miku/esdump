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
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/miku/esdump"
	"github.com/miku/esdump/stringutil"
	"github.com/sethgrid/pester"
	log "github.com/sirupsen/logrus"
)

var (
	query       = flag.String("q", "*", `lucene syntax query to run, example: 'affiliation:"alberta"'`)
	index       = flag.String("i", "fatcat_release", "index name")
	server      = flag.String("s", "https://search.fatcat.wiki", "elasticsearch server")
	scroll      = flag.String("scroll", "5m", "context timeout")
	size        = flag.Int("size", 1000, "batch size")
	verbose     = flag.Bool("verbose", false, "be verbose")
	showVersion = flag.Bool("v", false, "show version")
	idsFile     = flag.String("ids", "", "a path to a file with one id per line to fetch")
	massQuery   = flag.String("mq", "", "path to file, one lucene query per line")

	exampleUsage = `esdump uses the elasticsearch scroll API to stream
documents to stdout. First written to extract samples from
https:/search.fatcat.wiki (a scholarly communications preservation and
discovery project).

    $ esdump -s https://search.fatcat.wiki -i fatcat_release -q 'web archiving'

`
	Version   = "0.1.6"
	Commit    = "dev"
	Buildtime = ""
)

// identifierDump reads each line (id) from r and will create batched ids
// requests and will write the responses to the given writer.
func identifierDump(r io.Reader, w io.Writer) error {
	var (
		br    = bufio.NewReader(r)
		batch []string
	)
	queryFunc := func(batch []string) error {
		var (
			quoted []string
			link   string
		)
		for _, id := range batch {
			quoted = append(quoted, fmt.Sprintf("%q", id))
		}
		// TODO: marshal this.
		query := fmt.Sprintf(`{"query": {"ids": {"values": [%s]}}}`, strings.Join(quoted, ", "))
		if *verbose {
			log.Printf("%s (%d ids)", stringutil.Shorten(query, 80), len(quoted))
		}
		if *index == "" {
			link = fmt.Sprintf("%s/_search", *server)
		} else {
			link = fmt.Sprintf("%s/%s/_search", *server, *index)
		}
		req, err := http.NewRequest("GET", link, strings.NewReader(query))
		req.Header.Set("Content-Type", "application/json")
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
		return nil
	}
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		line = strings.ReplaceAll(strings.TrimSpace(line), "\n", "")
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}
		batch = append(batch, line)
		if len(batch)%*size == 0 {
			if err := queryFunc(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if err := queryFunc(batch); err != nil {
		return err
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
			var buf bytes.Buffer
			for _, field := range strings.Fields(*idsFile) {
				fmt.Fprintln(&buf, strings.TrimSpace(field))
			}
			r = &buf
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
	case *massQuery:
		// Read lines from file, run MassQuery.
		mq := esdump.MassQuery{
			Server: *server,
			Index:  *index,
			Writer: os.Stdout,
		}
		// TODO: Abtract various reading routines.
	default:
		ss := &esdump.BasicScroller{
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
			log.Printf("%d docs in %v (%d docs/s)", ss.Total(), ss.Elapsed(),
				int(float64(ss.Total())/ss.Elapsed().Seconds()))
		}
	}
}
