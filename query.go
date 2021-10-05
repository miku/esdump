package esdump

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/sethgrid/pester"
	"golang.org/x/sync/errgroup"
)

// MassQuery runs many requests in parallel. Does no pagination. Useful for the
// moment to get the result set size for a given query.  TODO: This is just a
// special case to request many URL in parallel and combining the results.
// TODO: Look into "multisearch",
// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html.
type MassQuery struct {
	Server  string // https://search.elastic.io
	Index   string
	Queries []string // query_string queries
	Size    int
	Writer  io.Writer
	Err     error
}

func (q *MassQuery) Run(ctx context.Context) error {
	g, _ := errgroup.WithContext(ctx)
	var (
		ch   = make(chan []byte)
		done = make(chan bool)
		w    = q.Writer
	)
	go func() {
		// Write out all results.
		for blob := range ch {
			if _, err := w.Write(blob); err != nil {
				q.Err = err
			}
			if _, err := io.WriteString(w, "\n"); err != nil {
				q.Err = err
			}
			if q.Err != nil {
				break
			}
		}
		done <- true
	}()

	// Bounded concurrency.
	sem := make(chan struct{}, 4)

	for _, query := range q.Queries {
		sem <- struct{}{}
		query := query
		g.Go(func() error {
			link := fmt.Sprintf(`%s/%s/_search?size=%d&q=%s`,
				q.Server, q.Index, q.Size, query)
			resp, err := pester.Get(link)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			b, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			ch <- b
			<-sem
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	close(ch)
	<-done
	return nil
}
