package esdump

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"

	"github.com/sethgrid/pester"
	"golang.org/x/sync/errgroup"
)

// MassQuery runs many requests in parallel. Does no pagination.
type MassQuery struct {
	Server  string // https://search.elastic.io
	Index   string
	Queries []string // query_string queries
	Size    int
	Writer  io.Writer
	Err     error
}

func (q *MassQuery) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	var (
		ch   = make(chan []byte)
		done = make(chan bool)
		w    = q.Writer
	)
	for _, query := range q.Queries {
		g.Go(func() error {
			link := fmt.Sprintf(`%s/%s/_search?size=%d&q=%s`,
				q.Server, q.Index, q.Size, url.QueryEscape(query))
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
			return nil
		})
	}
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
	if err := g.Wait(); err != nil {
		return err
	}
	close(ch)
	<-done
	return nil
}
