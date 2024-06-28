# esdump

Stream docs from Elasticsearch to stdout for ad-hoc data mangling using the
[Scroll
API](https://www.elastic.co/guide/en/elasticsearch/guide/master/scroll.html#scroll).
Just like [solrdump](https://github.com/ubleipzig/solrdump), but for
[elasticsearch](https://elastic.co/). Since esdump 0.1.11, the default operator can be set explicitly and changed from `OR` to `AND`.

Libraries can use both GET and POST requests to issue scroll requests.

* [elasticsearch-py](https://github.com/elastic/elasticsearch-py/blob/c0767a9569a719dcb15adec91a88afc32b27b1b0/elasticsearch/client/__init__.py#L1300-L1323) uses POST
* [esapi](https://github.com/elastic/go-elasticsearch/blob/6f36a473b19f05f20933da8f59347b308ab46594/esapi/api.scroll.go#L65) uses GET

This tool uses HTTP GET only, and does not clear scrolls (which would probably
use
[DELETE](https://github.com/elastic/go-elasticsearch/blob/6f36a473b19f05f20933da8f59347b308ab46594/esapi/api.clear_scroll.go#L60))
so this tool works with read-only servers, that only allow GET.

## Install

```
$ go install github.com/miku/esdump/cmd/esdump@latest
```

Or via a [release](https://github.com/miku/esdump/releases).

## Usage

```
esdump uses the elasticsearch scroll API to stream documents to stdout.

Originally written to extract samples from https://search.fatcat.wiki (a
scholarly communications preservation and discovery project).

    $ esdump -s https://search.fatcat.wiki -i fatcat_release -q 'web archiving'

Usage of ./esdump:
  -i string
        index name (default "fatcat_release")
  -ids string
        a path to a file with one id per line to fetch
  -l int
        limit number of documents fetched, zero means no limit
  -mq string
        path to file, one lucene query per line
  -op string
        default operator for query string queries (default "AND")
  -q string
        lucene syntax query to run, example: 'affiliation:"alberta"' (default "*")
  -s string
        elasticsearch server (default "https://search.fatcat.wiki")
  -scroll string
        context timeout (default "5m")
  -size int
        batch size (default 1000)
  -v    show version
  -verbose
        be verbose
```

## Performance data point(s)

```
925636 docs in 4m47.460217252s (3220 docs/s)
```

## TODO

* [ ] move to [`search_after`](https://www.elastic.co/guide/en/elasticsearch/reference/current/scroll-api.html)
