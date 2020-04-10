# esdump

Stream docs from Elasticsearch to stdout for ad-hoc data mangling using the
[Scroll
API](https://www.elastic.co/guide/en/elasticsearch/guide/master/scroll.html#scroll).
Just like [solrdump](https://github.com/ubleipzig/solrdump), but for
[elasticsearch](elastic.co/).

Libraries can use both GET and POST requests to issue scroll requests.

* [elasticsearch-py](https://github.com/elastic/elasticsearch-py/blob/c0767a9569a719dcb15adec91a88afc32b27b1b0/elasticsearch/client/__init__.py#L1300-L1323) uses POST
* [esapi](https://github.com/elastic/go-elasticsearch/blob/6f36a473b19f05f20933da8f59347b308ab46594/esapi/api.scroll.go#L65) uses GET

This tool uses HTTP GET only, and does not clear scrolls (which would probably
use
[DELETE](https://github.com/elastic/go-elasticsearch/blob/6f36a473b19f05f20933da8f59347b308ab46594/esapi/api.clear_scroll.go#L60))
so this tools work with read-only servers, that only allow GET.

## Install

```
$ go get github.com/miku/esdump/cmd/esdump
```

Or via a [release](https://github.com/miku/esdump/releases).

## Usage

```
$ esdump -h
esdump uses the elasticsearch scroll API to stream documents to stdout.
First written to extract samples from https:/search.fatcat.wiki, but might
be more generic.

    $ esdump -s https://search.fatcat.wiki -i fatcat_release -q 'affiliation:"alberta"'

Usage of ./esdump:
  -i string
        index name (default "fatcat_release")
  -q string
        query to run, empty means match all, example: 'affiliation:"alberta"' (default "web+archiving")
  -s string
        elasticsearch server (default "https://search.fatcat.wiki")
  -scroll string
        context timeout (default "10m")
  -size int
        batch size (default 1000)
  -verbose
        be verbose
```

