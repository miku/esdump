# esdump

Stream docs from Elasticsearch to stdout for ad-hoc data mangling using the
[Scroll
API](https://www.elastic.co/guide/en/elasticsearch/guide/master/scroll.html#scroll).
Just like [solrdump](https://github.com/ubleipzig/solrdump), but for
[elasticsearch](elastic.co/).


## Usage

```
$ esdump -h
esdump uses the elasticsearch scroll API to stream documents to stdout.
First written to extract samples from https:/search.fatcat.wiki, but might
be more generic.

    $ esdump -server https://search.fatcat.wiki -index fatcat_release -q 'affiliation:"alberta"' > docs.ndj

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

