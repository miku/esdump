package esdump

type MassQuery struct {
	Server  string // https://search.elastic.io
	Index   string
	Queries []string // query_string queries
	Result  []byte
	Error   err
}

func (q *MassQuery) Run() error {
	return nil
}
