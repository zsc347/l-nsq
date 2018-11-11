package httprouter

import (
	"net/http"
)

type Handle func(http.ResponseWriter, *http.Request, Params)

type Param struct {
	Key   string
	Value string
}

type Params []Param

func (ps Params) ByName(name string) string {
	for i := range ps {
		if ps[i].Key == name {
			return ps[i].Value
		}
	}
	return ""
}
