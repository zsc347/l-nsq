package quantile

type Result struct {
	Count       int                  `json:"count"`
	Percentiles []map[string]float64 `json:"percentiles"`
}

func (r *Result) String() string {
	var s []string
	// for _, item := range r.Percentiles {
	// 	s = append()
	// }
}
