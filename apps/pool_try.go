package main

import (
	"fmt"
	"sync"
)

func main() {
	p := &sync.Pool{
		New: func() interface{} {
			return 0
		},
	}

	g := p.Get()
	fmt.Println(g)
	a := p.Get().(int)
	p.Put(1)
	b := p.Get().(int)
	fmt.Println(a, b)

	var e interface{} = 100
	switch value := e.(type) {
	case int:
		fmt.Println("int", value)
	case string:
		fmt.Println("string", value)
	default:
		fmt.Println("unknown", value)
	}
}
