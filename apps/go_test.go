package main

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

func TestScanf(t *testing.T) {
	var depth int64

	f, err := os.OpenFile("input.txt", os.O_RDONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	_, err = fmt.Fscanf(f, "%d\n", &depth)
	if err != nil {
		panic(err)
	}
	fmt.Println(depth)
}

func TestPool(t *testing.T) {
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

func call(x int) (y int) {
	fmt.Println(x)
	fmt.Println(y)
	return
}

func TestCall(t *testing.T) {
	fmt.Println(call(4))
}
