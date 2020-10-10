package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type consumer struct {
	i *int
	l sync.Mutex
}


func main() {
	order := make([]int,0)
	for i, _ := range []int{1,2,3,4} {
		order = append(order, i)
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	for _,v := range order {
		fmt.Println(v)
	}
}
