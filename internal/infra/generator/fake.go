package generator

import (
	"encoding/json"
	"github.com/brianvoe/gofakeit"
)

type Foo struct {
	Bar      string
	Int      int
	Pointer  *int
	Name     string  `fake:"{firstname}"`   // Any available function all lowercase
	Sentence string  `fake:"{sentence:3}"`  // Can call with parameters
	RandStr  string  `fake:"{randomstring:[hello,world]}"`
	Number   string  `fake:"{number:1,10}"` // Comma separated for multiple values
	Skip     *string `fake:"skip"`          // Set to "skip" to not generate data for
}

type FooBar struct {
	Bars    []string `fake:"{name}"`              // Array of random size (1-10) with fake function applied
	Foos    []Foo    `fakesize:"3"`               // Array of size specified with faked struct
	FooBars []Foo    `fake:"{name}" fakesize:"3"` // Array of size 3 with fake function applied
}

func NewFooBar()string{
	var f FooBar
	gofakeit.Struct(&f)
	b, _ := json.Marshal(f)
	return string(b)
}