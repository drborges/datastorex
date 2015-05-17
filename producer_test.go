package datastorex_test

import (
	"github.com/drborges/datastorex"
	"github.com/drborges/goexpect"
	"testing"
)

func TestProducer(t *testing.T) {
	expect := goexpect.New(t)

	produce := datastorex.NewProducer(FakeDatastoreStream{})

	items := []interface{}{}
	for item := range produce() {
		items = append(items, item)
	}

	expect(len(items)).ToBe(2)
}
