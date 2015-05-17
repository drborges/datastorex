package datastorex_test

import (
	"testing"
	"github.com/drborges/gogae/goexpect"
	"github.com/drborges/datastorex"
)

func TestProducer(t *testing.T) {
	expect := goexpect.New(t)

	produce := datastorex.NewProducer(FakeDatastoreStream{})

	items := []interface{} {}
	for item := range produce() {
		items = append(items, item)
	}

	expect(len(items)).ToBe(2)
}
