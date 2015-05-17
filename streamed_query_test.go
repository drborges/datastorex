package datastorex_test

import (
	"appengine/aetest"
	"appengine/datastore"
	"github.com/drborges/datastorex"
	"github.com/drborges/goexpect"
	"testing"
)

func TestStream(t *testing.T) {
	expect := goexpect.New(t)

	c, _ := aetest.NewContext(nil)
	defer c.Close()

	createUsers(c, "Bianca", "Diego", "Ygor")

	stream := datastorex.StreamedQuery{
		Context:        c,
		BufferSize:     3,
		EntityProvider: userProvider,
		Query:          datastore.NewQuery("User").Limit(1),
	}

	items := []datastorex.Entity{}
	for item := range stream.Next() {
		items = append(items, item)
	}

	expect(len(items)).ToBe(3)

	item0 := datastorex.DatastoreItem{newUserKey(c, "Bianca"), &User{"Bianca"}}
	item1 := datastorex.DatastoreItem{newUserKey(c, "Diego"), &User{"Diego"}}
	item2 := datastorex.DatastoreItem{newUserKey(c, "Ygor"), &User{"Ygor"}}

	expect(item0).ToDeepEqual(items[0])
	expect(item1).ToDeepEqual(items[1])
	expect(item2).ToDeepEqual(items[2])
}
