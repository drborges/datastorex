package datastorex_test

import (
	"appengine/aetest"
	"appengine/datastore"
	"github.com/drborges/datastorex"
	"github.com/drborges/goexpect"
	"testing"
	"github.com/drborges/gostream"
)

func TestStream(t *testing.T) {
	expect := goexpect.New(t)

	c, _ := aetest.NewContext(nil)
	defer c.Close()

	createUsers(c, "UserA", "UserB", "UserC")

	stream := datastorex.StreamedQuery{
		Context:        c,
		BufferSize:     3,
		EntityProvider: userProvider,
		Query:          datastore.NewQuery("User").Limit(1),
	}

	items := []gostream.Data{}
	for item := range stream.Next() {
		items = append(items, item)
	}

	expect(len(items)).ToBe(3)

	expect(newDatastoreItem(c, User{"UserA"})).ToDeepEqual(items[0])
	expect(newDatastoreItem(c, User{"UserB"})).ToDeepEqual(items[1])
	expect(newDatastoreItem(c, User{"UserC"})).ToDeepEqual(items[2])
}
