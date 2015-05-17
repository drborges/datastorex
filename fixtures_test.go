package datastorex_test

import (
	"appengine"
	"appengine/datastore"
	"github.com/drborges/datastorex"
	"time"
)

type FakeDatastoreStream struct{}

func (stream FakeDatastoreStream) Next() chan datastorex.DatastoreItem {
	out := make(chan datastorex.DatastoreItem, 2)
	go func() {
		defer close(out)
		out <- datastorex.DatastoreItem{}
		out <- datastorex.DatastoreItem{}
	}()
	return out
}

func (stream FakeDatastoreStream) BufferSize() int {
	return 2
}

type User struct {
	Name string
}

func userProvider() datastorex.Entity {
	return &User{}
}

func newUserKey(c appengine.Context, name string) *datastore.Key {
	return datastore.NewKey(c, "User", name, 0, nil)
}

func createUsers(c appengine.Context, names ...string) {
	for _, name := range names {
		key := newUserKey(c, name)
		datastore.Put(c, key, &User{name})
	}

	// Give it some time until the entities are indexed and available to be queried
	time.Sleep(1 * time.Second)
}

func fetchUserByName(c appengine.Context, name string) User {
	var user User
	key := newUserKey(c, name)
	datastore.Get(c, key, &user)
	return user
}

func newDatastoreItem(c appengine.Context, user User) datastorex.DatastoreItem {
	return datastorex.DatastoreItem{newUserKey(c, user.Name), &user}
}
