package datastorex_test

import (
	"appengine"
	"appengine/aetest"
	"github.com/drborges/datastorex"
	"github.com/drborges/goexpect"
	"github.com/drborges/gostream"
	"testing"
)

var (
	batchSize = 2
	userA     = User{"UserA"}
	userB     = User{"UserB"}
	userC     = User{"UserC"}
)

func produceUsers(c appengine.Context, in chan gostream.Data, users ...User) {
	defer close(in)
	for _, user := range users {
		in <- newDatastoreItem(c, user)
	}
}

func TestPutMultiStream(t *testing.T) {
	expect := goexpect.New(t)
	c, _ := aetest.NewContext(nil)
	defer c.Close()

	in := make(chan gostream.Data)
	go produceUsers(c, in, userA, userB, userC)

	out := datastorex.NewPutMultiStream(c, batchSize)(in)
	batches := []datastorex.Batch{}
	for data := range out {
		batch := data.(datastorex.Batch)
		batches = append(batches, batch)
	}

	expect(len(batches)).ToBe(2)
	expect(fetchUserByName(c, userA.Name)).ToBe(userA)
	expect(fetchUserByName(c, userB.Name)).ToBe(userB)
	expect(fetchUserByName(c, userC.Name)).ToBe(userC)
}
