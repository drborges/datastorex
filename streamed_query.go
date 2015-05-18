package datastorex

import (
	"appengine"
	"appengine/datastore"
	"github.com/drborges/gostream"
)

type EntityProvider func() gostream.Data

type DatastoreItem struct {
	Key    *datastore.Key
	Entity interface{}
}

type Stream interface {
	Next() chan DatastoreItem
}

type StreamedQuery struct {
	BufferSize     int
	Context        appengine.Context
	Query          *datastore.Query
	EntityProvider EntityProvider
}

func (this *StreamedQuery) Next() chan DatastoreItem {
	if this.BufferSize == 0 {
		this.BufferSize = 100
	}

	out := make(chan DatastoreItem, this.BufferSize)

	go func() {
		defer close(out)

		for {
			var nextItem error
			var key *datastore.Key
			iterator := this.Query.Run(this.Context)

			currentPageCursor, err := iterator.Cursor()

			for {
				entity := this.EntityProvider()
				key, nextItem = iterator.Next(entity)
				if nextItem == datastore.Done {
					break
				}
				out <- DatastoreItem{key, entity}
			}

			nextPageCursor, err := iterator.Cursor()
			if err != nil {
				this.Context.Errorf("Something went wrong when getting current cursor.")
				return
			}

			if nextPageCursor == currentPageCursor {
				this.Context.Infof("Finished fetching all items.")
				break
			}

			this.Query = this.Query.Start(nextPageCursor)
			currentPageCursor = nextPageCursor
		}
	}()

	return out
}
