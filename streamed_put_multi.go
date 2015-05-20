package datastorex

import (
	"appengine"
	"appengine/datastore"
	"github.com/drborges/gostream"
)

type Batch struct {
	Keys     []*datastore.Key
	Entities []gostream.Data
}

func NewPutMultiStream(c appengine.Context, batchSize int) gostream.PipeStage {
	pipe := func(in, out chan gostream.Data) {
		defer close(out)
		keys := []*datastore.Key{}
		entities := []gostream.Data{}

		for item := range in {
			datastoreItem := item.(DatastoreItem)
			keys = append(keys, datastoreItem.Key)
			entities = append(entities, datastoreItem.Entity)

			if len(keys) == batchSize {
				if _, err := datastore.PutMulti(c, keys, entities); err != nil {
					panic("Could not save batch into datastore. Error: " + err.Error())
				}
				out <- Batch{keys, entities}
				keys = []*datastore.Key{}
				entities = []gostream.Data{}
			}
		}

		if len(keys) > 0 {
			if _, err := datastore.PutMulti(c, keys, entities); err != nil {
				panic("Could not save batch into datastore. Error: " + err.Error())
			}
			out <- Batch{keys, entities}
		}
	}

	return gostream.NewPipeStage(pipe)
}
