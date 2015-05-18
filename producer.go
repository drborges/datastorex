package datastorex

import "github.com/drborges/gostream"

func NewProducer(stream Stream) gostream.ProduceStage {
	produce := func(out chan gostream.Data) {
		defer close(out)
		for data := range stream.Next() {
			out <- data
		}
	}

	return func() chan gostream.Data {
		out := make(chan gostream.Data, stream.BufferSize())
		go produce(out)
		return out
	}
}
