package datastorex

import "github.com/drborges/gostream"

func NewProducer(stream Stream) gostream.ProduceStage {
	produce := func (out gostream.DataChannel) {
		defer close(out)
		for data := range stream.Next() {
			out <- data
		}
	}

	return func () gostream.DataChannel {
		out := make(gostream.DataChannel, stream.BufferSize())
		go produce(out)
		return out
	}
}