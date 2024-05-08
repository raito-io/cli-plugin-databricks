package repo

import (
	"context"

	"github.com/databricks/databricks-sdk-go/listing"
	"github.com/hashicorp/go-hclog"
	"github.com/raito-io/cli/base"
)

var logger hclog.Logger

func init() {
	logger = base.Logger()
}

func iteratorToChannel[T any](ctx context.Context, f func() listing.Iterator[T]) <-chan ChannelItem[T] {
	outputChannel := make(chan ChannelItem[T])

	go func() {
		defer close(outputChannel)

		send := func(item ChannelItem[T]) bool {
			select {
			case <-ctx.Done():
				return false
			case outputChannel <- item:
				return true
			}
		}

		it := f()

		for it.HasNext(ctx) {
			item, err := it.Next(ctx)
			if err != nil {
				send(ChannelItem[T]{Err: err})
				return
			}

			if !send(ChannelItem[T]{I: &item}) {
                return
            }
		}
	}()

	return outputChannel
}
