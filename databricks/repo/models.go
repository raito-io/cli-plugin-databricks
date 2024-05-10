package repo

import (
	"context"

	"github.com/raito-io/golang-set/set"
)

type ChannelItem[T any] struct {
	I   *T
	Err error
}

func (c ChannelItem[T]) Interface() interface{} {
	if c.Err != nil {
		return c.Err
	} else if c.I != nil {
		return *c.I
	} else {
		return nil
	}
}

func (c ChannelItem[T]) Error() error {
	return c.Err
}

func (c ChannelItem[T]) Item() T {
	return *c.I
}

func (c ChannelItem[T]) HasError() bool {
	return c.Err != nil
}

func (c ChannelItem[T]) HasItem() bool {
	return c.I != nil
}

func ArrayToChannel[T any](a []T) <-chan ChannelItem[T] {
	outputChannel := make(chan ChannelItem[T])

	go func() {
		defer close(outputChannel)

		for i := range a {
			outputChannel <- ChannelItem[T]{I: &a[i]}
		}
	}()

	return outputChannel
}

func ChannelToSet[T any, O comparable](channel func(ctx context.Context) <-chan ChannelItem[T], f func(T) O) (set.Set[O], error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	outputSet := set.NewSet[O]()

	ch := channel(ctx)
	for item := range ch {
		if item.HasError() {
			return nil, item.Error()
		}

		outputSet.Add(f(*item.I))
	}

	return outputSet, nil
}
