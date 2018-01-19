// kvcache.go
package kvcache

import (
	"errors"
	"reflect"
	"sync/atomic"
)

type ObjectCacher interface {
	Serialize() (obj string, err error)
	Deserialize(obj string) (err error)
}

type Bucket struct {
	keys []string
	data map[string]*atomic.Value
}

func NewBucket() *Bucket {
	return &Bucket{
		keys: make([]string, 0),
		data: make(map[string]*atomic.Value, 0),
	}
}

func (b *Bucket) Keys() []string {
	return b.keys
}

func (b *Bucket) Set(key string, objCacher ObjectCacher) error {
	str, err := objCacher.Serialize()
	if err != nil {
		return err
	}
	_, ok := b.data[key]
	if !ok {
		b.keys = append(b.keys, key)
		b.data[key] = &atomic.Value{}
	}
	b.data[key].Store(str)

	return nil
}

func (b *Bucket) Get(key string, objCacher ObjectCacher) error {
	v, ok := b.data[key]
	if !ok {
		return errors.New("invalid object key")
	}
	err := objCacher.Deserialize(v.Load().(string))
	if err != nil {
		return err
	}
	return nil
}

func (b *Bucket) SetObject(key string, obj interface{}) error {
	_, ok := b.data[key]
	if !ok {
		b.keys = append(b.keys, key)
		b.data[key] = &atomic.Value{}
	}
	if b.data[key].Load() != nil {
		if reflect.TypeOf(b.data[key].Load()) != reflect.TypeOf(obj) {
			return errors.New("obj must be type " + reflect.TypeOf(b.data[key].Load()).Name())
		}
	}
	b.data[key].Store(obj)
	return nil
}

func (b *Bucket) GetObject(key string) (obj interface{}, err error) {
	v, ok := b.data[key]
	if !ok {
		return nil, errors.New("invalid object key")
	}
	return v.Load(), nil
}

func (b *Bucket) Delete(keys ...string) int {
	effect := 0
	for _, v := range keys {
		_, ok := b.data[v]
		if ok {
			delete(b.data, v)
			effect++
			for ii, vv := range b.keys {
				if vv == v {
					b.keys = append(b.keys[:ii], b.keys[ii+1:]...)
				}
			}
		}
	}
	return effect
}
