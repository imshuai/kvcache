// Package kvcache provide a atomic cache surport
package kvcache

import (
	"errors"
	"reflect"
	"sync/atomic"
)

//ObjectCacher defined a interface to this cache engine
type ObjectCacher interface {
	Serialize() (obj string, err error)
	Deserialize(obj string) (err error)
}

//Bucket defined a container to this cache engine
type Bucket struct {
	keys []string
	data map[string]*atomic.Value
}

//NewBucket defined a function to create a new container of this cache engine
func NewBucket() *Bucket {
	return &Bucket{
		keys: make([]string, 0),
		data: make(map[string]*atomic.Value, 0),
	}
}

//Keys return array of all keys in the Bucket
func (b *Bucket) Keys() []string {
	return b.keys
}

//Set storage a object to Bucket
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

//Get use key to find object in Bucket
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

//SetObject storage a object direct to Bucket
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

//GetObject use key to find object in Bucket and return it without deserialize
func (b *Bucket) GetObject(key string) (obj interface{}, err error) {
	v, ok := b.data[key]
	if !ok {
		return nil, errors.New("invalid object key")
	}
	return v.Load(), nil
}

//Delete delete objects in array keys
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
