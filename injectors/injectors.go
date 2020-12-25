package injectors

import (
	"sync"

	"github.com/influx6/npkg/nerror"
	"github.com/influx6/npkg/nreflect"
)

type Creator func(i *Injector, v interface{}) (interface{}, error)

type Injector struct {
	sl    sync.RWMutex
	store map[string]Creator
}

func NewInjector() *Injector {
	return &Injector{
		store: map[string]Creator{},
	}
}

// Resolve allows resolution of a type based on name or alias which is used to
// execute the appropriate creator to create giving type.
func (i *Injector) Resolve(nameOrAlias string, args interface{}) (interface{}, error) {
	var typeCreator, typeRegistered = i.loadUp(nameOrAlias)
	if !typeRegistered {
		return nil, nerror.New("type is not registered")
	}
	return typeCreator(i, args)
}

// ResolveType attempts resolving into a type based on the actual reflect type information
// from the provided type. It returns the result of the generator.
func (i *Injector) ResolveType(targetType interface{}, args interface{}) (interface{}, error) {
	var typeName = nreflect.NameOf(targetType)
	var typeCreator, typeRegistered = i.loadUp(typeName)
	if !typeRegistered {
		return nil, nerror.New("type is not registered")
	}
	return typeCreator(i, args)
}

// AddCreator adds a creator for a giving reflect type which will
// return a value from the function everytime it's called.
// This can return a new value or the same, the creator decides that.
func (i *Injector) AddCreator(creator Creator, alias string) error {
	i.storeUp(alias, creator)
	return nil
}

// AddValue registers a giving value as is based on the name provided.
// We treat the value as is and no operation is performed, whenever the
// name is resolved from the injector the value of v is always returned.
func (i *Injector) AddValue(v interface{}, name string) error {
	var alreadyExisted = i.has(name)
	if alreadyExisted {
		return nerror.New("%q value already has been registered", name)
	}

	var vCreator = func(_ *Injector, _ interface{}) (interface{}, error) {
		return v, nil
	}
	i.storeUp(name, vCreator)
	return nil
}

func (i *Injector) storeUp(name string, creator Creator) {
	i.sl.Lock()
	i.store[name] = creator
	i.sl.Unlock()
}

func (i *Injector) has(name string) bool {
	i.sl.RLock()
	defer i.sl.RUnlock()
	var _, ok = i.store[name]
	return ok
}

func (i *Injector) loadUp(name string) (Creator, bool) {
	i.sl.RLock()
	defer i.sl.RUnlock()
	var cv, ok = i.store[name]
	return cv, ok
}
