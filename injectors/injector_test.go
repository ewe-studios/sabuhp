package injectors

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInjector_AddValue(t *testing.T) {
	var ij = NewInjector()
	require.NoError(t, ij.AddValue("alex", "user_static_name"))

	var value, valueErr = ij.Resolve("user_static_name", nil)
	require.NoError(t, valueErr)
	require.Equal(t, "alex", value)
}

type Ribbon struct {
	Name string
}

func TestInjector_AddCreator(t *testing.T) {
	var ij = NewInjector()
	require.NoError(t, ij.AddCreator(func(i *Injector, v interface{}) (interface{}, error) {
		return &Ribbon{}, nil
	}, "ribbon"))

	var value, valueErr = ij.Resolve("ribbon", nil)
	require.NoError(t, valueErr)

	var _, isRibbon = value.(*Ribbon)
	require.True(t, isRibbon)
}

func TestInjector_AddType(t *testing.T) {
	var ij = NewInjector()

	var item = (*Ribbon)(nil)
	require.NoError(t, ij.AddTypeValue(item, "ribbon"))

	var value, valueErr = ij.Resolve("ribbon", nil)
	require.NoError(t, valueErr)

	var _, isRibbon = value.(*Ribbon)
	require.True(t, isRibbon)
}

func TestInjector_AddType2(t *testing.T) {
	var ij = NewInjector()

	var item = &Ribbon{Name: "wale"}
	require.NoError(t, ij.AddTypeValue(item, "ribbon"))

	var value, valueErr = ij.ResolveType((*Ribbon)(nil), nil)
	require.NoError(t, valueErr)

	var item2, isRibbon = value.(*Ribbon)
	require.True(t, isRibbon)
	require.False(t, item2 == item)
}

type Ribbon2 struct{}

func TestInjector_AddType3(t *testing.T) {
	var ij = NewInjector()

	var item = &Ribbon2{}
	require.NoError(t, ij.AddTypeValue(item, "ribbon"))

	var value, valueErr = ij.ResolveType((*Ribbon2)(nil), nil)
	require.NoError(t, valueErr)

	var item2, isRibbon = value.(*Ribbon2)
	require.True(t, isRibbon)
	require.True(t, item2 == item)
}
