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
