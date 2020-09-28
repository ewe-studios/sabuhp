package sabuhp

import (
	"strings"
)

type request struct {
	path   string
	found  bool
	params map[string]string
}

var tests = []struct {
	key       string
	routeName string
	requests  []request
}{
	{"/first", "first_data", []request{ // 0
		{"/first", true, nil},
	}},
	{"/first/one", "first/one_data", []request{ // 1
		{"/first/one", true, nil},
	}},
	{"/first/one/two", "first/one/two_data", []request{ // 2
		{"/first/one/two", true, nil},
	}},
	{"/firstt", "firstt_data", []request{ // 3
		{"/firstt", true, nil},
	}},
	{"/second", "second_data", []request{ // 4
		{"/second", true, nil},
	}},
	{"/second/one", "second/one_data", []request{ // 5
		{"/second/one", true, nil},
	}},
	{"/second/one/two", "second/one/two_data", []request{ // 6
		{"/second/one/two", true, nil},
	}},
	{"/second/one/two/three", "second/one/two/three_data", []request{ // 7
		{"/second/one/two/three", true, nil},
	}},
	// named parameters.
	{"/first/one/with/:param1/:param2/:param3/static", "first/one/with/static/_data_otherparams_with_static_end", []request{ // 8
		{"/first/one/with/myparam1/myparam2/myparam3/static", true, map[string]string{
			"param1": "myparam1",
			"param2": "myparam2",
			"param3": "myparam3",
		}},
	}},
	{"/first/one/with/:param1/:param2/:param3", "first/one/with/with_data_threeparams", []request{ // 9
		{"/first/one/with/myparam1/myparam2/myparam3", true, map[string]string{
			"param1": "myparam1",
			"param2": "myparam2",
			"param3": "myparam3",
		}},
	}},
	{"/first/one/with/:param/static/:otherparam", "first/one/with/static/_data_otherparam", []request{ // 10
		{"/first/one/with/myparam1/static/myotherparam", true, map[string]string{
			"param":      "myparam1",
			"otherparam": "myotherparam",
		}},
	}},
	{"/first/one/with/:param", "first/one/with_data_param", []request{ // 11
		{"/first/one/with/singleparam", true, map[string]string{
			"param": "singleparam",
		}},
	}},
	// wildcard parameters.
	{"/second/wild/*mywildcardparam", "second/wildcard_1", []request{ // 12
		{"/second/wild/everything/else/can/go/here", true, map[string]string{
			"mywildcardparam": "everything/else/can/go/here",
		}},
		{"/second/wild/static/otherstatic/random", true, map[string]string{
			"mywildcardparam": "static/otherstatic/random",
		}},
	}},
	// no wildcard but same prefix.
	{"/second/wild/static", "second/no_wild", []request{ // 13
		{"/second/wild/static", true, nil},
	}},
	// no wildcard, parameter instead with same prefix.
	{"/second/wild/:param", "second/no_wild_but_param", []request{ // 14
		{"/second/wild/myparam", true, map[string]string{
			"param": "myparam",
		}},
	}},
	// even that is possible:
	{"/second/wild/:param/static", "second/with_param_and_static_should_fail", []request{ // 14
		{"/second/wild/myparam/static", true, map[string]string{
			"param": "myparam",
		}},
	}},

	{"/second/wild/static/otherstatic", "second/no_wild_two_statics", []request{ // 14
		{"/second/wild/static/otherstatic", true, nil},
	}},
	// root wildcard.
	{"/*anything", "root_wildcard", []request{ // 15
		{"/something/or/anything/can/be/stored/here", true, map[string]string{
			"anything": "something/or/anything/can/be/stored/here",
		}},
		{"/justsomething", true, map[string]string{
			"anything": "justsomething",
		}},
		{"/a_not_found", true, map[string]string{
			"anything": "a_not_found",
		}},
	}},
}

func countParams(key string) int {
	return strings.Count(key, ParamStart) + strings.Count(key, WildcardParamStart)
}
