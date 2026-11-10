package thread

import "testing"

// effectHash settles canonical arguments (ADR 0032, 2026-11-10): key
// order, space and escaping do not matter, numbers compare by value,
// and a string, a null and an absent field stay distinct.
func TestEffectHashCanonical(t *testing.T) {
	same := [][]string{
		{`{"a":1,"b":[1,2]}`, `{ "b" : [1, 2.0], "a" : 1e0 }`, `{"b":[10e-1,2],"a":1.00}`},
		{`{"x":0.5}`, `{"x":5e-1}`, `{"x":0.50}`, `{"x":50E-2}`},
		{`{"n":-0}`, `{"n":0}`, `{"n":0.0}`},
		{`{"s":"A<"}`, `{"s":"A<"}`},
		{`{"big":12345678901234567890123}`, `{"big":1.2345678901234567890123e22}`},
		{``, `null`, ` `},
	}
	for _, group := range same {
		want := effectHash([]byte(group[0]))
		for _, in := range group[1:] {
			if got := effectHash([]byte(in)); got != want {
				t.Errorf("effectHash(%s) != effectHash(%s)", in, group[0])
			}
		}
	}
	distinct := []string{`{"a":1}`, `{"a":"1"}`, `{"a":null}`, `{}`, `{"a":1.5}`, `{"a":12345678901234567891}`, `{"a":12345678901234567890}`, `{"a":1e999999}`, `{"a":10e999998}`, `not json`}
	seen := map[string]string{}
	for _, in := range distinct {
		h := effectHash([]byte(in))
		if prev, ok := seen[h]; ok {
			t.Errorf("effectHash(%s) == effectHash(%s)", in, prev)
		}
		seen[h] = in
	}
}
