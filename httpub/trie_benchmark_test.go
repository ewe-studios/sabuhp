package httpub

import (
	"testing"
)

func initTree(tree *Trie) {
	for _, tt := range tests {
		tree.insert(tt.key, tt.routeName, nil, nil)
	}
}

// go test -run=XXX -v -bench=BenchmarkTrieInsert -count=3
func BenchmarkTrieInsert(b *testing.B) {
	tree := NewTrie()

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		initTree(tree)
	}
}
