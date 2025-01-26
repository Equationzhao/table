package table

import (
	"fmt"
	"math/rand"
	"testing"
)

func BenchmarkInsert(b *testing.B) {
	b.ResetTimer()
	table := NewTable(16)
	for i := 0; i < b.N; i++ {
		table.Insert(fmt.Sprintf("key-%d", i), i)
	}
	b.StopTimer()
}

func BenchmarkBatchInsert(b *testing.B) {
	table := NewTable(16)

	keys := make([]any, 0, 1000)
	vals := make([]any, 0, 1000)

	for i := 0; i < 1000; i++ {
		keys = append(keys, fmt.Sprintf("key-%d", i))
		vals = append(vals, i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := table.InsertBatch(keys, vals); err != nil {
			b.Fatalf("BatchInsert 发生错误: %v", err)
		}
	}
}

func BenchmarkInsertVsBatchInsert(b *testing.B) {
	cap := 1000000
	keys := make([]any, 0, cap)
	vals := make([]any, 0, cap)

	for i := 0; i < cap; i++ {
		keys = append(keys, fmt.Sprintf("key-%d", rand.Int31()))
		vals = append(vals, i)
	}

	b.Run("Insert", func(b *testing.B) {
		table := NewTable(cap)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			for j := range cap {
				table.Insert(keys[j], vals[j])
			}
		}
	})

	b.Run("BatchInsert", func(b *testing.B) {
		table := NewTable(cap)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			table.InsertBatch(keys, vals)
		}
	})

	b.Run("built in map", func(b *testing.B) {
		table := make(map[any]any, cap)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			for j := range cap {
				table[keys[j]] = vals[j]
			}
		}
	})
}

func BenchmarkExpand(b *testing.B) {
	cap := 1000000
	table := NewTable(cap)

	keys := make([]any, 0, cap)
	vals := make([]any, 0, cap)
	for i := 0; i < cap; i++ {
		keys = append(keys, fmt.Sprintf("key-%d", i))
		vals = append(vals, i)
	}

	for i := 0; i < cap; i++ {
		table.Insert(keys[i], vals[i])
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		table.Expand(cap * 2)
	}
}
