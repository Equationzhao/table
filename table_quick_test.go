package table

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"testing/quick"
)

type randomOp struct {
	OpType string
	Key    string
	Value  int

	Keys   []any
	Values []any

	Expend2 int
}

func operationsTest(ops []randomOp) bool {
	table := NewTable(8)
	for _, op := range ops {
		switch op.OpType {
		case "Insert":
			table.Insert(op.Key, op.Value)
		case "Find":
			_ = table.Find(op.Key)
		case "Delete":
			table.Delete(op.Key)
		case "BatchInsert":
			table.InsertBatch(op.Keys, op.Values)
		case "BatchFind":
			_ = table.FindBatch(op.Keys)
		case "Shrink":
			table.Shrink()
		case "Expend":
			table.Expand(op.Expend2)
		}
	}
	return true
}

func generateRandomOps() []randomOp {
	numOps := rand.IntN(200) + 50
	ops := make([]randomOp, numOps)

	for i := 0; i < numOps; i++ {
		single := func() {
			ops[i].Key = fmt.Sprintf("key-%d", rand.IntN(100))
			ops[i].Value = rand.Int()
		}
		multi := func() {
			t := rand.UintN(3)
			ops[i].Keys = make([]any, 0, t)
			ops[i].Values = make([]any, 0, t)
			for j := range t {
				ops[i].Keys = append(ops[i].Keys, fmt.Sprintf("key-%d-%d", j, rand.IntN(100)))
				ops[i].Values = append(ops[i].Values, rand.Int())
			}
		}

		switch rand.IntN(5) {
		case 0:
			ops[i].OpType = "Insert"
			single()
		case 1:
			ops[i].OpType = "Find"
			single()
		case 2:
			ops[i].OpType = "Delete"
			single()
		case 3:
			ops[i].OpType = "BatchInsert"
			multi()
		case 4:
			ops[i].OpType = "BatchFind"
			multi()
		case 5:
			ops[i].OpType = "Shrink"
		case 6:
			ops[i].OpType = "Expend"
			ops[i].Expend2 = int(rand.Uint32N(10000))
		}
	}
	return ops
}

func TestQuickCheckOperations(t *testing.T) {
	cfg := &quick.Config{
		MaxCount: 100000,
	}

	if err := quick.Check(func() bool {
		ops := generateRandomOps()
		return operationsTest(ops)
	}, cfg); err != nil {
		t.Error("Random sequence test failed:", err)
	}
}
