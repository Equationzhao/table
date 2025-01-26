package table

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
)

func TestTableBasic(t *testing.T) {
	table := NewTable(8)

	// 测试插入
	table.Insert("apple", 1)
	table.Insert("banana", 2)
	table.Insert("cherry", 3)

	if table.Size() != 3 {
		t.Errorf("期望 size=3, 实际 size=%d", table.Size())
	}

	// 测试查找
	if v := table.Find("apple"); v != 1 {
		t.Errorf("查找 apple 失败,返回 %v", v)
	}
	if v := table.Find("banana"); v != 2 {
		t.Errorf("查找 banana 失败,返回 %v", v)
	}
	if v := table.Find("not-exist"); v != nil {
		t.Errorf("查找不存在的键,期望 nil,返回 %v", v)
	}

	// 测试删除
	if ok := table.Delete("banana"); !ok {
		t.Errorf("删除 banana 失败")
	}
	if v := table.Find("banana"); v != nil {
		t.Errorf("期望 banana 被删除后查找为 nil, 结果=%v", v)
	}

	// 测试更新
	table.Insert("apple", 999)
	if v := table.Find("apple"); v != 999 {
		t.Errorf("更新 apple 值失败,返回 %v", v)
	}
}

func TestTableResize(t *testing.T) {
	table := NewTable(4)

	// 反复插入,使其触发扩容
	for i := 0; i < 100; i++ {
		table.Insert(fmt.Sprintf("key%d", i), i)
	}

	// 测试插入后,表容量应大于等于最初容量
	if table.Capacity() <= 4 {
		t.Errorf("扩容失败,当前容量=%d", table.Capacity())
	}

	// 测试所有插入的数据是否能查到
	for i := 0; i < 100; i++ {
		v := table.Find(fmt.Sprintf("key%d", i))
		if v != i {
			t.Errorf("查找 key%d 失败,返回 %v", i, v)
		}
	}
}

// 场景 1：空表操作
// - 尝试在空表中查找和删除一个不存在的键,验证返回值是否正确
func TestEmptyTableOperations(t *testing.T) {
	table := NewTable(8)

	// 在空表中查找
	val := table.Find("non-exist-key")
	if val != nil {
		t.Errorf("在空表中查找不存在的键,期望 nil, 实际返回=%v", val)
	}

	// 在空表中删除
	ok := table.Delete("non-exist-key")
	if ok {
		t.Errorf("在空表中删除不存在的键,期望 false, 实际返回 true")
	}
}

// 场景 2：重复键插入
// - 插入相同的键多次,确保值被正确更新,且表的 size 不会重复增加
func TestDuplicateKeys(t *testing.T) {
	table := NewTable(8)

	table.Insert("dup-key", 1)
	if table.Size() != 1 {
		t.Errorf("第一次插入后,期望 size=1, 实际=%d", table.Size())
	}
	table.Insert("dup-key", 2)
	if table.Size() != 1 {
		t.Errorf("更新已存在的键后,期望 size 不变 (依旧=1), 实际=%d", table.Size())
	}

	val := table.Find("dup-key")
	if val != 2 {
		t.Errorf("重复插入后,期望值被更新为 2, 实际=%v", val)
	}
}

// 场景 3：负载因子边界
// - 插入若干键值对使表接近负载因子阈值,验证插入后的扩容行为
func TestLoadFactorBoundary(t *testing.T) {
	// capacity=8, loadFactor=0.75 => 最大安全存储元素 ~6 个左右
	table := NewTable(8)

	// 插入 6 个元素
	for i := 0; i < 6; i++ {
		table.Insert(fmt.Sprintf("key-%d", i), i)
	}

	// 此时表应尚未扩容
	if table.Capacity() != 8 {
		t.Errorf("在负载因子阈值附近应尚未扩容,实际容量=%d", table.Capacity())
	}

	// 再插入第 7 个,看是否扩容
	table.Insert("key-6", 6)
	if table.Capacity() == 8 {
		t.Errorf("在插入第 7 个元素后,期望扩容,但容量仍是 8")
	}

	// 检查所有已插入的数据是否正常
	for i := 0; i < 7; i++ {
		val := table.Find(fmt.Sprintf("key-%d", i))
		if val != i {
			t.Errorf("插入后查找失败, key-%d, 返回=%v", i, val)
		}
	}
}

// 场景 4：冲突处理
//   - 使用一个恒定哈希值函数,模拟所有键映射到同一个槽位,验证冲突解决的正确性
//     这里我们改写局部的 getIndex 函数来模拟冲突
func TestHighConflictScenario(t *testing.T) {
	table := NewTable(8)

	patch := gomonkey.ApplyFunc(table.getIndex, func(key any) int {
		return 0
	})
	patch.Reset()

	// 插入多个键,全部 hash 到同一个槽位
	for i := 0; i < 10; i++ {
		table.Insert(fmt.Sprintf("conflict-%d", i), i)
	}

	// 验证所有数据是否可正常查找到
	for i := 0; i < 10; i++ {
		val := table.Find(fmt.Sprintf("conflict-%d", i))
		if val != i {
			t.Errorf("冲突场景查找失败, conflict-%d, 返回=%v", i, val)
		}
	}
}

// 场景 5：大规模插入和扩容
// - 插入 10 万个随机键值对,验证性能和数据正确性
func TestMassiveInsertAndResize(t *testing.T) {
	table := NewTable(16)
	count := 100000

	keys := make([]string, 0, count)
	values := make([]int, 0, count)

	// 生成随机键值对
	for i := 0; i < count; i++ {
		k := fmt.Sprintf("%d-%d", rand.Int(), i)
		keys = append(keys, k)
		values = append(values, i)
	}

	// 插入
	start := time.Now()
	for i := 0; i < count; i++ {
		table.Insert(keys[i], values[i])
	}
	elapsedInsert := time.Since(start)

	if table.Size() != count {
		t.Errorf("大规模插入后,期望 size=%d, 实际=%d", count, table.Size())
	}

	// 查找
	start = time.Now()
	for i := 0; i < count; i++ {
		val := table.Find(keys[i])
		if val != values[i] {
			t.Errorf("大规模插入后查找失败,key=%s, 期望=%d, 实际=%v", keys[i], values[i], val)
		}
	}
	elapsedFind := time.Since(start)

	t.Logf("插入 %d 个元素耗时: %v", count, elapsedInsert)
	t.Logf("查找 %d 个元素耗时: %v", count, elapsedFind)
}

// 场景 6：删除与复用
// - 插入多个键值对,删除其中一部分,再插入新键值对,验证删除槽位是否被正确复用
func TestDeleteAndReuse(t *testing.T) {
	table := NewTable(8)

	// 插入 5 个元素
	for i := 0; i < 5; i++ {
		table.Insert(fmt.Sprintf("key%d", i), i)
	}

	// 删除其中 2 个
	table.Delete("key1")
	table.Delete("key3")

	// 再插入新键值对,看看之前删除的槽是否被复用
	table.Insert("key-new1", 100)
	table.Insert("key-new2", 200)

	// 检查所有元素
	if table.Find("key1") != nil {
		t.Errorf("key1 已删除,期望为 nil, 但返回 %v", table.Find("key1"))
	}
	if table.Find("key3") != nil {
		t.Errorf("key3 已删除,期望为 nil, 但返回 %v", table.Find("key3"))
	}

	// 新插入元素是否正确
	v1 := table.Find("key-new1")
	v2 := table.Find("key-new2")
	if v1 != 100 || v2 != 200 {
		t.Errorf("新元素插入失败,返回 key-new1=%v, key-new2=%v", v1, v2)
	}
}

// 场景 7：探测链完整性
// - 构造特殊场景（插入、删除、再插入）验证探测链是否保持完整
func TestProbeChainIntegrity(t *testing.T) {
	table := NewTable(8)

	// 随机插入若干数据
	for i := 0; i < 10; i++ {
		table.Insert(fmt.Sprintf("pkey%d", i), i)
	}

	// 删除偶数索引键
	for i := 0; i < 10; i += 2 {
		table.Delete(fmt.Sprintf("pkey%d", i))
	}

	// 再插入更多数据,观察探测链
	for i := 10; i < 15; i++ {
		table.Insert(fmt.Sprintf("pkey%d", i), i*100)
	}

	// 逐一验证查找
	for i := 0; i < 10; i++ {
		val := table.Find(fmt.Sprintf("pkey%d", i))
		if i%2 == 0 {
			// 已删除的键
			if val != nil {
				t.Errorf("pkey%d 已删除,期望 nil,返回=%v", i, val)
			}
		} else {
			// 未删除的键
			if val != i {
				t.Errorf("pkey%d 未删除,期望=%d,返回=%v", i, i, val)
			}
		}
	}

	// 新插入的数据
	for i := 10; i < 15; i++ {
		val := table.Find(fmt.Sprintf("pkey%d", i))
		if val != i*100 {
			t.Errorf("pkey%d 期望=%d, 返回=%v", i, i*100, val)
		}
	}
}

// 场景 8：极端输入
// - 插入 nil、空字符串、非 ASCII 字符串、负数等特殊值
func TestExtremeInputs(t *testing.T) {
	table := NewTable(8)

	table.Insert(nil, "nil-value")
	table.Insert("", "empty-string")
	table.Insert("中文键", "中文值")
	table.Insert(-123, 456)
	table.Insert("special\nline", "with\nnewline")

	// 查找
	if val := table.Find(nil); val != "nil-value" {
		t.Errorf("nil 作为键查找失败,期望=nil-value, 实际=%v", val)
	}
	if val := table.Find(""); val != "empty-string" {
		t.Errorf("空字符串查找失败,期望=empty-string, 实际=%v", val)
	}
	if val := table.Find("中文键"); val != "中文值" {
		t.Errorf("非 ASCII 字符串查找失败,期望=中文值, 实际=%v", val)
	}
	if val := table.Find(-123); val != 456 {
		t.Errorf("负数查找失败,期望=456, 实际=%v", val)
	}
	if val := table.Find("special\nline"); val != "with\nnewline" {
		t.Errorf("含换行字符的键查找失败,期望=with\\nnewline, 实际=%v", val)
	}
}

// TestBatchInsertAndFind 测试批量插入与批量查找
func TestBatchInsertAndFind(t *testing.T) {
	table := NewTable(8)

	keys := []any{"apple", "banana", "cherry", "date"}
	vals := []any{1, 2, 3, 4}

	if err := table.InsertBatch(keys, vals); err != nil {
		t.Fatalf("InsertBatch 发生错误: %v", err)
	}

	// 确认 size
	if table.Size() != 4 {
		t.Errorf("批量插入后 size 期望=4, 实际=%d", table.Size())
	}

	// 批量查找
	result := table.FindBatch([]any{"apple", "banana", "cherry", "date", "not-exist"})
	// 应分别返回 [1, 2, 3, 4, nil]
	expected := []any{1, 2, 3, 4, nil}
	for i := 0; i < len(result); i++ {
		if result[i] != expected[i] {
			t.Errorf("批量查找错误, key=%v, 期望=%v, 实际=%v", i, expected[i], result[i])
		}
	}
}

// TestBatchInsertCapacity 测试批量插入时自动扩容
func TestBatchInsertCapacity(t *testing.T) {
	table := NewTable(8)

	// 构造一个超出负载因子的批量插入
	// 负载因子0.75, 初始容量8, 约 6 个后扩容
	keys := make([]any, 10)
	vals := make([]any, 10)
	for i := 0; i < 10; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
		vals[i] = i
	}

	if err := table.InsertBatch(keys, vals); err != nil {
		t.Fatalf("批量插入错误：%v", err)
	}

	if table.Size() != 10 {
		t.Errorf("批量插入后 size 期望=10, 实际=%d", table.Size())
	}

	// 再查看容量是否已扩容 (>=16)
	if table.Capacity() < 16 {
		t.Errorf("批量插入后容量应扩容到至少 16, 实际=%d", table.Capacity())
	}

	// 查验证
	for i := 0; i < 10; i++ {
		val := table.Find(fmt.Sprintf("key-%d", i))
		if val != i {
			t.Errorf("批量插入后查找失败, key=%s, 期望=%d, 实际=%v", fmt.Sprintf("key-%d", i), i, val)
		}
	}
}

// TestBatchInsertFindEdgeCases 一些边界条件测试
func TestBatchInsertFindEdgeCases(t *testing.T) {
	table := NewTable(4)

	// 1) 测试空切片
	if err := table.InsertBatch([]any{}, []any{}); err != nil {
		t.Errorf("插入空切片不应报错,但返回错误: %v", err)
	}

	r := table.FindBatch([]any{})
	if len(r) != 0 {
		t.Errorf("查找空切片返回长度应为 0, 实际=%d", len(r))
	}

	// 2) keys 与 values 长度不一致
	err := table.InsertBatch([]any{"k1"}, []any{"v1", "v2"})
	if err == nil {
		t.Errorf("keys 与 values 长度不一致时应报错,但未报错")
	}

	// 3) 大量插入
	keys, vals := []any{}, []any{}
	for i := 0; i < 20; i++ {
		keys = append(keys, fmt.Sprintf("kxxx%d", i))
		vals = append(vals, i)
	}
	// 批量插入
	err = table.InsertBatch(keys, vals)
	if err != nil {
		t.Errorf("批量插入出现错误: %v", err)
	}

	// 逐个校验
	results := table.FindBatch(keys)
	for i, v := range results {
		if v != i {
			t.Errorf("批量插入后查找错误, key=%s, 期望=%d, 实际=%v", keys[i], i, v)
		}
	}
}

func TestNilKey(t *testing.T) {
	table := NewTable(8)

	table.Insert(nil, "hello nil")
	val := table.Find(nil)
	if val != "hello nil" {
		t.Errorf("expected 'hello nil', got %v", val)
	}

	table.Insert(nil, "updated nil")
	val = table.Find(nil)
	if val != "updated nil" {
		t.Errorf("expected 'updated nil', got %v", val)
	}

	deleted := table.Delete(nil)
	if !deleted {
		t.Errorf("expected delete to succeed for nil key")
	}

	val = table.Find(nil)
	if val != nil {
		t.Errorf("expected nil after deletion, got %v", val)
	}
}

func TestConcurrentRead(t *testing.T) {
	table := NewTable(8)

	// 插入一些初始数据
	for i := 0; i < 100; i++ {
		table.Insert(fmt.Sprintf("key-%d", i), i)
	}

	var wg sync.WaitGroup
	readCount := 100
	results := make([]any, readCount)

	// 启动多个 goroutine 进行并发读取
	for i := 0; i < readCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			results[index] = table.Find(fmt.Sprintf("key-%d", index))
		}(i)
	}

	// 等待所有 goroutine 完成
	wg.Wait()

	// 验证结果
	for i := 0; i < readCount; i++ {
		if results[i] != i {
			t.Errorf("并发读取失败, key=key-%d, 期望=%d, 实际=%v", i, i, results[i])
		}
	}
}

// 测试 Expand 方法
func TestExpand(t *testing.T) {
	table := NewTable(8)

	// 插入一些数据
	for i := 0; i < 10; i++ {
		table.Insert(fmt.Sprintf("key-%d", i), i)
	}

	// 扩容到 16
	table.Expand(16)

	// 验证容量是否正确
	if table.Capacity() != 16 {
		t.Errorf("期望容量为 16, 实际为 %d", table.Capacity())
	}

	// 验证数据是否仍然存在
	for i := 0; i < 10; i++ {
		if val := table.Find(fmt.Sprintf("key-%d", i)); val != i {
			t.Errorf("期望找到 key-%d, 实际为 %v", i, val)
		}
	}

	table.Expand(10)

	if table.Capacity() != 16 {
		t.Errorf("期望容量为 16, 实际为 %d", table.Capacity())
	}
}

// 测试 Shrink 方法
func TestShrink(t *testing.T) {
	table := NewTable(8)
	table.Shrink()
	if table.Capacity() != 8 {
		t.Errorf("期望容量为 8, 实际为 %d", table.Capacity())
	}

	table.Expand(9)
	table.Shrink()

	for i := 0; i < 10; i++ {
		table.Insert(fmt.Sprintf("key-%d", i), i)
	}

	table.Expand(32)

	if table.Capacity() != 32 {
		t.Errorf("期望容量为 32, 实际为 %d", table.Capacity())
	}

	table.Shrink()

	if table.Capacity() >= 32 {
		t.Errorf("期望容量小于 32, 实际为 %d", table.Capacity())
	}

	for i := 0; i < 10; i++ {
		if val := table.Find(fmt.Sprintf("key-%d", i)); val != i {
			t.Errorf("期望找到 key-%d, 实际为 %v", i, val)
		}
	}
}
