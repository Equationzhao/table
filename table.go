package table

import (
	"fmt"
	"math"

	"github.com/cespare/xxhash"
)

// 元数据标记常量
const (
	metaEmpty = 0 // 空槽位
	metaFull  = 1 // 已占用槽位
	metaDel   = 2 // 删除的槽位（可复用）
)

type Entry struct {
	meta  byte
	key   any
	value any
}

type Table struct {
	entries []Entry

	capacity int

	size int

	// 负载因子阈值，超过此阈值就需要扩容
	loadFactor float64

	hashFn func(key any) uint64
}

func NewTable(capacity int) *Table {
	// 初始容量不能太小，避免过度冲突
	if capacity < 8 {
		capacity = 8
	}
	// 默认负载因子
	loadFactor := 0.75

	res := &Table{
		entries:    make([]Entry, capacity),
		capacity:   capacity,
		size:       0,
		loadFactor: loadFactor,
		hashFn: func(k any) uint64 {
			return xxhash.Sum64String(fmt.Sprintf("%v", k))
		},
	}
	return res
}

// getIndex 返回为键计算的初始槽位索引
func (st *Table) getIndex(key any) int {
	return int(st.hashFn(key) % uint64(st.capacity))
}

// findSlot 采用开放寻址（这里用线性探测的示例）
// slotIndex: 初始索引
// key: 用于查找冲突的目标键
// insertMode: 是否处于插入模式。插入模式下遇到删除标记也可复用。
func (st *Table) findSlot(slotIndex int, key any, insertMode bool) int {
	start := slotIndex
	for {
		meta := st.entries[slotIndex].meta & 0x03 // 只取低两位

		// 情况 1：空槽位
		//  - 查找模式下，如果是空槽位则代表没找到，直接返回该索引
		//  - 插入模式下，空槽位可以直接插入
		if meta == metaEmpty {
			return slotIndex
		}

		// 情况 2：删除标记
		//  - 查找模式下，继续探测
		//  - 插入模式下，可以复用此槽位
		if meta == metaDel && insertMode {
			return slotIndex
		}

		// 情况 3：已占用槽位，需要比较是否是要找的目标键
		if meta == metaFull {
			if st.entries[slotIndex].key == key {
				// 找到了匹配键，直接返回
				return slotIndex
			}
		}

		// 线性探测：继续往下一个槽位找
		slotIndex = (slotIndex + 1) % st.capacity

		// 如果绕了一圈还没找到，说明表满了或冲突严重（理应在插入前扩容）
		if slotIndex == start {
			return -1 // 插入失败，或没找到
		}
	}
}

// Insert 插入或更新键值
func (st *Table) Insert(key any, value any) {
	// 当 size 超过 loadFactor * capacity 时，需要扩容
	if float64(st.size+1) > float64(st.capacity)*st.loadFactor {
		st.resize(st.capacity * 2)
	}

	index := st.getIndex(key)

	// 找槽位，插入模式
	slot := st.findSlot(index, key, true)

	meta := st.entries[slot].meta & 0x03

	// 如果当前槽位是空或删除，则是新插入
	if meta == metaEmpty || meta == metaDel {
		st.size++
		st.entries[slot].meta = metaFull
		st.entries[slot].key = key
		st.entries[slot].value = value
	} else {
		// 如果是已占用，则说明 key 相同，更新值
		st.entries[slot].value = value
	}
}

// InsertBatch 批量插入键值，避免多次触发扩容
func (st *Table) InsertBatch(keys []any, values []any) error {
	if len(keys) != len(values) {
		return fmt.Errorf("length not match")
	}

	totalIncoming := len(keys)
	// 先一次性检查并确保容量足够
	// 可能一次扩容不足，循环直到足够
	for float64(st.size+totalIncoming) > float64(st.capacity)*st.loadFactor {
		st.resize(st.capacity * 2)
	}

	// 再进行逐个插入
	for i, k := range keys {
		st.Insert(k, values[i])
	}
	return nil
}

// Find 查找键对应的值，找不到返回 nil
func (st *Table) Find(key any) any {
	index := st.getIndex(key)

	slot := st.findSlot(index, key, false)
	if slot < 0 {
		return nil
	}

	meta := st.entries[slot].meta & 0x03
	// 如果是空槽位或删除槽位，说明找不到对应键
	if meta == metaEmpty || meta == metaDel {
		return nil
	}

	return st.entries[slot].value
}

// FindBatch 批量查找键
func (st *Table) FindBatch(keys []any) []any {
	results := make([]any, len(keys))
	for i, key := range keys {
		results[i] = st.Find(key)
	}
	return results
}

// Delete 删除 key，成功返回 true，失败返回 false
func (st *Table) Delete(key any) bool {
	index := st.getIndex(key)

	slot := st.findSlot(index, key, false)
	if slot < 0 {
		return false
	}

	meta := st.entries[slot].meta & 0x03
	if meta == metaFull && st.entries[slot].key == key {
		// 逻辑删除，只标记为删除
		st.entries[slot].meta = metaDel
		st.entries[slot].key = nil
		st.entries[slot].value = nil
		st.size--
		return true
	}
	return false
}

// resize 扩容哈希表
func (st *Table) resize(newCapacity int) {
	newTable := NewTable(newCapacity)
	for i := 0; i < st.capacity; i++ {
		meta := st.entries[i].meta & 0x03
		if meta == metaFull {
			key := st.entries[i].key
			value := st.entries[i].value
			newTable.Insert(key, value)
		}
	}
	// 用新的 table 替换旧 table
	*st = *newTable
}

// Expand 扩容哈希表到指定的新容量
func (st *Table) Expand(newCapacity int) {
	if newCapacity <= st.capacity {
		return
	}

	st.resize(newCapacity)
}

// Shrink 缩小哈希表
func (st *Table) Shrink() {
	const minCap = 8

	if st.capacity <= minCap {
		return
	}

	idealCap := int(math.Ceil(float64(st.size) / st.loadFactor))
	if idealCap < minCap {
		idealCap = minCap
	}

	if idealCap < st.capacity {
		st.resize(idealCap)
	}
}

// Size 返回当前存储键值对的数量
func (st *Table) Size() int {
	return st.size
}

// Capacity 返回当前哈希表容量
func (st *Table) Capacity() int {
	return st.capacity
}
