package main

import (
	"errors"
	"math"
	"math/rand"
)

const (
	InitSize    int64 = 8
	ForceRatio  int64 = 2
	GrowRatio   int64 = 2
	DefaultStep int   = 1
)

var (
	EpErr = errors.New("expand error")
	ExErr = errors.New("key exists error")
	NkErr = errors.New("key doesn't exist error")
)

type Entry struct {
	Key  *Gobj
	Val  *Gobj
	next *Entry
}

type htable struct {
	table []*Entry
	size  int64
	mask  int64
	used  int64
}

type DictType struct {
	HashFunc  func(key *Gobj) int64
	EqualFunc func(key1, key2 *Gobj) bool
}

type Dict struct {
	DictType
	hts       [2]*htable
	rehashIdx int64
	// iterators
}

func DictCreate(dictType DictType) *Dict {
	var dict Dict
	dict.DictType = dictType
	dict.rehashIdx = -1
	return &dict
}

func (dict *Dict) isRehashing() bool {
	return dict.rehashIdx != -1
}

// 每次只变化一小次  这里的step可以自己进行设置
func (dict *Dict) rehashStep() {
	dict.rehash(DefaultStep)
}

func (dict *Dict) rehash(step int) {
	for step > 0 {
		// 如果第一张表为空（迁移完了），说明第一张的表的内容已经完全复制到第二张表了，则直接使用第一张表，删除第二张表
		if dict.hts[0].used == 0 {
			dict.hts[0] = dict.hts[1]
			dict.hts[1] = nil
			dict.rehashIdx = -1
			return
		}

		// find an nonull slot  找到有值的slot
		for dict.hts[0].table[dict.rehashIdx] == nil {
			dict.rehashIdx += 1
		}

		entry := dict.hts[0].table[dict.rehashIdx]
		for entry != nil {
			ne := entry.next
			idx := dict.HashFunc(entry.Key) & dict.hts[1].mask
			// 采用头插法
			entry.next = dict.hts[1].table[idx]
			dict.hts[1].table[idx] = entry
			dict.hts[0].used--
			dict.hts[1].used++
			entry = ne
		}
		dict.hts[0].table[dict.rehashIdx] = nil
		dict.rehashIdx++
		step--
	}
}

// 获取下一阶段表的大小
func nextPower(size int64) int64 {
	for i := InitSize; i < math.MaxInt64; i *= 2 {
		if i >= size {
			return i
		}
	}
	return -1
}

func (dict *Dict) expand(size int64) error {
	sz := nextPower(size)
	if dict.isRehashing() || (dict.hts[0] != nil && dict.hts[0].size >= sz) {
		return EpErr
	}

	var ht htable
	ht.size = sz
	ht.mask = sz - 1
	ht.used = 0
	ht.table = make([]*Entry, sz)

	if dict.hts[0] == nil {
		dict.hts[0] = &ht
		return nil
	}

	dict.hts[1] = &ht
	dict.rehashIdx = 0
	return nil
}

func (dict *Dict) expandIfNeed() error {
	if dict.isRehashing() {
		return nil
	}

	if dict.hts[0] == nil {
		return dict.expand(InitSize)
	}

	if (dict.hts[0].used > dict.hts[0].size) && (dict.hts[0].used/dict.hts[0].size > ForceRatio) {
		return dict.expand(dict.hts[0].size * GrowRatio)
	}

	return nil
}

// return the index of a free slot, return -1 if the key exists or err.
func (dict *Dict) keyIndex(key *Gobj) int64 {
	err := dict.expandIfNeed()
	if err != nil {
		return -1
	}

	h := dict.HashFunc(key)
	var idx int64

	for i := 0; i <= 1; i++ {
		idx = h & dict.hts[i].mask
		e := dict.hts[i].table[idx]
		for e != nil {
			if dict.EqualFunc(e.Key, key) {
				return -1
			}
			e = e.next
		}

		if !dict.isRehashing() {
			break
		}
	}
	return idx
}

func (dict *Dict) AddRaw(key *Gobj) *Entry {
	if dict.isRehashing() {
		dict.rehashStep()
	}

	idx := dict.keyIndex(key)
	if idx == -1 {
		return nil
	}

	var ht *htable
	if dict.isRehashing() {
		ht = dict.hts[1]
	} else {
		ht = dict.hts[0]
	}

	var e Entry
	e.Key = key
	key.IncrRefCount()
	e.next = ht.table[idx]
	ht.table[idx] = &e
	ht.used++
	return &e
}

// Add add a new key-val pair, return err if key exists
func (dict *Dict) Add(key, val *Gobj) error {
	entry := dict.AddRaw(key)
	if entry == nil {
		return ExErr
	}

	entry.Val = val
	val.IncrRefCount()
	return nil
}

func (dict *Dict) Find(key *Gobj) *Entry {
	if dict.hts[0] == nil {
		return nil
	}

	if dict.isRehashing() {
		dict.rehashStep()
	}

	h := dict.HashFunc(key)
	for i := 0; i <= 1; i++ {
		idx := h & dict.hts[i].mask
		e := dict.hts[i].table[idx]
		for e != nil {
			if dict.EqualFunc(e.Key, key) {
				return e
			}
			e = e.next
		}
		if !dict.isRehashing() {
			break
		}
	}
	return nil
}

func (dict *Dict) Set(key, val *Gobj) {
	if err := dict.Add(key, val); err == nil {
		return
	}

	entry := dict.Find(key)
	// 该key对应的旧val的引用需要-1
	entry.Val.DecrRefCount()
	entry.Val = val
	val.IncrRefCount()
}

func freeEntry(e *Entry) {
	e.Key.DecrRefCount()
	e.Val.DecrRefCount()
}

func (dict *Dict) Delete(key *Gobj) error {
	if dict.hts[0] == nil {
		return NkErr
	}

	if dict.isRehashing() {
		dict.rehashStep()
	}

	h := dict.HashFunc(key)
	for i := 0; i <= 1; i++ {
		idx := h & dict.hts[i].mask
		e := dict.hts[i].table[idx]
		var prev *Entry
		for e != nil {
			if dict.EqualFunc(e.Key, key) {
				if prev == nil {
					dict.hts[i].table[idx] = e.next
				} else {
					prev.next = e.next
				}

				freeEntry(e)
				return nil
			}

			prev = e
			e = e.next
		}

		if !dict.isRehashing() {
			break
		}
	}

	return NkErr
}

func (dict *Dict) Get(key *Gobj) *Gobj {
	entry := dict.Find(key)
	if entry == nil {
		return nil
	}

	return entry.Val
}

func (dict *Dict) RandomGet() *Entry {
	if dict.hts[0] == nil {
		return nil
	}

	t := 0
	if dict.isRehashing() {
		dict.rehashStep()
		if dict.hts[1] != nil && dict.hts[1].used > dict.hts[0].used {
			// simplify the logic, random get in the bigger table
			t = 1
		}
	}

	idx := rand.Int63n(dict.hts[t].size)
	cnt := 0
	for dict.hts[t].table[idx] == nil && cnt < 1000 {
		idx = rand.Int63n(dict.hts[t].size)
		cnt++
	}

	//如果随机1000次之后还是空，那么就返回空
	if dict.hts[t].table[idx] == nil {
		return nil
	}

	var listLen int64
	p := dict.hts[t].table[idx]
	for p != nil {
		listLen++
		p = p.next
	}

	listIdx := rand.Int63n(listLen)
	p = dict.hts[t].table[idx]
	for i := int64(0); i < listIdx; i++ {
		p = p.next
	}

	return p
}
