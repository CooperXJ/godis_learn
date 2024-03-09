package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDict(t *testing.T) {
	dict := DictCreate(DictType{EqualFunc: GStrEqual, HashFunc: GStrHash})
	entry := dict.RandomGet()
	assert.Nil(t, entry)

	k1 := CreateObject(GSTR, "k1")
	v1 := CreateObject(GSTR, "v1")
	e := dict.Add(k1, v1)
	assert.Nil(t, e)

	entry = dict.Find(k1)
	assert.Equal(t, k1, entry.Key)
	assert.Equal(t, v1, entry.Val)
	assert.Equal(t, 2, k1.refCount)
	assert.Equal(t, 2, v1.refCount)

	e = dict.Delete(k1)
	assert.Nil(t, e)
	entry = dict.Find(k1)
	assert.Nil(t, e)
	assert.Equal(t, 1, k1.refCount)
	assert.Equal(t, 1, v1.refCount)

	e = dict.Add(k1, v1)
	assert.Nil(t, e)
	v := dict.Get(k1)
	assert.Equal(t, v1, v)
	v2 := CreateObject(GSTR, "v2")
	dict.Set(k1, v2)
	v = dict.Get(k1)
	assert.Equal(t, v2, v)
	assert.Equal(t, 2, v2.refCount)
	assert.Equal(t, 1, v1.refCount)
}

func TestRehash(t *testing.T) {
	dict := DictCreate(DictType{
		HashFunc:  GStrHash,
		EqualFunc: GStrEqual,
	})

	entry := dict.RandomGet()
	assert.Nil(t, entry)

	value := int(InitSize * (ForceRatio + 1))
	for i := 0; i < value; i++ {
		key := CreateObject(GSTR, fmt.Sprintf("k%v", i))
		val := CreateObject(GSTR, fmt.Sprintf("v%v", i))
		e := dict.Add(key, val)
		assert.Nil(t, e)
	}
	assert.Equal(t, false, dict.isRehashing())
	key := CreateObject(GSTR, fmt.Sprintf("k%v", value))
	val := CreateObject(GSTR, fmt.Sprintf("v%v", value))
	e := dict.Add(key, val)
	assert.Nil(t, e)
	assert.Equal(t, true, dict.isRehashing())
	assert.Equal(t, int64(0), dict.rehashIdx)
	assert.Equal(t, int64(InitSize), dict.hts[0].size)
	assert.Equal(t, int64(InitSize*GrowRatio), dict.hts[1].size)
	for i := 0; i <= int(InitSize); i++ {
		dict.RandomGet()
	}
	assert.Equal(t, false, dict.isRehashing())
	assert.Equal(t, int64(InitSize*GrowRatio), dict.hts[0].size)
	assert.Nil(t, dict.hts[1])
	for i := 0; i <= value; i++ {
		key := CreateObject(GSTR, fmt.Sprintf("k%v", i))
		entry = dict.Find(key)
		assert.NotNil(t, entry)
		assert.Equal(t, fmt.Sprintf("v%v", i), entry.Val.StrVal())
	}
}
