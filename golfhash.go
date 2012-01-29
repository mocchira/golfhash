// Copyright 2011 Yoshiyuki Kanno. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package golfhash

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"unsafe"
)

const (
	defPower     = 12
	defMaxProbes = 15
	defSubNum    = 1<<defPower + defMaxProbes
	hashLow      = 6
	hashOne      = 1 << hashLow
	hashMask     = hashOne - 1
	hashBits     = 8 * 8
	hashSubHash  = hashMask
	hashNil      = 0
)

type hashVal uint64

type HashType interface {
	Hash(src unsafe.Pointer) hashVal
	Equal(src, dst unsafe.Pointer) bool
	String(src unsafe.Pointer) string
}

type StrHashType struct{}

func (str StrHashType) Hash(src unsafe.Pointer) hashVal {
	var hash hashVal
	hash = 33054211828000289
	for _, b := range []byte(*((*string)(src))) {
		hash = (hash ^ hashVal(b)) * 23344194077549503
	}
	return hash
}
func (str StrHashType) Equal(src, dst unsafe.Pointer) bool {
	return *(*string)(src) == *(*string)(dst)
}
func (str StrHashType) String(src unsafe.Pointer) string {
	return *(*string)(src)
}

type IntHashType struct{}

func (i IntHashType) Hash(src unsafe.Pointer) hashVal {
	return hashVal(*(*int)(src)) << 32
}
func (i IntHashType) Equal(src, dst unsafe.Pointer) bool {
	return *(*int)(src) == *(*int)(dst)
}
func (i IntHashType) String(src unsafe.Pointer) string {
	return strconv.Itoa(*(*int)(src))
}

type hashEntry struct {
	hash hashVal
	key  unsafe.Pointer
	val  unsafe.Pointer
}

type hashSubTable struct {
	power     uint8
	maxProbes uint8
	entry     [defSubNum]*hashEntry
	buf       [defSubNum * 2]hashEntry
}
type Golfhash struct {
	Count int64
	power uint8
	table *hashSubTable
	htype HashType
}

var (
	IntegerHashType IntHashType
	StringHashType  StrHashType
)

func newHashSubTable(power uint8) *hashSubTable {
	st := &hashSubTable{
		power:     power,
		maxProbes: defMaxProbes,
	}
	return st
}
func Init(ht HashType) *Golfhash {
	return &Golfhash{
		Count: 0,
		power: defPower,
		table: newHashSubTable(defPower),
		htype: ht,
	}
}

func (golf *Golfhash) Remove(key unsafe.Pointer) bool {
	var di uint64
	var pv unsafe.Pointer
	pst := golf.table
	hash := golf.htype.Hash(key)
	hash &^= hashMask
	if hash < hashLow {
		hash += hashOne
	}
	for {
		if golf.lookup(key, hash, &pst, &di, &pv, 0) {
			if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&pst.entry[di])), unsafe.Pointer(pst.entry[di]), nil) {
				atomic.AddInt64(&golf.Count, -1)
				return true
			}
		} else {
			break
		}
	}
	return false
}

func (golf *Golfhash) Lookup(key unsafe.Pointer, val *unsafe.Pointer) bool {
	var di uint64
	pst := golf.table
	hash := golf.htype.Hash(key)
	hash &^= hashMask
	if hash < hashLow {
		hash += hashOne
	}
	return golf.lookup(key, hash, &pst, &di, val, 0)
}

func (golf *Golfhash) lookup(key unsafe.Pointer, hash hashVal, st **hashSubTable, i *uint64, val *unsafe.Pointer, used uint8) bool {
	shift := hashBits - (*st).power - used
	idxMask := 1<<(*st).power - 1
	idx := (uint64(hash) >> uint64(shift)) & uint64(idxMask)
	for *i = idx; *i < idx+uint64((*st).maxProbes); *i++ {
		e := (*st).entry[*i]
		if e == nil {
			continue
		}
		if e.hash&hashMask == hashSubHash {
			var ni uint64
			pstOld := *st
			used += (*st).power
			*st = (*hashSubTable)(e.val)
			if golf.lookup(key, hash, st, &ni, val, used) {
				*i = ni
				return true
			}
			*st = pstOld
		} else if (e.hash ^ hash) < hashSubHash {
			if golf.htype.Equal(key, e.key) {
				*val = e.val
				return true
			}
		}
	}
	return false
}

func (golf *Golfhash) insert2alloc(key unsafe.Pointer, val unsafe.Pointer, hash hashVal, st *hashSubTable, used uint8) bool {
	shift := hashBits - st.power - used
	idxMask := 1<<st.power - 1
	idx := (uint64(hash) >> uint64(shift)) & uint64(idxMask)
	for i := idx; i < idx+uint64(st.maxProbes); i++ {
		e := st.entry[i]
		if e != nil && (e.hash&hashMask) == hashSubHash {
			used += st.power
			if golf.insert2alloc(key, val, hash, (*hashSubTable)(e.val), used) {
				return true
			}
		} else {
			nst := newHashSubTable(golf.power)
			if e != nil {
				golf.insert2nil(e.key, e.val, e.hash, nst, used+st.power) // no possiblity to fail
			}
			var he *hashEntry
			if e == &st.buf[i] {
				he = &st.buf[i+defSubNum]
			} else {
				he = &st.buf[i]
			}
			he.hash = hashSubHash
			he.key = nil
			he.val = unsafe.Pointer(nst)
			if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&st.entry[i])), unsafe.Pointer(e), unsafe.Pointer(he)) {
				if golf.insert2nil(key, val, hash, nst, used+st.power) {
					return true
				}
			}
		}
	}
	return false
}

func (golf *Golfhash) insert2nil(key unsafe.Pointer, val unsafe.Pointer, hash hashVal, st *hashSubTable, used uint8) bool {
	shift := hashBits - st.power - used
	idxMask := 1<<st.power - 1
	idx := (uint64(hash) >> uint64(shift)) & uint64(idxMask)
	for i := idx; i < idx+uint64(st.maxProbes); i++ {
		e := st.entry[i]
		if e == nil {
			var he *hashEntry
			if e == &st.buf[i] {
				he = &st.buf[i+defSubNum]
			} else {
				he = &st.buf[i]
			}
			he.hash = hash
			he.key = key
			he.val = val
			if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&st.entry[i])), unsafe.Pointer(e), unsafe.Pointer(he)) {
				return true
			}
		} else if e.hash&hashMask == hashSubHash {
			used += st.power
			if golf.insert2nil(key, val, hash, (*hashSubTable)(e.val), used) {
				return true
			}
		}
	}
	return false
}

func (golf *Golfhash) Insert(key unsafe.Pointer, val unsafe.Pointer) bool {
	var di uint64
	var pv unsafe.Pointer
	hash := golf.htype.Hash(key)
	hash &^= hashMask
	if hash < hashLow {
		hash += hashOne
	}
	pst := golf.table
	for {
		if golf.lookup(key, hash, &pst, &di, &pv, 0) {
			var he *hashEntry
			if pst.entry[di] == &pst.buf[di] {
				he = &pst.buf[di+defSubNum]
			} else {
				he = &pst.buf[di]
			}
			he.hash = hash
			he.key = key
			he.val = val
			if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&pst.entry[di])), unsafe.Pointer(pst.entry[di]), unsafe.Pointer(he)) {
				atomic.AddInt64(&golf.Count, 1)
				return true
			}
		} else {
			break
		}

	}
	for {
		if golf.insert2nil(key, val, hash, golf.table, 0) {
			atomic.AddInt64(&golf.Count, 1)
			return true
		}
		if golf.insert2alloc(key, val, hash, golf.table, 0) {
			atomic.AddInt64(&golf.Count, 1)
			return true
		}
	}
	// usually not reached
	return false
}

type Visitor func(arg interface{}, level uint8, key, val unsafe.Pointer)

func (golf *Golfhash) visit(st *hashSubTable, used, level uint8, visitor Visitor, arg interface{}) {
	shift := hashBits - st.power - used
	idxMask := 1<<st.power - 1
	var i uint64
	for ; i < uint64(len(st.entry)); i++ {
		e := st.entry[i]
		if e == nil {
			continue
		}
		if e.hash&hashMask == hashSubHash {
			golf.visit((*hashSubTable)(e.val), used+st.power, level+1, visitor, arg)
			continue
		} else {
			visitor(arg, level, e.key, e.val)
		}
		idx := (uint64(e.hash) >> uint64(shift)) & uint64(idxMask)
		if i >= idx+uint64(st.maxProbes) || i < idx {
			panic(fmt.Sprintf("inconsisntet index: %d, used: %d, level: %d", i, used, level))
		}
	}
}

func (golf *Golfhash) Visit(visitor Visitor, arg interface{}) {
	golf.visit(golf.table, 0, 0, visitor, arg)
}
