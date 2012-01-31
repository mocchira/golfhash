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
	defMaxProbes = 8
	defSubNum    = 1<<defPower + defMaxProbes
	hashLow      = 6
	hashOne      = 1 << hashLow
	hashMask     = hashOne - 1
	hashIdxMask  = 1<<defPower - 1
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
	shift int
	used  int
	entry [defSubNum]*hashEntry
}
type Golfhash struct {
	Count int64
	power int
	table *hashSubTable
	htype HashType
}

type hashInsertPoint struct {
	idx int
	st  *hashSubTable
}

var (
	IntegerHashType IntHashType
	StringHashType  StrHashType
)

func newHashSubTable(used int) *hashSubTable {
	st := &hashSubTable{
		shift: hashBits - used,
		used:  used,
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
	var hi1, hi2 hashInsertPoint
	var di int
	var pv unsafe.Pointer
	pst := golf.table
	hash := golf.htype.Hash(key)
	hash &^= hashMask
	if hash < hashLow {
		hash += hashOne
	}
	for {
		if golf.lookup(key, hash, &pst, &di, &pv, &hi1, &hi2) {
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
	var hi1, hi2 hashInsertPoint
	var di int
	pst := golf.table
	hash := golf.htype.Hash(key)
	hash &^= hashMask
	if hash < hashLow {
		hash += hashOne
	}
	return golf.lookup(key, hash, &pst, &di, val, &hi1, &hi2)
}

func (golf *Golfhash) lookup(key unsafe.Pointer,
				hash hashVal,
				st **hashSubTable,
				i *int,
				val *unsafe.Pointer,
				pnil *hashInsertPoint,
				palloc *hashInsertPoint) bool {
	idx := int((uint64(hash) >> uint64((*st).shift)) & uint64(hashIdxMask))
	for *i = idx; *i < idx+defMaxProbes; *i++ {
		e := (*st).entry[*i]
		if e == nil {
			if pnil.st == nil {
				pnil.st = *st
				pnil.idx = *i
			}
			continue
		}
		if e.hash&hashMask == hashSubHash {
			var ni int
			pstOld := *st
			*st = (*hashSubTable)(e.val)
			if golf.lookup(key, hash, st, &ni, val, pnil, palloc) {
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
		if palloc.st == nil {
			palloc.st = *st
			palloc.idx = *i
		}
	}
	return false
}

func (golf *Golfhash) insert2alloc(key unsafe.Pointer, val unsafe.Pointer, hash hashVal, st *hashSubTable) bool {
	idx := int((uint64(hash) >> uint64(st.shift)) & uint64(hashIdxMask))
	for i := idx; i < idx+defMaxProbes; i++ {
		e := st.entry[i]
		if e != nil && (e.hash&hashMask) == hashSubHash {
			if golf.insert2alloc(key, val, hash, (*hashSubTable)(e.val)) {
				return true
			}
		} else {
			nst := newHashSubTable(st.used + defPower)
			if e != nil {
				golf.insert2nil(e.key, e.val, e.hash, nst) // no possiblity to fail
			}
			he := &hashEntry{
				hash: hashSubHash,
				key:  nil,
				val:  unsafe.Pointer(nst),
			}
			if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&st.entry[i])), unsafe.Pointer(e), unsafe.Pointer(he)) {
				if golf.insert2nil(key, val, hash, nst) {
					return true
				}
			}
		}
	}
	return false
}

func (golf *Golfhash) insert2nil(key unsafe.Pointer, val unsafe.Pointer, hash hashVal, st *hashSubTable) bool {
	idx := int((uint64(hash) >> uint64(st.shift)) & uint64(hashIdxMask))
	for i := idx; i < idx+defMaxProbes; i++ {
		e := st.entry[i]
		if e == nil {
			he := &hashEntry{
				hash: hash,
				key:  key,
				val:  val,
			}
			if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&st.entry[i])), unsafe.Pointer(e), unsafe.Pointer(he)) {
				return true
			}
		} else if e.hash&hashMask == hashSubHash {
			if golf.insert2nil(key, val, hash, (*hashSubTable)(e.val)) {
				return true
			}
		}
	}
	return false
}

func (golf *Golfhash) Insert(key unsafe.Pointer, val unsafe.Pointer) bool {
	var hi1, hi2 hashInsertPoint
	var di int
	var pv unsafe.Pointer
	hash := golf.htype.Hash(key)
	hash &^= hashMask
	if hash < hashLow {
		hash += hashOne
	}
	pst := golf.table
	he := &hashEntry{
		hash: hash,
		key:  key,
		val:  val,
	}
	for {
		if golf.lookup(key, hash, &pst, &di, &pv, &hi1, &hi2) {
			if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&pst.entry[di])), unsafe.Pointer(pst.entry[di]), unsafe.Pointer(he)) {
				return false
			}
		} else {
			if hi1.st != nil {
				if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&hi1.st.entry[hi1.idx])), nil, unsafe.Pointer(he)) {
					atomic.AddInt64(&golf.Count, 1)
					return true
				}
			}
			if hi2.st != nil {
				e := hi2.st.entry[hi2.idx]
				if e.hash&hashMask == hashSubHash {
					break
				}
				nst := newHashSubTable(hi2.st.used + defPower)
				if e != nil {
					golf.insert2nil(e.key, e.val, e.hash, nst) // no possiblity to fail
				}
				hest := &hashEntry{
					hash: hashSubHash,
					key:  nil,
					val:  unsafe.Pointer(nst),
				}
				if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&hi2.st.entry[hi2.idx])), unsafe.Pointer(e), unsafe.Pointer(hest)) {
					if golf.insert2nil(key, val, hash, nst) {
						atomic.AddInt64(&golf.Count, 1)
						return true
					}
				}
			}
			break
		}

	}
	for {
		if golf.insert2nil(key, val, hash, golf.table) {
			atomic.AddInt64(&golf.Count, 1)
			return true
		}
		if golf.insert2alloc(key, val, hash, golf.table) {
			atomic.AddInt64(&golf.Count, 1)
			return true
		}
	}
	// usually not reached
	return false
}

type Visitor func(arg interface{}, level int, key, val unsafe.Pointer)

func (golf *Golfhash) visit(st *hashSubTable, level int, visitor Visitor, arg interface{}) {
	var i int
	for ; i < len(st.entry); i++ {
		e := st.entry[i]
		if e == nil {
			continue
		}
		if e.hash&hashMask == hashSubHash {
			golf.visit((*hashSubTable)(e.val), level+1, visitor, arg)
			continue
		} else {
			visitor(arg, level, e.key, e.val)
		}
		idx := int((uint64(e.hash) >> uint64(st.shift)) & uint64(hashIdxMask))
		if i >= idx+defMaxProbes || i < idx {
			panic(fmt.Sprintf("inconsisntet index: %d, used: %d, level: %d", i, st.used, level))
		}
	}
}

func (golf *Golfhash) Visit(visitor Visitor, arg interface{}) {
	golf.visit(golf.table, 0, visitor, arg)
}
