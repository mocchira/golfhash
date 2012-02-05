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
	count int64
	power int
	table *hashSubTable
	htype HashType
}

type hashInsertPoint struct {
	idx int
	st  *hashSubTable
}

type triEntryList []*triEntry

type triEntry struct {
	key         string
	val         []unsafe.Pointer
	entryLists  []*triEntryList
	iEntryLists []triEntryList
	numChildren []int32
}
type Trihash struct {
	count int64
	entry *triEntry
}

type HashInterface interface {
	Remove(key unsafe.Pointer) bool
	Lookup(key unsafe.Pointer, val *unsafe.Pointer) bool
	Insert(key unsafe.Pointer, val unsafe.Pointer) bool
	Visit(visitor Visitor, arg interface{})
	Count() int64
}

var (
	IntegerHashType IntHashType
	StringHashType  StrHashType
)

const (
	OASkipListStructure = iota
	TriStructure
)

func newHashSubTable(used int) *hashSubTable {
	st := &hashSubTable{
		shift: hashBits - used,
		used:  used,
	}
	return st
}
func newTrientry(key, val unsafe.Pointer, si, ei int) *triEntry {
	var s string
	el := 1
	if key != nil {
		s = (*(*string)(key))[si:ei]
		el = len(s)
		//fmt.Printf("new s:%s len:%d\n", s, el)
	}
	tri := &triEntry{
		key:         s,
		val:         make([]unsafe.Pointer, el),
		entryLists:  make([]*triEntryList, el),
		iEntryLists: make([]triEntryList, el),
		numChildren: make([]int32, el),
	}
	tri.val[el-1] = val
	for i := 0; i < el; i++ {
		tri.iEntryLists[i] = triEntryList(make([]*triEntry, 10))
		tri.entryLists[i] = &tri.iEntryLists[i]
	}
	return tri
}

func Init(s int, ht HashType) HashInterface {
	switch s {
	case OASkipListStructure:
		return &Golfhash{
			count: 0,
			power: defPower,
			table: newHashSubTable(defPower),
			htype: ht,
		}
	case TriStructure:
		return &Trihash{
			count: 0,
			entry: newTrientry(nil, nil, 0, 0),
		}
	}
	return nil
}

func (tri *Trihash) Remove(key unsafe.Pointer) bool {
	te := tri.entry
	var lidx, idx, vi, si int
	for {
		if tri.lookup(&te, &lidx, &idx, key, &vi, &si) {
			dst := (*te.entryLists[lidx])[idx]
			dst.val[vi] = nil
			atomic.AddInt64(&tri.count, -1)
			atomic.AddInt32(&te.numChildren[lidx], -1)
			return true
		} else {
			break
		}
	}
	return false
}

func (tri *Trihash) lookup(te **triEntry, lidx, idx *int, key unsafe.Pointer, vi *int, si *int) bool {
	str := []byte(*(*string)(key))
	//fmt.Printf("lookup lidx:%d idx:%d si:%d\n", *lidx, *idx, *si)
	li := str[*si] % byte(len(*(*te).entryLists[*lidx]))
	if nte := (*(*te).entryLists[*lidx])[li]; nte != nil {
		ei := *si
		nstr := []byte(nte.key)
		for _, b := range nstr {
			if len(str) == ei {
				break
			}
			if b == str[ei] {
				ei++
				continue
			} else {
				break
			}
		}
		if len(str) == ei {
			*idx = int(li)
			*vi = ei - *si - 1
			//fmt.Printf("lookup val:%v vi:%d\n", nte.val, *vi)
			return true
		}
		if (ei - *si) > 0 {
			*te = nte
			*lidx = ei - *si - 1
			*si = ei
			return tri.lookup(te, lidx, idx, key, vi, si)
		}
	}
	*idx = int(li)
	*vi = -1
	return false
}
func (tri *Trihash) Lookup(key unsafe.Pointer, val *unsafe.Pointer) bool {
	te := tri.entry
	var lidx, idx, si, vi int
	if tri.lookup(&te, &lidx, &idx, key, &vi, &si) {
		*val = (*te.entryLists[lidx])[idx].val[vi]
		return *val != nil
	}
	return false
}
func (tri *Trihash) Insert(key unsafe.Pointer, val unsafe.Pointer) bool {
	var lidx, idx, vi, si int
	for {
		te := tri.entry
		lidx, idx, vi, si = 0, 0, 0, 0
		if tri.lookup(&te, &lidx, &idx, key, &vi, &si) {
			dst := (*te.entryLists[lidx])[idx]
			dst.val[vi] = val
			return false
		} else {
			//fmt.Printf("insert lidx:%d idx:%d\n", lidx, idx)
			str := []byte(*(*string)(key))
			if (*te.entryLists[lidx])[idx] == nil {
				// expand entry list
				// 1. when match substrings
				// [before]
				// one TRI node has entryLists which number is decided by own string length
				// -abcde <---- dst point to
				//      |-fg
				//      |-hi
				// [insert]
				// `abd`
				// [after]
				// -abcde    <---- dst point to
				//   |-d|
				//      |
				//      |-fg
				//      |-hi
				// make new TriEntry(d), make new entryList which is placed at pos `b`
				// compose a[b] + d
				// atomic swap &abcde[1] from nil to new entryList
				ne := newTrientry(key, val, si, len(str))
				if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&(*te.entryLists[lidx])[idx])), nil, unsafe.Pointer(ne)) {
					atomic.AddInt64(&tri.count, 1)
					atomic.AddInt32(&te.numChildren[lidx], 1)
					return true
				}
			} else {
				// 2. when not match at all
				// [before]
				//  |-[0]ab
				//  |-[1]nil
				//  |-[2]cd
				// [insert]
				// `dd` into the list result in conflicting index 0.
				// [after]
				//  |-[0]ab
				//  |-[1]nil
				//  |-[2]cd
				//  |-[3]dd
				// expand entryList, read & copy all pointers to new entryList and set `gd`
				// atomic swap *entryList
				pLists := te.entryLists[lidx]
				oldLen := len(*pLists)
				newLen := oldLen << 1
				fmt.Printf("new old:%d new:%d\n", oldLen, newLen)
				tmpLists := triEntryList(make([]*triEntry, newLen))
				for pos, ent := range *pLists {
					if ent != nil {
						fmt.Printf("realloc pos:%d key:%s\n", pos, ent.key)
						ni := ent.key[0] % byte(newLen)
						tmpLists[ni] = ent
					}
				}
				ni := str[si] % byte(newLen)
				if tmpLists[ni] != nil {
					fmt.Printf("conflict char:%c key:%s\n", str[si], string(str))
					continue
				}
				ne := newTrientry(key, val, si, len(str))
				tmpLists[ni] = ne
				if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&te.entryLists[lidx])), unsafe.Pointer(pLists), unsafe.Pointer(&tmpLists)) {
					atomic.AddInt64(&tri.count, 1)
					atomic.AddInt32(&te.numChildren[lidx], 1)
					return true
				}

			}
		}
	}
	return false
}

func (tri *Trihash) visit(te *triEntry, level int, visitor Visitor, arg interface{}) {
	for i := 0; i < len(te.entryLists); i++ {
		if v := te.val[i]; v != nil {
			key := (te.key)[:i+1]
			visitor(arg, level, unsafe.Pointer(&key), v)
		}
		if l := *te.entryLists[i]; l != nil {
			for _, e := range l {
				if e != nil {
					tri.visit(e, level+1, visitor, arg)
				}
			}
		}
	}

}
func (tri *Trihash) Visit(visitor Visitor, arg interface{}) {
	tri.visit(tri.entry, 0, visitor, arg)
}
func (tri *Trihash) Count() int64 {
	return tri.count
}

func (golf *Golfhash) Count() int64 {
	return golf.count
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
				atomic.AddInt64(&golf.count, -1)
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
					atomic.AddInt64(&golf.count, 1)
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
						atomic.AddInt64(&golf.count, 1)
						return true
					}
				}
			}
			break
		}

	}
	for {
		if golf.insert2nil(key, val, hash, golf.table) {
			atomic.AddInt64(&golf.count, 1)
			return true
		}
		if golf.insert2alloc(key, val, hash, golf.table) {
			atomic.AddInt64(&golf.count, 1)
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
