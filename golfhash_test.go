package golfhash

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func init() {
	now := time.Now()
	rand.Seed(now.UnixNano())
}

type typeTestCase struct {
	ht HashType
	pk unsafe.Pointer
	v  hashVal
}

func TestTypeHash(t *testing.T) {
	var (
		ints = [3]int{1, -1, 999999999}
		strs = [7]string{
			"key",
			"key1",
			"key2",
			"abcdefghijklmn",
			"abcdefghijklm",
			"135792468",
			"135792464",
		}
		testHash = [...]typeTestCase{
			{IntegerHashType, unsafe.Pointer(&ints[0]), 4294967296},
			{IntegerHashType, unsafe.Pointer(&ints[1]), 18446744069414584320},
			{IntegerHashType, unsafe.Pointer(&ints[2]), 4294967291705032704},
			{StringHashType, unsafe.Pointer(&strs[0]), 6669095675671937900},
			{StringHashType, unsafe.Pointer(&strs[1]), 7915237258779523427},
			{StringHashType, unsafe.Pointer(&strs[2]), 7938581452857072930},
			{StringHashType, unsafe.Pointer(&strs[3]), 3280151737065537630},
			{StringHashType, unsafe.Pointer(&strs[4]), 15105585979892399948},
			{StringHashType, unsafe.Pointer(&strs[5]), 16535278558080922240},
			{StringHashType, unsafe.Pointer(&strs[6]), 16815408887011516276},
		}
	)
	var prev typeTestCase
	for _, test := range testHash {
		h := test.ht.Hash(test.pk)
		fmt.Printf("hash: %d, str: %s\n", h, test.ht.String(test.pk))
		if h != test.v {
			t.Errorf("inconsistent hash: %d expected: %d", h, test.v)
		}
		if test.ht == prev.ht && test.ht.Equal(test.pk, prev.pk) {
			t.Errorf("inconsistent compararision: %s, %s", test.ht.String(test.pk), test.ht.String(prev.pk))
		}
		prev = test
	}
}

func printVisitor(arg interface{}, level int, key, val unsafe.Pointer) {
	tab := strings.Repeat("\t", int(level))
	switch arg.(type) {
	case IntHashType:
		fmt.Printf("%skey: %d, val: %d\n", tab, *(*int)(key), *(*int)(val))
	case StrHashType:
		fmt.Printf("%skey: %s, val: %s\n", tab, *(*string)(key), *(*string)(val))
	}
}
func countVisitor(arg interface{}, level int, key, val unsafe.Pointer) {
	if si, ok := arg.([]int); ok {
		si[level] += 1
	}
}

func TestTriHashBase(t *testing.T) {
	golf := Init(TriStructure, StringHashType)
	if golf.Count() != 0 {
		t.Errorf("inconsistent data count: %d expected:%d", golf.Count(), 0)
	}
	key := "key"
	val := "val"
	if !golf.Insert(unsafe.Pointer(&key), unsafe.Pointer(&val)) {
		t.Fatalf("insert failed key:%s val:%s", key, val)
	}
	golf.Visit(printVisitor, StringHashType)
	var result *string
	if !golf.Lookup(unsafe.Pointer(&key), (*unsafe.Pointer)(unsafe.Pointer(&result))) {
		t.Fatalf("lookup failed key:%s", key)
	} else {
		fmt.Printf("lookup key:%s, val:%s\n", key, *result)
	}
	if golf.Count() != 1 {
		t.Fatalf("inconsistent data count: %d expected:%d", golf.Count(), 1)
	}
	golf.Visit(printVisitor, StringHashType)
	if !golf.Remove(unsafe.Pointer(&key)) {
		t.Fatalf("remove failed key:%s", key)
	}
	if golf.Lookup(unsafe.Pointer(&key), (*unsafe.Pointer)(unsafe.Pointer(&result))) {
		t.Fatalf("inconsistent lookup result key:%s val:%s", key, *result)
	}
	golf.Visit(printVisitor, StringHashType)
	if golf.Count() != 0 {
		t.Fatalf("inconsistent data count: %d expected:%d", golf.Count(), 0)
	}

}

func TestStringHashBase(t *testing.T) {
	golf := Init(OASkipListStructure, StringHashType)
	if golf.Count() != 0 {
		t.Errorf("inconsistent data count: %d expected:%d", golf.Count(), 0)
	}
	key := "key"
	val := "val"
	if !golf.Insert(unsafe.Pointer(&key), unsafe.Pointer(&val)) {
		t.Fatalf("insert failed key:%s val:%s", key, val)
	}
	var result *string
	if !golf.Lookup(unsafe.Pointer(&key), (*unsafe.Pointer)(unsafe.Pointer(&result))) {
		t.Fatalf("lookup failed key:%s", key)
	} else {
		fmt.Printf("lookup key:%s, val:%s\n", key, *result)
	}
	if golf.Count() != 1 {
		t.Fatalf("inconsistent data count: %d expected:%d", golf.Count(), 1)
	}
	if !testing.Short() {
		golf.Visit(printVisitor, StringHashType)
	}
	if !golf.Remove(unsafe.Pointer(&key)) {
		t.Fatalf("remove failed key:%s", key)
	}
	if golf.Lookup(unsafe.Pointer(&key), (*unsafe.Pointer)(unsafe.Pointer(&result))) {
		t.Fatalf("inconsistent lookup result key:%s val:%s", key, *result)
	}
	if golf.Count() != 0 {
		t.Fatalf("inconsistent data count: %d expected:%d", golf.Count(), 0)
	}

}

func TestIntegerHashBase(t *testing.T) {
	golf := Init(OASkipListStructure, IntegerHashType)
	if golf.Count() != 0 {
		t.Errorf("inconsistent data count: %d expected:%d", golf.Count(), 0)
	}
	key := 20120926
	val := 20120322
	if !golf.Insert(unsafe.Pointer(&key), unsafe.Pointer(&val)) {
		t.Fatalf("insert failed key:%d val:%d", key, val)
	}
	var result *int
	if !golf.Lookup(unsafe.Pointer(&key), (*unsafe.Pointer)(unsafe.Pointer(&result))) {
		t.Fatalf("lookup failed key:%d", key)
	} else {
		fmt.Printf("lookup key:%d, val:%d\n", key, *result)
	}
	if golf.Count() != 1 {
		t.Fatalf("inconsistent data count: %d expected:%d", golf.Count(), 1)
	}
	if !testing.Short() {
		golf.Visit(printVisitor, IntegerHashType)
	}
	if !golf.Remove(unsafe.Pointer(&key)) {
		t.Fatalf("remove failed key:%d", key)
	}
	if golf.Lookup(unsafe.Pointer(&key), (*unsafe.Pointer)(unsafe.Pointer(&result))) {
		t.Fatalf("inconsistent lookup result key:%d val:%d", key, *result)
	}
	if golf.Count() != 0 {
		t.Fatalf("inconsistent data count: %d expected:%d", golf.Count(), 0)
	}

}

func TestIntegerHashInsert100000(t *testing.T) {
	golf := Init(OASkipListStructure, IntegerHashType)
	if golf.Count() != 0 {
		t.Fatalf("inconsistent data count: %d expected:%d", golf.Count(), 0)
	}
	const loop = 1e5
	var fk, lk, dup int
	for i := 0; i < loop; i++ {
		key := rand.Int()
		val := rand.Int()
		if i == 0 {
			fk = key
		} else if i == loop-1 {
			lk = key
		}
		if !golf.Insert(unsafe.Pointer(&key), unsafe.Pointer(&val)) {
			dup++
		}
	}
	var result *int
	if !golf.Lookup(unsafe.Pointer(&fk), (*unsafe.Pointer)(unsafe.Pointer(&result))) {
		t.Errorf("inconsistent lookup result key:%d val:%d", fk, *result)
	}
	if !golf.Lookup(unsafe.Pointer(&lk), (*unsafe.Pointer)(unsafe.Pointer(&result))) {
		t.Errorf("inconsistent lookup result key:%d val:%d", lk, *result)
	}
	if !testing.Short() {
		golf.Visit(printVisitor, IntegerHashType)
	} else {
		counter := make([]int, 5)
		golf.Visit(countVisitor, counter)
		fmt.Printf("counter/level:%v, dup:%d\n", counter, dup)
	}
	if int(golf.Count()) != loop-dup {
		t.Fatalf("inconsistent data count: %d expected:%d", golf.Count(), loop-dup)
	}
}

func TestStringHashInsert100000(t *testing.T) {
	golf := Init(OASkipListStructure, StringHashType)
	testStringHashInsert100000(golf, t)
}
func TestStringTriInsert100000(t *testing.T) {
	golf := Init(TriStructure, StringHashType)
	testStringHashInsert100000(golf, t)
}

func testStringHashInsert100000(golf HashInterface, t *testing.T) {
	if golf.Count() != 0 {
		t.Fatalf("inconsistent data count: %d expected:%d", golf.Count(), 0)
	}
	const loop = 1e5
	var fk, lk string
	var dup int
	for i := 0; i < loop; i++ {
		key := strconv.Itoa(rand.Int())
		val := strconv.Itoa(rand.Int())
		if i == 0 {
			fk = key
		} else if i == loop-1 {
			lk = key
		}
		if !golf.Insert(unsafe.Pointer(&key), unsafe.Pointer(&val)) {
			dup++
		}
	}
	var result *string
	if !golf.Lookup(unsafe.Pointer(&fk), (*unsafe.Pointer)(unsafe.Pointer(&result))) {
		t.Errorf("inconsistent lookup result key:%s val:%s", fk, *result)
	}
	if !golf.Lookup(unsafe.Pointer(&lk), (*unsafe.Pointer)(unsafe.Pointer(&result))) {
		t.Errorf("inconsistent lookup result key:%s val:%s", lk, *result)
	}
	if !testing.Short() {
		golf.Visit(printVisitor, StringHashType)
	} else {
		counter := make([]int, 10)
		golf.Visit(countVisitor, counter)
		fmt.Printf("counter/level:%v, dup:%d\n", counter, dup)
	}
	if int(golf.Count()) != loop-dup {
		t.Fatalf("inconsistent data count: %d expected:%d", golf.Count(), loop-dup)
	}
}

func TestStringHashParallelOp(t *testing.T) {
	golf := Init(OASkipListStructure, StringHashType)
	testStringHashParallelOp(golf, t)
}
func TestStringTriParallelOp(t *testing.T) {
	golf := Init(TriStructure, StringHashType)
	testStringHashParallelOp(golf, t)
}
func testStringHashParallelOp(golf HashInterface, t *testing.T) {
	const numRec = 1e5
	const numGoroutines = 8
	var dup int64
	done := make(chan bool)
	for t := 0; t < numGoroutines; t++ {
		go func(tid int) {
			for i := 0; i < numRec; i++ {
				key := strconv.Itoa(rand.Intn(numRec) + tid*numRec)
				val := strconv.Itoa(rand.Intn(numRec))
				if !golf.Insert(unsafe.Pointer(&key), unsafe.Pointer(&val)) {
					atomic.AddInt64(&dup, 1)
				}
			}
			done <- true
		}(t)
	}
	for t := 0; t < numGoroutines; t++ {
		<-done
	}
	counter := make([]int, 10)
	golf.Visit(countVisitor, counter)
	fmt.Printf("counter/level:%v, dup:%d\n", counter, dup)
	if golf.Count() != (numGoroutines*numRec - dup) {
		t.Fatalf("inconsistent data count: %d expected:%d", golf.Count(), numGoroutines*numRec-dup)
	}
	// count
	/*
		var total int64
		for _, c := range counter {
			total += int64(c)
		}
		if golf.Count() != total {
			t.Fatalf("inconsistent data count: %d expected:%d", golf.Count(), total)
		}
	*/
}

const (
	numGoroutines = 8
)

func BenchmarkStringHashInsert(b *testing.B) {
	golf := Init(OASkipListStructure, StringHashType)
	benchmarkStringHashInsert(golf, b)
}
func BenchmarkStringTriInsert(b *testing.B) {
	golf := Init(TriStructure, StringHashType)
	benchmarkStringHashInsert(golf, b)
}
func benchmarkStringHashInsert(golf HashInterface, b *testing.B) {
	done := make(chan bool)
	for t := 0; t < numGoroutines; t++ {
		go func(tid int) {
			for i := 0; i < b.N; i++ {
				key := strconv.FormatInt(rand.Int63(), 10)
				val := strconv.FormatInt(rand.Int63(), 10)
				golf.Insert(unsafe.Pointer(&key), unsafe.Pointer(&val))
			}
			done <- true
		}(t)
	}
	for t := 0; t < numGoroutines; t++ {
		<-done
	}
}
func BenchmarkStringHashInsertBuiltin(b *testing.B) {
	var lk sync.RWMutex
	m := make(map[string]*string)
	done := make(chan bool)
	for t := 0; t < numGoroutines; t++ {
		go func(tid int) {
			for i := 0; i < b.N; i++ {
				key := strconv.FormatInt(rand.Int63(), 10)
				val := strconv.FormatInt(rand.Int63(), 10)
				lk.Lock()
				m[key] = &val
				lk.Unlock()
			}
			done <- true
		}(t)
	}
	for t := 0; t < numGoroutines; t++ {
		<-done
	}
}

func BenchmarkStringHashLookup(b *testing.B) {
	golf := Init(OASkipListStructure, StringHashType)
	benchmarkStringHashLookup(golf, b)
}
func BenchmarkStringTriLookup(b *testing.B) {
	golf := Init(TriStructure, StringHashType)
	benchmarkStringHashLookup(golf, b)
}
func benchmarkStringHashLookup(golf HashInterface, b *testing.B) {
	b.StopTimer()
	const numRec = 5e5
	var dup, hit int
	var pval unsafe.Pointer
	for i := 0; i < numRec; i++ {
		key := strconv.Itoa(rand.Intn(numRec))
		val := strconv.Itoa(rand.Intn(numRec))
		if !golf.Insert(unsafe.Pointer(&key), unsafe.Pointer(&val)) {
			dup++
		}
	}
	counter := make([]int, 10)
	golf.Visit(countVisitor, counter)
	fmt.Printf("counter/level:%v, dup:%d\n", counter, dup)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(rand.Intn(numRec))
		if golf.Lookup(unsafe.Pointer(&key), &pval) {
			hit++
		}
	}
}
func BenchmarkStringHashLookupBuiltin(b *testing.B) {
	b.StopTimer()
	const numRec = 5e5
	var dup, hit int
	m := make(map[string]*string)
	for i := 0; i < numRec; i++ {
		key := strconv.Itoa(rand.Intn(numRec))
		val := strconv.Itoa(rand.Intn(numRec))
		if _, exist := m[key]; exist {
			dup++
		}
		m[key] = &val
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(rand.Intn(numRec))
		if _, exist := m[key]; exist {
			hit++
		}
	}
}

func BenchmarkIntHashInsert(b *testing.B) {
	golf := Init(OASkipListStructure, IntegerHashType)
	done := make(chan bool)
	for t := 0; t < numGoroutines; t++ {
		go func(tid int) {
			for i := 0; i < b.N; i++ {
				key := rand.Int()
				val := rand.Int()
				golf.Insert(unsafe.Pointer(&key), unsafe.Pointer(&val))
			}
			done <- true
		}(t)
	}
	for t := 0; t < numGoroutines; t++ {
		<-done
	}
}
func BenchmarkIntHashInsertBuiltin(b *testing.B) {
	var lk sync.RWMutex
	m := make(map[int]*int)
	done := make(chan bool)
	for t := 0; t < numGoroutines; t++ {
		go func(tid int) {
			for i := 0; i < b.N; i++ {
				key := rand.Int()
				val := rand.Int()
				lk.Lock()
				m[key] = &val
				lk.Unlock()
			}
			done <- true
		}(t)
	}
	for t := 0; t < numGoroutines; t++ {
		<-done
	}
}
