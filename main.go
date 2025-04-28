package main

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"unsafe"
)

const shardCount = 128

const workerCount = 12

type GlobalState struct {
	shards       [shardCount]*shard
	usedAccsPool sync.Pool
	shardsPool   sync.Pool
}

type shard struct {
	accs map[string]uint
	mu   sync.Mutex
}

func NewGlobalState() *GlobalState {
	gs := &GlobalState{
		usedAccsPool: sync.Pool{
			New: func() any {
				m := make(map[string]struct{}, 100)
				return &m
			},
		},
		shardsPool: sync.Pool{

			New: func() any {
				s := make([]*shard, 0, 16)
				return &s
			},
		},
	}

	for i := 0; i < shardCount; i++ {
		gs.shards[i] = &shard{accs: make(map[string]uint)}
	}

	return gs
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write(*(*[]byte)(unsafe.Pointer(&s)))
	return h.Sum32()
}

func (gs *GlobalState) getAccountID(name string) uint32 {
	return hashString(name)
}

func (gs *GlobalState) getShardByID(id uint32) *shard {
	return gs.shards[uint32(id)%shardCount]
}

func (gs *GlobalState) InitAccount(name string, balance uint) {
	accID := gs.getAccountID(name)
	s := gs.getShardByID(accID)
	s.accs[name] = balance
}

type AccountUpdate struct {
	Name          string
	BalanceChange int
}

type Transaction interface {
	Updates() []AccountUpdate
}

type transfer struct {
	from  string
	to    string
	value int
}

func (t transfer) Updates() []AccountUpdate {
	return []AccountUpdate{
		{Name: t.from, BalanceChange: -t.value},
		{Name: t.to, BalanceChange: t.value},
	}
}

func (gs *GlobalState) buildBatch(pendingTx []Transaction) (batch []Transaction, retry []Transaction) {
	usedAccounts := gs.usedAccsPool.Get().(*map[string]struct{})
	for k := range *usedAccounts {
		delete(*usedAccounts, k)
	}

	for _, tx := range pendingTx {
		u := tx.Updates()
		conflict := false

		for _, acc := range u {
			if _, exists := (*usedAccounts)[acc.Name]; exists {
				conflict = true
				break
			}
		}

		if conflict {
			retry = append(retry, tx)
		} else {
			for _, acc := range u {
				(*usedAccounts)[acc.Name] = struct{}{}
			}
			batch = append(batch, tx)
		}
	}

	gs.usedAccsPool.Put(usedAccounts)
	return
}

func (gs *GlobalState) Execute(transactions []Transaction) {
	pendingTx := transactions

	for len(pendingTx) > 0 {
		batch, retryTx := gs.buildBatch(pendingTx)

		txCh := make(chan Transaction, len(batch))
		var wg sync.WaitGroup

		for i := 0; i < workerCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for tx := range txCh {
					updates := tx.Updates()
					gs.applyUpdates(updates)
				}
			}()
		}

		for _, tx := range batch {
			txCh <- tx
		}

		close(txCh)
		wg.Wait()

		pendingTx = retryTx
	}
}

func (gs *GlobalState) applyUpdates(updates []AccountUpdate) {
	shardSet := make(map[*shard]struct{})
	for _, u := range updates {
		accID := gs.getAccountID(u.Name)
		shard := gs.getShardByID(accID)
		shardSet[shard] = struct{}{}
	}

	shards := gs.shardsPool.Get().(*[]*shard)
	*shards = (*shards)[:0]
	for s := range shardSet {
		*shards = append(*shards, s)
	}

	sort.Slice(*shards, func(i, j int) bool {
		return uintptr(unsafe.Pointer((*shards)[i])) < uintptr(unsafe.Pointer((*shards)[j]))
	})

	for _, s := range *shards {
		s.mu.Lock()
	}

	defer func() {
		for _, s := range *shards {
			s.mu.Unlock()
		}
		gs.shardsPool.Put(shards)
	}()

	for _, u := range updates {
		if u.BalanceChange < 0 {
			accID := gs.getAccountID(u.Name)
			shard := gs.getShardByID(accID)
			if shard.accs[u.Name] < uint(-u.BalanceChange) {
				return
			}
		}
	}

	for _, u := range updates {
		accID := gs.getAccountID(u.Name)
		shard := gs.getShardByID(accID)
		shard.accs[u.Name] += uint(int(u.BalanceChange))
	}
}

func main() {
	gs := NewGlobalState()

	gs.InitAccount("A", 100)
	gs.InitAccount("B", 200)
	gs.InitAccount("C", 300)
	gs.InitAccount("G", 400)
	gs.InitAccount("K", 400)
	gs.InitAccount("E", 400)
	gs.InitAccount("F", 400)

	txs := []Transaction{
		transfer{"A", "B", 100},
		transfer{"B", "C", 250},
		transfer{"G", "C", 300},
		transfer{"K", "B", 420},
		transfer{"E", "F", 220},
		transfer{"B", "K", 90},
		transfer{"A", "B", 100},
		transfer{"B", "C", 250},
		transfer{"G", "C", 300},
		transfer{"K", "B", 420},
		transfer{"E", "F", 220},
		transfer{"B", "K", 90},
	}

	gs.Execute(txs)

	type Acc struct {
		Name    string
		Balance uint
	}

	result := make([]Acc, 0)

	for _, s := range gs.shards {
		for k, v := range s.accs {
			result = append(result, Acc{Name: k, Balance: v})
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	fmt.Println(result)
}
