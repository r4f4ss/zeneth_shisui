package history

import (
	"bytes"
	"fmt"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

// Maximum header length is 2048 bytes
// Expects clients to store the full window of 8192 blocks of this data
// 100 slots for reorgs
// Max size in bytes is 2048 * (8192 + 100)
// https://github.com/ethereum/portal-network-specs/blob/master/history/history-network.md#ephemeral-block-headers
var headerStoreCacheSize = 2048 * (8192 + 100)

type ephemeralStore struct {
	headerStore *fastcache.Cache
	log         log.Logger
}

func NewEphemeralStore() *ephemeralStore {
	store := &ephemeralStore{
		headerStore: fastcache.New(headerStoreCacheSize),
		log:         log.New("sub-protocol", "history"),
	}
	return store
}

func (e *ephemeralStore) handleContents(contentKeys [][]byte, contents [][]byte) error {
	var parentHash common.Hash
	gotHead := false
	for i, content := range contents {
		contentKey := contentKeys[i]
		if ContentType(contentKey[0]) != OfferEphemeralType {
			return fmt.Errorf("content key diferent of type Ephemeral: content key %x", contentKey)
		}

		header, err := DecodeBlockHeader(content)
		if err != nil {
			return err
		}

		isHead := true
		if !gotHead && !isHead {
			e.log.Info("ephemeral header is not HEAD", "hash", header.Hash())
			continue
		} else if isHead {
			gotHead = true
		}

		headerhash := header.Hash()
		if i > 0 && parentHash.Cmp(headerhash) != 0 {
			return fmt.Errorf("hash diferent from last block paretHash: hash %x, paretHash %x", headerhash, parentHash)
		}

		if !bytes.Equal(headerhash.Bytes(), contentKey[1:]) {
			return fmt.Errorf("header hash diferent from block_hash: header hash %x, content key %x", headerhash, contentKey[1:])
		}

		has := e.headerStore.Has(headerhash.Bytes())
		if has {
			return nil
		} else {
			h, err := header.MarshalJSON()
			if err != nil {
				return err
			}
			e.headerStore.Set(headerhash.Bytes(), h)
		}

		parentHash = header.ParentHash
	}
	return nil
}

// func (e *ephemeralStore) validateContent(contentKey []byte, content []byte) error {
// 	//verificar se o hash é o hash informado
// 	return nil
// }
