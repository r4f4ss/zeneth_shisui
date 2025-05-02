package history

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"

	"github.com/OffchainLabs/go-bitfield"
	gcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/zen-eth/shisui/portalwire"
	utp "github.com/zen-eth/utp-go"
)

// // Maximum header length is 2048 bytes
// // Expects clients to store the full window of 8192 blocks of this data
// // 100 slots for reorgs
// // Max size in bytes is 2048 * (8192 + 100)
// // https://github.com/ethereum/portal-network-specs/blob/master/history/history-network.md#ephemeral-block-headers
// var headerStoreCacheSize = 2048 * (8192 + 100)

// var ErrEphemeralContentNotFound = errors.New("ephemeral content not found")

// type ephemeralStore struct {
// 	HeaderStore *fastcache.Cache
// 	log         log.Logger
// }

// func NewEphemeralStore() *ephemeralStore {
// 	store := &ephemeralStore{
// 		HeaderStore: fastcache.New(headerStoreCacheSize),
// 		log:         log.New("sub-protocol", "history"),
// 	}
// 	return store
// }

// func (e *ephemeralStore) handleContents(contentKeys [][]byte, contents [][]byte) error {
// 	var parentHash common.Hash
// 	gotHead := false
// 	for i, content := range contents {
// 		contentKey := contentKeys[i]
// 		if !isEphemeralOfferType(contentKey) {
// 			return fmt.Errorf("content key diferent of type Ephemeral: content key %x", contentKey)
// 		}

// 		header, err := DecodeBlockHeader(content)
// 		if err != nil {
// 			return err
// 		}

// 		isHead := true
// 		if !gotHead && !isHead {
// 			e.log.Info("ephemeral header is not HEAD", "hash", header.Hash())
// 			continue
// 		} else if isHead {
// 			gotHead = true
// 		}

// 		headerhash := header.Hash()
// 		if i > 0 && parentHash.Cmp(headerhash) != 0 {
// 			return fmt.Errorf("hash diferent from last block paretHash: hash %x, paretHash %x", headerhash, parentHash)
// 		}

// 		if !bytes.Equal(headerhash.Bytes(), contentKey[1:]) {
// 			return fmt.Errorf("header hash diferent from block_hash: header hash %x, content key %x", headerhash, contentKey[1:])
// 		}

// 		has := e.headerStore.Has(headerhash.Bytes())
// 		if has {
// 			return nil
// 		} else {
// 			h, err := header.MarshalJSON()
// 			if err != nil {
// 				return err
// 			}
// 			e.headerStore.Set(headerhash.Bytes(), h)
// 		}

// 		parentHash = header.ParentHash
// 	}
// 	return nil
// }

// func isEphemeralOfferType(contentKey []byte) bool {
// 	return ContentType(contentKey[0]) == OfferEphemeralType
// }

// func filterEphemeralContentKeys(request *portalwire.Offer) (portalwire.CommonAccept, [][]byte) {
// 	contentKeyBitlist := bitfield.NewBitlist(uint64(len(request.ContentKeys)))
// 	acceptContentKeys := make([][]byte, 0)
// 	for i, contentKey := range request.ContentKeys {
// 		isHead := false
// 		if isHead {
// 			break
// 		}
// 		contentKeyBitlist.SetBitAt(uint64(i), true)
// 		acceptContentKeys = append(acceptContentKeys, contentKey)
// 	}
// 	accept := &portalwire.Accept{
// 		ContentKeys: contentKeyBitlist,
// 	}
// 	return accept, acceptContentKeys
// }

func (h *Network) isEphemeralOfferType(contentKey []byte) bool {
	return ContentType(contentKey[0]) == OfferEphemeralType
}

func (h *Network) isEphemeralFindContentType(contentKey []byte) bool {
	return ContentType(contentKey[0]) == FindContentEphemeralType
}

func (h *Network) filterEphemeralContentKeys(request *portalwire.Offer) (portalwire.CommonAccept, [][]byte) {
	contentKeyBitlist := bitfield.NewBitlist(uint64(len(request.ContentKeys)))
	acceptContentKeys := make([][]byte, 0)
	for i, contentKey := range request.ContentKeys {
		isHead := false
		if isHead {
			break
		}
		contentKeyBitlist.SetBitAt(uint64(i), true)
		acceptContentKeys = append(acceptContentKeys, contentKey)
	}
	accept := &portalwire.Accept{
		ContentKeys: contentKeyBitlist,
	}
	return accept, acceptContentKeys
}

func (h *Network) handleEphemeralContents(contentKeys [][]byte, contents [][]byte) error {
	var parentHash gcommon.Hash
	gotHead := false
	for i, content := range contents {
		contentKey := contentKeys[i]
		if !h.isEphemeralOfferType(contentKey) {
			return fmt.Errorf("content key diferent of type Ephemeral: content key %x", contentKey)
		}

		header, err := DecodeBlockHeader(content)
		if err != nil {
			return err
		}

		isHead := true
		if !gotHead && !isHead {
			h.log.Info("ephemeral header is not HEAD", "hash", header.Hash())
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

		has := h.portalProtocol.EphemeralHeaderCache.Has(headerhash.Bytes())
		if has {
			return nil
		} else {
			he, err := header.MarshalJSON()
			if err != nil {
				return err
			}
			h.portalProtocol.EphemeralHeaderCache.Set(headerhash.Bytes(), he)
		}

		parentHash = header.ParentHash
	}
	return nil
}

func (h *Network) handleEphemeralFindContent(n *enode.Node, addr *net.UDPAddr, msg []byte) ([]byte, error) {
	request := &FindContentEphemeralHeadersKey{}
	err := request.UnmarshalSSZ(msg[1:])
	if err != nil {
		return nil, err
	}

	contentOverhead := 1 + 1 // msg id + SSZ Union selector
	maxPayloadSize := portalwire.MaxPacketSize - portalwire.TalkRespOverhead - contentOverhead
	// enrOverhead := 4 // per added ENR, 4 bytes offset overhead
	// // var err error
	// contentKey := request.ContentKey
	// contentId := p.toContentId(contentKey)
	// if contentId == nil {
	// 	return nil, ErrNilContentKey
	// }

	var content [][]byte
	var head []byte
	ok := false
	head, ok = h.portalProtocol.EphemeralHeaderCache.HasGet(head, request.BlockHash)
	if !ok {
		return nil, portalwire.ErrContentNotFound
	}

	var pointer *types.Header
	content = make([][]byte, request.AncestorCount+1, request.AncestorCount+1)
	content[0] = head
	err = pointer.UnmarshalJSON(head)
	if err != nil {
		return nil, err
	}
	for i := 1; i < int(request.AncestorCount)+1; i++ {
		olderHash := pointer.ParentHash
		head, ok = h.portalProtocol.EphemeralHeaderCache.HasGet(head, olderHash.Bytes())
		if !ok {
			break
		}
		content[i] = head
		err = pointer.UnmarshalJSON(head)
		if err != nil {
			return nil, err
		}
	}

	if len(content) <= maxPayloadSize {
		rawContentMsg := &EphemeralHeaderPayload{
			Payload: content,
		}

		h.portalProtocol.Log.Trace(">> CONTENT_RAW/history", "protocol", "history", "source", addr, "content", rawContentMsg)
		//TODO metrics
		// if metrics.Enabled() {
		// 	h.portalProtocol.PortalMetrics.messagesSentContent.Mark(1)
		// }

		var rawContentMsgBytes []byte
		rawContentMsgBytes, err = rawContentMsg.MarshalSSZ()
		if err != nil {
			return nil, err
		}

		contentMsgBytes := make([]byte, 0, len(rawContentMsgBytes)+1)
		contentMsgBytes = append(contentMsgBytes, portalwire.ContentRawSelector)
		contentMsgBytes = append(contentMsgBytes, rawContentMsgBytes...)

		talkRespBytes := make([]byte, 0, len(contentMsgBytes)+1)
		talkRespBytes = append(talkRespBytes, portalwire.CONTENT)
		talkRespBytes = append(talkRespBytes, contentMsgBytes...)

		return talkRespBytes, nil
	} else {
		connectionId := h.portalProtocol.Utp.CidWithAddr(n, addr, false)

		go func(bctx context.Context, connId *utp.ConnectionId) {
			var conn *utp.UtpStream
			var connectCtx context.Context
			var cancel context.CancelFunc
			for {
				select {
				case <-bctx.Done():
					return
				default:
					h.portalProtocol.Log.Debug("will accept find content conn from: ", "nodeId", n.ID().String(), "source", addr, "connId", connId)
					connectCtx, cancel = context.WithTimeout(bctx, portalwire.DefaultUTPConnectTimeout)
					defer cancel()
					conn, err = h.portalProtocol.Utp.AcceptWithCid(connectCtx, connectionId)
					if err != nil {
						//TODo mettrics
						// if metrics.Enabled() {
						// 	h.portalProtocol.portalMetrics.utpOutFailConn.Inc(1)
						// }
						h.portalProtocol.Log.Error("failed to accept utp connection for handle find content", "connId", connectionId.Send, "err", err)
						return
					}

					writeCtx, writeCancel := context.WithTimeout(bctx, portalwire.DefaultUTPWriteTimeout)
					defer writeCancel()
					content, err = h.encodeUtpEphemeralContent(content)
					if err != nil {
						// if metrics.Enabled() {
						// 	p.portalMetrics.utpOutFailConn.Inc(1)
						// }
						h.portalProtocol.Log.Error("encode utp content failed", "err", err)
						return
					}
					var n int
					n, err = conn.Write(writeCtx, content)
					conn.Close()
					if err != nil {
						// if metrics.Enabled() {
						// 	p.portalMetrics.utpOutFailWrite.Inc(1)
						// }
						h.portalProtocol.Log.Error("failed to write content to utp connection", "err", err)
						return
					}

					// if metrics.Enabled() {
					// 	p.portalMetrics.utpOutSuccess.Inc(1)
					// }
					h.portalProtocol.Log.Trace("wrote content size to utp connection", "n", n)
					return
				}
			}
		}(h.closeCtx, connectionId)

		idBuffer := make([]byte, 2)
		binary.BigEndian.PutUint16(idBuffer, connectionId.Send)
		connIdMsg := &portalwire.ConnectionId{
			Id: idBuffer,
		}

		h.portalProtocol.Log.Trace(">> CONTENT_CONNECTION_ID/history", "protocol", "history", "source", addr, "connId", connIdMsg)
		// if metrics.Enabled() {
		// 	p.portalMetrics.messagesSentContent.Mark(1)
		// }
		var connIdMsgBytes []byte
		connIdMsgBytes, err = connIdMsg.MarshalSSZ()
		if err != nil {
			return nil, err
		}

		contentMsgBytes := make([]byte, 0, len(connIdMsgBytes)+1)
		contentMsgBytes = append(contentMsgBytes, portalwire.ContentConnIdSelector)
		contentMsgBytes = append(contentMsgBytes, connIdMsgBytes...)

		talkRespBytes := make([]byte, 0, len(contentMsgBytes)+1)
		talkRespBytes = append(talkRespBytes, portalwire.CONTENT)
		talkRespBytes = append(talkRespBytes, contentMsgBytes...)

		return talkRespBytes, nil
	}
}

func (h *Network) encodeUtpEphemeralContent(w rlp.EncoderBuffer, data [][]byte) ([]byte, error) {

	list := w.List()
	list
	w.WriteBytes(n.Key)
	if n.Val != nil {
		n.Val.encode(w)
	} else {
		_, _ = w.Write(rlp.EmptyString)
	}
	w.ListEnd(offset)

	return encodeSingleContent(data), nil
	return data, nil
}

func (n *fullNode) encode(w rlp.EncoderBuffer) {
	offset := w.List()
	for _, c := range n.Children {
		if c != nil {
			c.encode(w)
		} else {
			_, _ = w.Write(rlp.EmptyString)
		}
	}
	w.ListEnd(offset)
}
