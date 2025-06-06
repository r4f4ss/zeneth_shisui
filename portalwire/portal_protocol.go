package portalwire

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"net"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/p2p/nat"
	"github.com/ethereum/go-ethereum/p2p/netutil"
	"github.com/ethereum/go-ethereum/rlp"
	ssz "github.com/ferranbt/fastssz"
	cache "github.com/go-pkgz/expirable-cache/v3"
	"github.com/holiman/uint256"
	zrntcommon "github.com/protolambda/zrnt/eth2/beacon/common"
	"github.com/protolambda/ztyp/view"
	pingext "github.com/zen-eth/shisui/portalwire/ping_ext"
	"github.com/zen-eth/shisui/storage"
	utp "github.com/zen-eth/utp-go"
)

const (
	// TalkResp message is a response message so the session is established and a
	// regular discv5 packet is assumed for size calculation.
	// Regular message = IV + header + message
	// talkResp message = rlp: [request-id, response]
	talkRespOverhead = 16 + // IV size
		55 + // header size
		1 + // talkResp msg id
		3 + // rlp encoding outer list, max length will be encoded in 2 bytes
		9 + // request id (max = 8) + 1 byte from rlp encoding byte string
		3 + // rlp encoding response byte string, max length in 2 bytes
		16 // HMAC

	portalFindnodesResultLimit = 32

	defaultUTPConnectTimeout = 15 * time.Second

	defaultUTPWriteTimeout = 60 * time.Second

	defaultUTPReadTimeout = 60 * time.Second

	DefaultUtpConnSize = 50

	// These are the concurrent offers per Portal wire protocol that is running.
	// Using the `offerQueue` allows for limiting the amount of offers send and
	// thus how many streams can be started.
	// TODO:
	// More thought needs to go into this as it is currently on a per network
	// basis. Keep it simple like that? Or limit it better at the stream transport
	// level? In the latter case, this might still need to be checked/blocked at
	// the very start of sending the offer, because blocking/waiting too long
	// between the received accept message and actually starting the stream and
	// sending data could give issues due to timeouts on the other side.
	// And then there are still limits to be applied also for FindContent and the
	// incoming directions.
	concurrentOffers = 50

	offerQueueSize = concurrentOffers * 20

	lookupRequestLimit = 3 // max requests against a single node during lookup

	maxPacketSize = 1280
)

const (
	TransientOfferRequestKind           byte = 0x01
	PersistOfferRequestKind             byte = 0x02
	TransientOfferRequestWithResultKind byte = 0x03
)

type OfferTraceType int

const (
	// Success indicates all accepted content keys in bitlist were transferred
	Success OfferTraceType = iota
	// Declined means peer is not interested in any of the offered content keys
	Declined
	// Failed indicates the offer failed, perhaps locally or from timeout/transfer failure
	Failed
)

var expirationVersionMinutes = 5 * time.Minute // cache versionsCache expiration time in minutes

var versionsCacheSize = nBuckets * (bucketSize + maxReplacements) // VersionsCacheSize ideally should have the buckets plus the replacement Buckets size

type protocolVersions []uint8

func (pv protocolVersions) ENRKey() string { return "pv" }

var Versions protocolVersions = protocolVersions{0, 1} //protocol network versions defined here

type ClientTag string

func (c ClientTag) ENRKey() string { return "c" }

const Tag ClientTag = "shisui"

var MaxDistance = hexutil.MustDecode("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")

var PortalBootnodes = []string{
	// Trin team's bootnodes
	"enr:-Jy4QIs2pCyiKna9YWnAF0zgf7bT0GzlAGoF8MEKFJOExmtofBIqzm71zDvmzRiiLkxaEJcs_Amr7XIhLI74k1rtlXICY5Z0IDAuMS4xLWFscGhhLjEtMTEwZjUwgmlkgnY0gmlwhKEjVaWJc2VjcDI1NmsxoQLSC_nhF1iRwsCw0n3J4jRjqoaRxtKgsEe5a-Dz7y0JloN1ZHCCIyg",
	"enr:-Jy4QKSLYMpku9F0Ebk84zhIhwTkmn80UnYvE4Z4sOcLukASIcofrGdXVLAUPVHh8oPCfnEOZm1W1gcAxB9kV2FJywkCY5Z0IDAuMS4xLWFscGhhLjEtMTEwZjUwgmlkgnY0gmlwhJO2oc6Jc2VjcDI1NmsxoQLMSGVlxXL62N3sPtaV-n_TbZFCEM5AR7RDyIwOadbQK4N1ZHCCIyg",
	"enr:-Jy4QH4_H4cW--ejWDl_W7ngXw2m31MM2GT8_1ZgECnfWxMzZTiZKvHDgkmwUS_l2aqHHU54Q7hcFSPz6VGzkUjOqkcCY5Z0IDAuMS4xLWFscGhhLjEtMTEwZjUwgmlkgnY0gmlwhJ31OTWJc2VjcDI1NmsxoQPC0eRkjRajDiETr_DRa5N5VJRm-ttCWDoO1QAMMCg5pIN1ZHCCIyg",
	// Fluffy team's bootnodes
	"enr:-IS4QGUtAA29qeT3cWVr8lmJfySmkceR2wp6oFQtvO_uMe7KWaK_qd1UQvd93MJKXhMnubSsTQPJ6KkbIu0ywjvNdNEBgmlkgnY0gmlwhMIhKO6Jc2VjcDI1NmsxoQJ508pIqRqsjsvmUQfYGvaUFTxfsELPso_62FKDqlxI24N1ZHCCI40",
	"enr:-IS4QNaaoQuHGReAMJKoDd6DbQKMbQ4Mked3Gi3GRatwgRVVPXynPlO_-gJKRF_ZSuJr3wyHfwMHyJDbd6q1xZQVZ2kBgmlkgnY0gmlwhMIhKO6Jc2VjcDI1NmsxoQM2kBHT5s_Uh4gsNiOclQDvLK4kPpoQucge3mtbuLuUGYN1ZHCCI44",
	"enr:-IS4QBdIjs6S1ZkvlahSkuYNq5QW3DbD-UDcrm1l81f2PPjnNjb_NDa4B5x4olHCXtx0d2ZeZBHQyoHyNnuVZ-P1GVkBgmlkgnY0gmlwhMIhKO-Jc2VjcDI1NmsxoQOO3gFuaCAyQKscaiNLC9HfLbVzFdIerESFlOGcEuKWH4N1ZHCCI40",
	"enr:-IS4QM731tV0CvQXLTDcZNvgFyhhpAjYDKU5XLbM7sZ1WEzIRq4zsakgrv3KO3qyOYZ8jFBK-VzENF8o-vnykuQ99iABgmlkgnY0gmlwhMIhKO-Jc2VjcDI1NmsxoQMTq6Cdx3HmL3Q9sitavcPHPbYKyEibKPKvyVyOlNF8J4N1ZHCCI44",
	// Ultralight team's bootnodes
	"enr:-IS4QFV_wTNknw7qiCGAbHf6LxB-xPQCktyrCEZX-b-7PikMOIKkBg-frHRBkfwhI3XaYo_T-HxBYmOOQGNwThkBBHYDgmlkgnY0gmlwhKRc9_OJc2VjcDI1NmsxoQKHPt5CQ0D66ueTtSUqwGjfhscU_LiwS28QvJ0GgJFd-YN1ZHCCE4k",
	"enr:-IS4QDpUz2hQBNt0DECFm8Zy58Hi59PF_7sw780X3qA0vzJEB2IEd5RtVdPUYZUbeg4f0LMradgwpyIhYUeSxz2Tfa8DgmlkgnY0gmlwhKRc9_OJc2VjcDI1NmsxoQJd4NAVKOXfbdxyjSOUJzmA4rjtg43EDeEJu1f8YRhb_4N1ZHCCE4o",
	"enr:-IS4QGG6moBhLW1oXz84NaKEHaRcim64qzFn1hAG80yQyVGNLoKqzJe887kEjthr7rJCNlt6vdVMKMNoUC9OCeNK-EMDgmlkgnY0gmlwhKRc9-KJc2VjcDI1NmsxoQLJhXByb3LmxHQaqgLDtIGUmpANXaBbFw3ybZWzGqb9-IN1ZHCCE4k",
	"enr:-IS4QA5hpJikeDFf1DD1_Le6_ylgrLGpdwn3SRaneGu9hY2HUI7peHep0f28UUMzbC0PvlWjN8zSfnqMG07WVcCyBhADgmlkgnY0gmlwhKRc9-KJc2VjcDI1NmsxoQJMpHmGj1xSP1O-Mffk_jYIHVcg6tY5_CjmWVg1gJEsPIN1ZHCCE4o",
}

var (
	ErrNilContentKey   = errors.New("content key cannot be nil")
	ErrContentNotFound = storage.ErrContentNotFound
	ErrEmptyResp       = errors.New("empty resp")

	errClosed  = errors.New("socket closed")
	errTimeout = errors.New("RPC timeout")
	errLowPort = errors.New("low port")
)

type ContentElement struct {
	Node        enode.ID
	ContentKeys [][]byte
	Contents    [][]byte
}

type ContentEntry struct {
	ContentKey []byte
	Content    []byte
}

type TransientOfferRequest struct {
	Contents []*ContentEntry
}

type TransientOfferRequestWithResult struct {
	Content *ContentEntry
	Result  chan *OfferTrace
}

type PersistOfferRequest struct {
	ContentKeys [][]byte
}

type OfferRequest struct {
	Kind    byte
	Request interface{}
}

type OfferRequestWithNode struct {
	Request *OfferRequest
	Node    *enode.Node

	permit Permit
}

type ContentInfoResp struct {
	Content     []byte
	UtpTransfer bool
}

type traceContentInfoResp struct {
	Node        *enode.Node
	Flag        byte
	Content     any
	UtpTransfer bool
}

// OfferTrace Define the type that can hold any variant
type OfferTrace struct {
	Type        OfferTraceType
	ContentKeys []byte // Only used for Success case
}

type SetPortalProtocolOption func(p *PortalProtocol)

type PortalProtocolConfig struct {
	BootstrapNodes                []*enode.Node
	ListenAddr                    string
	NetRestrict                   *netutil.Netlist
	NodeRadius                    *uint256.Int
	RadiusCacheSize               int
	CapabilitiesCacheSize         int
	EphemeralHeaderCountCacheSize int
	ContentKeyCacheSize           int
	VersionsCacheSize             int
	VersionsCacheTTL              time.Duration
	NodeDBPath                    string
	NAT                           nat.Interface
	clock                         mclock.Clock
	TrustedBlockRoot              []byte
	MaxUtpConnSize                int
}

func DefaultPortalProtocolConfig() *PortalProtocolConfig {
	return &PortalProtocolConfig{
		BootstrapNodes:                make([]*enode.Node, 0),
		ListenAddr:                    ":9009",
		NetRestrict:                   nil,
		RadiusCacheSize:               32 * 1024 * 1024,
		CapabilitiesCacheSize:         32 * 1024 * 1024,
		EphemeralHeaderCountCacheSize: 32 * 1024 * 1024,
		ContentKeyCacheSize:           32 * 1024 * 1024,
		VersionsCacheSize:             versionsCacheSize,
		VersionsCacheTTL:              expirationVersionMinutes,
		NodeDBPath:                    "",
		clock:                         mclock.System{},
		TrustedBlockRoot:              make([]byte, 0),
		MaxUtpConnSize:                DefaultUtpConnSize,
	}
}

type PortalProtocol struct {
	table *Table

	protocolId   string
	protocolName string

	DiscV5         *discover.UDPv5
	localNode      *enode.LocalNode
	Log            log.Logger
	PrivateKey     *ecdsa.PrivateKey
	NetRestrict    *netutil.Netlist
	BootstrapNodes []*enode.Node
	conn           discover.UDPConn

	Utp *UtpTransportService

	validSchemes              enr.IdentityScheme
	radiusCache               *fastcache.Cache
	capabilitiesCache         *fastcache.Cache
	ephemeralHeaderCountCache *fastcache.Cache
	closeCtx                  context.Context
	cancelCloseCtx            context.CancelFunc
	storage                   storage.ContentStorage
	toContentId               func(contentKey []byte) []byte

	contentQueue chan *ContentElement
	offerQueue   chan *OfferRequestWithNode

	portMappingRegister chan *portMapping
	clock               mclock.Clock
	NAT                 nat.Interface

	portalMetrics  *portalMetrics
	PingExtensions pingext.PingExtension

	disableTableInitCheck bool
	currentVersions       protocolVersions
	transferringKeyCache  *fastcache.Cache

	versionsCache cache.Cache[*enode.Node, uint8]
}

func defaultContentIdFunc(contentKey []byte) []byte {
	digest := sha256.Sum256(contentKey)
	return digest[:]
}

func WithDisableTableInitCheckOption(disable bool) SetPortalProtocolOption {
	return func(p *PortalProtocol) {
		p.disableTableInitCheck = disable
	}
}

func NewPortalProtocol(config *PortalProtocolConfig, protocolId ProtocolId, privateKey *ecdsa.PrivateKey, conn discover.UDPConn, localNode *enode.LocalNode, discV5 *discover.UDPv5, utp *UtpTransportService, storage storage.ContentStorage, contentQueue chan *ContentElement, versionsCache cache.Cache[*enode.Node, uint8], setOpts ...SetPortalProtocolOption) (*PortalProtocol, error) {
	// set versions in test
	currentVersions := protocolVersions{}
	err := localNode.Node().Load(&currentVersions)
	if err != nil {
		currentVersions = Versions
	}

	closeCtx, cancelCloseCtx := context.WithCancel(context.Background())

	protocol := &PortalProtocol{
		protocolId:                string(protocolId),
		protocolName:              protocolId.Name(),
		Log:                       log.New("protocol", protocolId.Name()),
		PrivateKey:                privateKey,
		NetRestrict:               config.NetRestrict,
		BootstrapNodes:            config.BootstrapNodes,
		radiusCache:               fastcache.New(config.RadiusCacheSize),
		capabilitiesCache:         fastcache.New(config.CapabilitiesCacheSize),
		ephemeralHeaderCountCache: fastcache.New(config.EphemeralHeaderCountCacheSize),
		versionsCache:             versionsCache,
		closeCtx:                  closeCtx,
		cancelCloseCtx:            cancelCloseCtx,
		localNode:                 localNode,
		validSchemes:              enode.ValidSchemes,
		storage:                   storage,
		toContentId:               defaultContentIdFunc,
		contentQueue:              contentQueue,
		offerQueue:                make(chan *OfferRequestWithNode, offerQueueSize),
		conn:                      conn,
		DiscV5:                    discV5,
		NAT:                       config.NAT,
		clock:                     config.clock,
		Utp:                       utp,
		currentVersions:           currentVersions,
		transferringKeyCache:      fastcache.New(config.ContentKeyCacheSize),
	}

	for _, setOpt := range setOpts {
		setOpt(protocol)
	}

	if metrics.Enabled() {
		protocol.portalMetrics = newPortalMetrics(protocolId.Name())
	}

	switch protocolId.Name() {
	case "history":
		protocol.PingExtensions = HistoryPingExtension{}
	case "state":
		protocol.PingExtensions = StatePingExtension{}
	case "beacon":
		protocol.PingExtensions = BeaconPingExtension{}
	default:
		protocol.PingExtensions = DefaultPingExtension{}
	}

	return protocol, nil
}

func (p *PortalProtocol) Start() error {
	p.setupPortMapping()

	err := p.setupDiscV5AndTable()
	if err != nil {
		return err
	}

	p.DiscV5.RegisterTalkHandler(p.protocolId, p.handleTalkRequest)
	if p.Utp != nil {
		err = p.Utp.Start()
	}
	if err != nil {
		return err
	}

	go p.table.loop()

	for i := 0; i < concurrentOffers; i++ {
		go p.offerWorker()
	}

	return nil
}

func (p *PortalProtocol) Stop() {
	p.cancelCloseCtx()
	p.table.close()
}

// Only used for testing
func (p *PortalProtocol) WaitForClose() <-chan struct{} {
	return p.closeCtx.Done()
}

func (p *PortalProtocol) RoutingTableInfo() [][]string {
	return p.table.nodeIds()
}

func (p *PortalProtocol) AddEnr(n *enode.Node) {
	added := p.table.addFoundNode(n, true)
	if !added {
		p.Log.Warn("add node failed", "id", n.ID(), "ip", n.IPAddr())
		return
	}
	id := n.ID().String()
	p.radiusCache.Set([]byte(id), MaxDistance)
}

func (p *PortalProtocol) Radius() *uint256.Int {
	return p.storage.Radius()
}

func (p *PortalProtocol) setupUDPListening() error {
	laddr := p.conn.LocalAddr().(*net.UDPAddr)
	p.localNode.SetFallbackUDP(laddr.Port)
	p.Log.Debug("UDP listener up", "addr", laddr)

	if !laddr.IP.IsLoopback() && !laddr.IP.IsPrivate() {
		p.portMappingRegister <- &portMapping{
			protocol: "UDP",
			name:     "ethereum portal peer discovery",
			port:     laddr.Port,
		}
	}
	return nil
}

func (p *PortalProtocol) setupDiscV5AndTable() error {
	err := p.setupUDPListening()
	if err != nil {
		return err
	}

	cfg := Config{
		PrivateKey:       p.PrivateKey,
		NetRestrict:      p.NetRestrict,
		Bootnodes:        p.BootstrapNodes,
		Log:              p.Log,
		DisableInitCheck: p.disableTableInitCheck,
	}

	p.table, err = newTable(p, p.localNode.Database(), cfg)
	if err != nil {
		return err
	}

	return nil
}

func (p *PortalProtocol) ping(node *enode.Node) (uint64, error) {
	pong, _, err := p.pingInner(node)
	if err != nil {
		return 0, err
	}

	return pong.EnrSeq, nil
}

func (p *PortalProtocol) pingInner(node *enode.Node) (*Pong, []byte, error) {
	capabilitiesBytes := p.capabilitiesCache.Get(nil, []byte(node.ID().String()))

	var payloadType = pingext.ClientInfo

	if capabilitiesBytes != nil {
		capabilities := &pingext.CapabilitiesPayload{}
		if err := capabilities.UnmarshalSSZ(capabilitiesBytes); err == nil {
			uint16Capabilities := make([]uint16, 0, len(*capabilities))
			for _, value := range *capabilities {
				uint16Capabilities = append(uint16Capabilities, uint16(value))
			}

			if ext := p.PingExtensions.LatestMutuallySupportedBaseExtension(uint16Capabilities); ext != nil {
				switch *ext {
				case pingext.BasicRadius:
					payloadType = pingext.BasicRadius
				case pingext.HistoryRadius:
					payloadType = pingext.HistoryRadius
				}
			}
		}
	}

	payload, err := p.genPayloadByType(payloadType)
	if err != nil {
		return nil, nil, err
	}

	return p.pingInnerWithPayload(node, payloadType, payload)
}

func (p *PortalProtocol) pingInnerWithPayload(node *enode.Node, payloadType uint16, payload []byte) (*Pong, []byte, error) {
	enrSeq := p.Self().Seq()
	pingRequest := &Ping{
		EnrSeq:      enrSeq,
		PayloadType: payloadType,
		Payload:     payload,
	}

	p.Log.Trace(">> PING/"+p.protocolName, "ip", p.Self().IP().String(), "source", p.Self().ID(), "target", node.ID(), "ping", pingRequest)
	if metrics.Enabled() {
		p.portalMetrics.messagesSentPing.Mark(1)
	}
	pingRequestBytes, err := pingRequest.MarshalSSZ()
	if err != nil {
		return nil, nil, err
	}

	talkRequestBytes := make([]byte, 1+len(pingRequestBytes))
	talkRequestBytes[0] = PING
	copy(talkRequestBytes[1:], pingRequestBytes)
	talkResp, err := p.DiscV5.TalkRequest(node, p.protocolId, talkRequestBytes)

	if err != nil {
		return nil, nil, err
	}

	p.Log.Trace("<< PONG/"+p.protocolName, "source", p.Self().ID(), "target", node.ID(), "res", talkResp)
	if metrics.Enabled() {
		p.portalMetrics.messagesReceivedPong.Mark(1)
	}

	return p.processPong(node, talkResp)
}

func (p *PortalProtocol) genPayloadByType(payloadType uint16) ([]byte, error) {
	radiusBytes, err := p.Radius().MarshalSSZ()
	if err != nil {
		return nil, err
	}
	var data []byte
	switch payloadType {
	case pingext.ClientInfo:
		payload := pingext.NewClientInfoAndCapabilitiesPayload(radiusBytes, p.PingExtensions.Extensions())

		data, err = payload.MarshalSSZ()
		if err != nil {
			return nil, err
		}

	case pingext.BasicRadius:
		payload := pingext.NewBasicRadiusPayload(radiusBytes)
		data, err = payload.MarshalSSZ()
		if err != nil {
			return nil, err
		}
	case pingext.HistoryRadius:
		payload := pingext.NewHistoryRadiusPayload(radiusBytes, 0)
		data, err = payload.MarshalSSZ()
		if err != nil {
			return nil, err
		}
	default:
		return nil, pingext.ErrPayloadTypeIsNotSupported{}
	}
	return data, nil
}

func (p *PortalProtocol) findNodes(node *enode.Node, distances []uint) ([]*enode.Node, error) {
	if p.localNode.ID() == node.ID() {
		return make([]*enode.Node, 0), nil
	}

	encodedDistances := make([][2]byte, len(distances))
	for i, distance := range distances {
		binary.LittleEndian.PutUint16(encodedDistances[i][:], uint16(distance))
	}

	findNodes := &FindNodes{
		Distances: encodedDistances,
	}

	p.Log.Trace(">> FIND_NODES/"+p.protocolName, "protocol", p.protocolName, "id", node.ID(), "findNodes", findNodes)
	if metrics.Enabled() {
		p.portalMetrics.messagesSentFindNodes.Mark(1)
	}
	findNodesBytes, err := findNodes.MarshalSSZ()
	if err != nil {
		return nil, err
	}

	talkRequestBytes := make([]byte, 1+len(findNodesBytes))
	talkRequestBytes[0] = FINDNODES
	copy(talkRequestBytes[1:], findNodesBytes)
	talkResp, err := p.DiscV5.TalkRequest(node, p.protocolId, talkRequestBytes)
	if err != nil {
		return nil, err
	}

	return p.processNodes(node, talkResp, distances)
}

func (p *PortalProtocol) findContent(node *enode.Node, contentKey []byte) (byte, interface{}, error) {
	findContent := &FindContent{
		ContentKey: contentKey,
	}

	p.Log.Trace(">> FIND_CONTENT/"+p.protocolName, "id", node.ID(), "findContent", findContent)
	if metrics.Enabled() {
		p.portalMetrics.messagesSentFindContent.Mark(1)
	}
	findContentBytes, err := findContent.MarshalSSZ()
	if err != nil {
		return 0xff, nil, err
	}

	talkRequestBytes := make([]byte, 1+len(findContentBytes))
	talkRequestBytes[0] = FINDCONTENT
	copy(talkRequestBytes[1:], findContentBytes)
	talkResp, err := p.DiscV5.TalkRequest(node, p.protocolId, talkRequestBytes)
	if err != nil {
		return 0xff, nil, err
	}

	return p.processContent(node, talkResp)
}

func (p *PortalProtocol) offer(node *enode.Node, offerRequest *OfferRequest, permit Permit) ([]byte, error) {
	contentKeys := getContentKeys(offerRequest)

	offer := &Offer{
		ContentKeys: contentKeys,
	}

	p.Log.Trace(">> OFFER/"+p.protocolName, "id", node.ID(), "offer", offer)
	if metrics.Enabled() {
		p.portalMetrics.messagesSentOffer.Mark(1)
	}
	offerBytes, err := offer.MarshalSSZ()
	if err != nil {
		return nil, err
	}

	talkRequestBytes := make([]byte, 1+len(offerBytes))
	talkRequestBytes[0] = OFFER
	copy(talkRequestBytes[1:], offerBytes)
	talkResp, err := p.DiscV5.TalkRequest(node, p.protocolId, talkRequestBytes)
	if err != nil {
		return nil, err
	}

	return p.processOffer(node, talkResp, offerRequest, permit)
}

func (p *PortalProtocol) processOffer(target *enode.Node, resp []byte, request *OfferRequest, permit Permit) ([]byte, error) {
	notStartedUtp := true
	defer func() {
		if notStartedUtp {
			permit.Release()
		}
	}()
	var err error
	if len(resp) == 0 {
		return nil, ErrEmptyResp
	}
	if resp[0] != ACCEPT {
		return nil, fmt.Errorf("invalid accept response")
	}

	p.Log.Info("will process Offer", "id", target.ID(), "ip", target.IP().To4().String(), "port", target.UDP())

	accept, err := p.parseOfferResp(target, resp[1:])
	if err != nil {
		return nil, err
	}

	p.Log.Trace("<< ACCEPT/"+p.protocolName, "id", target.ID(), "accept", accept)
	if metrics.Enabled() {
		p.portalMetrics.messagesReceivedAccept.Mark(1)
	}
	isAdded := p.table.addFoundNode(target, true)
	if isAdded {
		log.Debug("Node added to bucket", "protocol", p.protocolName, "node", target.IP(), "port", target.UDP())
	} else {
		log.Debug("Node added to replacements list", "protocol", p.protocolName, "node", target.IP(), "port", target.UDP())
	}
	var contentKeysNumber int
	switch request.Kind {
	case TransientOfferRequestKind:
		contentKeysNumber = len(request.Request.(*TransientOfferRequest).Contents)
	case TransientOfferRequestWithResultKind:
		contentKeysNumber = 1
	default:
		contentKeysNumber = len(request.Request.(*PersistOfferRequest).ContentKeys)
	}

	var resultChan chan *OfferTrace
	if request.Kind == TransientOfferRequestWithResultKind {
		resultChan = request.Request.(*TransientOfferRequestWithResult).Result
	} else {
		resultChan = nil
	}
	acceptIndices := accept.GetAcceptIndices()
	if accept.GetKeyLength() != contentKeysNumber {
		if resultChan != nil {
			resultChan <- &OfferTrace{
				Type: Failed,
			}
		}
		return nil, fmt.Errorf("accepted content key bitlist has invalid size, expected %d, got %d", contentKeysNumber, accept.GetKeyLength())
	}

	if len(acceptIndices) == 0 {
		if resultChan != nil {
			resultChan <- &OfferTrace{
				Type: Declined,
			}
		}
		p.Log.Debug("trace offer declined", "keys", accept.GetContentKeys())
		return accept.GetContentKeys(), nil
	}
	notStartedUtp = false
	connId := binary.BigEndian.Uint16(accept.GetConnectionId())
	go func(ctx context.Context) {
		defer permit.Release()
		var conn *utp.UtpStream
		for {
			select {
			case <-ctx.Done():
				return
			default:
				contents := make([][]byte, 0, len(acceptIndices))
				var content []byte
				switch request.Kind {
				case TransientOfferRequestKind:
					for _, index := range acceptIndices {
						content = request.Request.(*TransientOfferRequest).Contents[index].Content
						contents = append(contents, content)
					}
				case TransientOfferRequestWithResultKind:
					content = request.Request.(*TransientOfferRequestWithResult).Content.Content
					contents = append(contents, content)
				default:
					for _, index := range acceptIndices {
						contentKey := request.Request.(*PersistOfferRequest).ContentKeys[index]
						contentId := p.toContentId(contentKey)
						if contentId != nil {
							content, err = p.storage.Get(contentKey, contentId)
							if err != nil {
								p.Log.Error("failed to get content from storage", "err", err)
								contents = append(contents, []byte{})
							} else {
								contents = append(contents, content)
							}
						} else {
							contents = append(contents, []byte{})
						}
					}
				}

				var contentsPayload []byte
				contentsPayload = encodeContents(contents)

				connctx, conncancel := context.WithTimeout(ctx, defaultUTPConnectTimeout)
				defer conncancel()
				conn, err = p.Utp.DialWithCid(connctx, target, connId)

				if err != nil {
					if metrics.Enabled() {
						p.portalMetrics.utpOutFailConn.Inc(1)
					}
					if resultChan != nil {
						resultChan <- &OfferTrace{
							Type: Failed,
						}
					}
					p.Log.Error("failed to dial utp connection", "err", err)
					return
				}

				var written int
				writeCtx, writeCancel := context.WithTimeout(ctx, defaultUTPWriteTimeout)
				defer writeCancel()
				written, err = conn.Write(writeCtx, contentsPayload)
				conn.Close()
				if err != nil {
					if metrics.Enabled() {
						p.portalMetrics.utpOutFailWrite.Inc(1)
					}
					if resultChan != nil {
						resultChan <- &OfferTrace{
							Type: Failed,
						}
					}
					p.Log.Error("failed to write to utp connection", "err", err)
					return
				}
				p.Log.Trace(">> CONTENT/"+p.protocolName, "id", target.ID(), "size", written)
				if metrics.Enabled() {
					p.portalMetrics.messagesSentContent.Mark(1)
					p.portalMetrics.utpOutSuccess.Inc(1)
				}
				if resultChan != nil {
					resultChan <- &OfferTrace{
						Type:        Success,
						ContentKeys: accept.GetContentKeys(),
					}
				}
				return
			}
		}
	}(p.closeCtx)

	return accept.GetContentKeys(), nil
}

func (p *PortalProtocol) processContent(target *enode.Node, resp []byte) (byte, interface{}, error) {
	if len(resp) == 0 {
		return 0x00, nil, ErrEmptyResp
	}

	if resp[0] != CONTENT {
		return 0xff, nil, fmt.Errorf("invalid content response")
	}

	p.Log.Info("will process content", "id", target.ID(), "ip", target.IP().To4().String(), "port", target.UDP())

	switch resp[1] {
	case ContentRawSelector:
		content := &Content{}
		err := content.UnmarshalSSZ(resp[2:])
		if err != nil {
			return 0xff, nil, err
		}

		p.Log.Trace("<< CONTENT/"+p.protocolName, "id", target.ID(), "content", content)
		if metrics.Enabled() {
			p.portalMetrics.messagesReceivedContent.Mark(1)
		}
		isAdded := p.table.addFoundNode(target, true)
		if isAdded {
			log.Debug("Node added to bucket", "protocol", p.protocolName, "node", target.IP(), "port", target.UDP())
		} else {
			log.Debug("Node added to replacements list", "protocol", p.protocolName, "node", target.IP(), "port", target.UDP())
		}
		return resp[1], content.Content, nil
	case ContentConnIdSelector:
		connIdMsg := &ConnectionId{}
		err := connIdMsg.UnmarshalSSZ(resp[2:])
		if err != nil {
			return 0xff, nil, err
		}

		p.Log.Trace("<< CONTENT_CONNECTION_ID/"+p.protocolName, "id", target.ID(), "resp", common.Bytes2Hex(resp), "connIdMsg", connIdMsg)
		if metrics.Enabled() {
			p.portalMetrics.messagesReceivedContent.Mark(1)
		}
		isAdded := p.table.addFoundNode(target, true)
		if isAdded {
			log.Debug("Node added to bucket", "protocol", p.protocolName, "node", target.IP(), "port", target.UDP())
		} else {
			log.Debug("Node added to replacements list", "protocol", p.protocolName, "node", target.IP(), "port", target.UDP())
		}
		connctx, conncancel := context.WithTimeout(p.closeCtx, defaultUTPConnectTimeout)
		defer conncancel()
		connId := binary.BigEndian.Uint16(connIdMsg.Id)
		conn, err := p.Utp.DialWithCid(connctx, target, connId)
		if err != nil {
			if metrics.Enabled() {
				p.portalMetrics.utpInFailConn.Inc(1)
			}
			return 0xff, nil, err
		}
		defer conn.Close()
		// Read ALL the data from the connection until EOF and return it
		readCtx, readCancel := context.WithTimeout(p.closeCtx, defaultUTPReadTimeout)
		defer readCancel()
		var data []byte
		n, err := conn.ReadToEOF(readCtx, &data)
		if err != nil {
			if metrics.Enabled() {
				p.portalMetrics.utpInFailRead.Inc(1)
			}
			return 0xff, nil, err
		}
		p.Log.Trace("<< CONTENT/"+p.protocolName, "id", target.ID(), "size", n, "data", data)
		data, err = p.decodeUtpContent(target, data)
		if err != nil {
			if metrics.Enabled() {
				p.portalMetrics.utpOutFailConn.Inc(1)
			}
			return 0xff, nil, err
		}
		if metrics.Enabled() {
			p.portalMetrics.messagesReceivedContent.Mark(1)
			p.portalMetrics.utpInSuccess.Inc(1)
		}
		return resp[1], data, nil
	case ContentEnrsSelector:
		enrs := &Enrs{}
		err := enrs.UnmarshalSSZ(resp[2:])

		if err != nil {
			return 0xff, nil, err
		}

		p.Log.Trace("<< CONTENT_ENRS/"+p.protocolName, "id", target.ID(), "enrs.size", len(enrs.Enrs))
		if metrics.Enabled() {
			p.portalMetrics.messagesReceivedContent.Mark(1)
		}
		isAdded := p.table.addFoundNode(target, true)
		if isAdded {
			log.Debug("Node added to bucket", "protocol", p.protocolName, "node", target.IP(), "port", target.UDP())
		} else {
			log.Debug("Node added to replacements list", "protocol", p.protocolName, "node", target.IP(), "port", target.UDP())
		}
		nodes := p.filterNodes(target, enrs.Enrs, nil)
		return resp[1], nodes, nil
	default:
		return 0xff, nil, fmt.Errorf("invalid content response")
	}
}

func (p *PortalProtocol) processNodes(target *enode.Node, resp []byte, distances []uint) ([]*enode.Node, error) {
	if len(resp) == 0 {
		return nil, ErrEmptyResp
	}

	if resp[0] != NODES {
		return nil, fmt.Errorf("invalid nodes response")
	}

	nodesResp := &Nodes{}
	err := nodesResp.UnmarshalSSZ(resp[1:])
	if err != nil {
		return nil, err
	}

	isAdded := p.table.addFoundNode(target, true)
	if isAdded {
		log.Debug("Node added to bucket", "protocol", p.protocolName, "node", target.IP(), "port", target.UDP())
	} else {
		log.Debug("Node added to replacements list", "protocol", p.protocolName, "node", target.IP(), "port", target.UDP())
	}
	nodes := p.filterNodes(target, nodesResp.Enrs, distances)

	return nodes, nil
}

func (p *PortalProtocol) filterNodes(target *enode.Node, enrs [][]byte, distances []uint) []*enode.Node {
	var (
		seen     = make(map[enode.ID]struct{})
		err      error
		verified = 0
		n        *enode.Node
	)

	nodes := make([]*enode.Node, 0, len(enrs))
	for _, b := range enrs {
		record := &enr.Record{}
		err = rlp.DecodeBytes(b, record)
		if err != nil {
			p.Log.Error("Invalid record in nodes response", "id", target.ID(), "err", err)
			continue
		}
		n, err = p.verifyResponseNode(target, record, distances, seen)
		if err != nil {
			p.Log.Error("Invalid record in nodes response", "id", target.ID(), "err", err)
			continue
		}
		verified++
		nodes = append(nodes, n)
	}

	p.Log.Trace("<< NODES/"+p.protocolName, "id", target.ID(), "total", len(enrs), "verified", verified, "nodes.size", len(nodes))
	if metrics.Enabled() {
		p.portalMetrics.messagesReceivedNodes.Mark(1)
	}
	return nodes
}

func (p *PortalProtocol) processPong(target *enode.Node, resp []byte) (*Pong, []byte, error) {
	if len(resp) == 0 {
		return nil, nil, ErrEmptyResp
	}
	if resp[0] != PONG {
		return nil, nil, fmt.Errorf("invalid pong response")
	}
	pong := &Pong{}
	err := pong.UnmarshalSSZ(resp[1:])
	if err != nil {
		return nil, nil, err
	}

	p.Log.Trace("<< PONG_RESPONSE/"+p.protocolName, "id", target.ID(), "pong", pong)
	if metrics.Enabled() {
		p.portalMetrics.messagesReceivedPong.Mark(1)
	}

	isAdded := p.table.addFoundNode(target, true)
	if isAdded {
		log.Debug("Node added to bucket", "protocol", p.protocolName, "node", target.IP(), "port", target.UDP())
	} else {
		log.Debug("Node added to replacements list", "protocol", p.protocolName, "node", target.IP(), "port", target.UDP())
	}

	dataRadius, err := p.processPongPayload(target, pong)
	if err != nil {
		return nil, nil, err
	}

	return pong, dataRadius, nil
}

func (p *PortalProtocol) processPongPayload(target *enode.Node, pong *Pong) ([]byte, error) {
	if n := p.table.getNodeOrReplacement(target.ID()); n != nil {
		if n.Seq() < pong.EnrSeq {
			// Update the node in the routing table
			_, err := p.RequestENR(n)
			if err != nil {
				p.Log.Error("failed to request ENR", "err", err)
			}
		}

		if !p.PingExtensions.IsSupported(pong.PayloadType) {
			return nil, pingext.ErrPayloadTypeIsNotSupported{}
		}

		switch pong.PayloadType {
		case pingext.ClientInfo:
			payload := &pingext.ClientInfoAndCapabilitiesPayload{}
			err := payload.UnmarshalSSZ(pong.Payload)
			if err != nil {
				return nil, err
			}
			p.processClientInfo(target.ID(), payload)
		case pingext.BasicRadius:
			payload := &pingext.BasicRadiusPayload{}
			err := payload.UnmarshalSSZ(pong.Payload)
			if err != nil {
				return nil, err
			}
			p.processBasicRadius(target.ID(), payload)
		case pingext.HistoryRadius:
			payload := &pingext.HistoryRadiusPayload{}
			err := payload.UnmarshalSSZ(pong.Payload)
			if err != nil {
				return nil, err
			}
			p.processHistoryRadius(target.ID(), payload)
		default:
			return nil, pingext.ErrPayloadTypeIsNotSupported{}
		}

		radiusBytes := p.radiusCache.Get(nil, []byte(target.ID().String()))
		return radiusBytes, nil
	}
	return nil, nil
}

func (p *PortalProtocol) handleTalkRequest(node *enode.Node, addr *net.UDPAddr, msg []byte) []byte {
	if n := p.table.getNode(node.ID()); n == nil {
		p.table.addInboundNode(node)
	}

	msgCode := msg[0]

	switch msgCode {
	case PING:
		pingRequest := &Ping{}
		err := pingRequest.UnmarshalSSZ(msg[1:])
		if err != nil {
			p.Log.Error("failed to unmarshal ping request", "err", err)
			return nil
		}

		p.Log.Trace("<< PING/"+p.protocolName, "protocol", p.protocolName, "source", node.ID(), "pingRequest", pingRequest)
		if metrics.Enabled() {
			p.portalMetrics.messagesReceivedPing.Mark(1)
		}
		resp, err := p.handlePing(node.ID(), pingRequest)
		if err != nil {
			p.Log.Error("failed to handle ping request", "err", err)
			return nil
		}

		return resp
	case FINDNODES:
		findNodesRequest := &FindNodes{}
		err := findNodesRequest.UnmarshalSSZ(msg[1:])
		if err != nil {
			p.Log.Error("failed to unmarshal find nodes request", "err", err)
			return nil
		}

		p.Log.Trace("<< FIND_NODES/"+p.protocolName, "protocol", p.protocolName, "source", node.ID(), "findNodesRequest", findNodesRequest)
		if metrics.Enabled() {
			p.portalMetrics.messagesReceivedFindNodes.Mark(1)
		}
		resp, err := p.handleFindNodes(addr, findNodesRequest)
		if err != nil {
			p.Log.Error("failed to handle find nodes request", "err", err)
			return nil
		}

		return resp
	case FINDCONTENT:
		findContentRequest := &FindContent{}
		err := findContentRequest.UnmarshalSSZ(msg[1:])
		if err != nil {
			p.Log.Error("failed to unmarshal find content request", "err", err)
			return nil
		}

		p.Log.Trace("<< FIND_CONTENT/"+p.protocolName, "protocol", p.protocolName, "source", node.ID(), "findContentRequest", findContentRequest)
		if metrics.Enabled() {
			p.portalMetrics.messagesReceivedFindContent.Mark(1)
		}
		resp, err := p.handleFindContent(node, addr, findContentRequest)
		if err != nil {
			p.Log.Error("failed to handle find content request", "err", err)
			return nil
		}

		return resp
	case OFFER:
		offerRequest := &Offer{}
		err := offerRequest.UnmarshalSSZ(msg[1:])
		if err != nil {
			p.Log.Error("failed to unmarshal offer request", "err", err)
			return nil
		}

		p.Log.Trace("<< OFFER/"+p.protocolName, "protocol", p.protocolName, "source", node.ID(), "offerRequest", offerRequest)
		if metrics.Enabled() {
			p.portalMetrics.messagesReceivedOffer.Mark(1)
		}
		resp, err := p.handleOffer(node, addr, offerRequest)
		if err != nil {
			p.Log.Error("failed to handle offer request", "err", err)
			return nil
		}

		return resp
	}

	return nil
}

func (p *PortalProtocol) handlePing(id enode.ID, ping *Ping) ([]byte, error) {
	var pong Pong
	var err error
	if !p.PingExtensions.IsSupported(ping.PayloadType) {
		errPayload := pingext.GetErrorPayloadBytes(pingext.ErrorNotSupported)
		pong = p.createPong(pingext.Error, errPayload)
	} else {
		switch ping.PayloadType {
		case pingext.ClientInfo:
			payload := &pingext.ClientInfoAndCapabilitiesPayload{}
			err = payload.UnmarshalSSZ(ping.Payload)
			if err != nil {
				errPayload := pingext.GetErrorPayloadBytes(pingext.ErrorDecodePayload)
				pong = p.createPong(pingext.Error, errPayload)
				break
			}
			// Process the ping asynchronously. Any errors occurring in processPing
			// will not affect the synchronous pong response. This is an intentional
			// design choice to decouple ping processing from the response.
			go p.processPing(id, ping, payload)
			pong, err = p.handleClientInfo()
			if err != nil {
				return nil, err
			}
		case pingext.BasicRadius:
			payload := &pingext.BasicRadiusPayload{}
			err = payload.UnmarshalSSZ(ping.Payload)
			if err != nil {
				errPayload := pingext.GetErrorPayloadBytes(pingext.ErrorDecodePayload)
				pong = p.createPong(pingext.Error, errPayload)
				break
			}
			go p.processPing(id, ping, payload)
			pong, err = p.handleBasicRadius()
			if err != nil {
				return nil, err
			}
		case pingext.HistoryRadius:
			payload := &pingext.HistoryRadiusPayload{}
			err = payload.UnmarshalSSZ(ping.Payload)
			if err != nil {
				errPayload := pingext.GetErrorPayloadBytes(pingext.ErrorDecodePayload)
				pong = p.createPong(pingext.Error, errPayload)
				break
			}
			go p.processPing(id, ping, payload)
			pong, err = p.handleHistoryRadius()
			if err != nil {
				return nil, err
			}
		case pingext.Error:
			errBytes := pingext.GetErrorPayloadBytes(pingext.ErrorSystemError)
			pong = p.createPong(ping.PayloadType, errBytes)
		}
	}
	p.Log.Trace(">> PONG/"+p.protocolName, "protocol", p.protocolName, "ping_type", ping.PayloadType, "source", id, "pong", pong)
	if metrics.Enabled() {
		p.portalMetrics.messagesSentPong.Mark(1)
	}
	pongBytes, err := pong.MarshalSSZ()

	if err != nil {
		return nil, err
	}

	talkRespBytes := make([]byte, 1+len(pongBytes))
	talkRespBytes[0] = PONG
	copy(talkRespBytes[1:], pongBytes)

	return talkRespBytes, nil
}

func (p *PortalProtocol) processPing(id enode.ID, ping *Ping, payload interface{}) {
	if n := p.table.getNodeOrReplacement(id); n != nil {
		if n.Seq() < ping.EnrSeq {
			// Update the node in the routing table
			_, err := p.RequestENR(n)
			if err != nil {
				p.Log.Error("failed to request ENR", "err", err)
			}
		}

		if !p.PingExtensions.IsSupported(ping.PayloadType) {
			p.Log.Error("unsupported ping payload type", "type", ping.PayloadType)
			return
		}

		switch pld := payload.(type) {
		case *pingext.ClientInfoAndCapabilitiesPayload:
			p.processClientInfo(id, pld)
		case *pingext.BasicRadiusPayload:
			p.processBasicRadius(id, pld)
		case *pingext.HistoryRadiusPayload:
			p.processHistoryRadius(id, pld)
		default:
			p.Log.Error("unknown payload type", "type", ping.PayloadType)
		}
	}
}

// updateRadiusCacheIfNeeded compares the incoming radius with the cached value and updates the cache if necessary.
// It returns true if the cache was updated, false otherwise.
func (p *PortalProtocol) updateRadiusCacheIfNeeded(id enode.ID, nodeIdBytes []byte, incomingRadius zrntcommon.Root) bool {
	cachedDataRadiusBytes := p.radiusCache.Get(nil, nodeIdBytes)

	radiusMatch := false
	if cachedDataRadiusBytes != nil && len(cachedDataRadiusBytes) == common.HashLength {
		if bytes.Equal(incomingRadius[:], cachedDataRadiusBytes) {
			radiusMatch = true
		}
	}

	if !radiusMatch {
		p.Log.Debug("DataRadius mismatch or cache miss, updating radius cache", "node", id, "payloadRadius", incomingRadius, "cachedRadiusBytes", hexutil.Encode(cachedDataRadiusBytes))
		p.radiusCache.Set(nodeIdBytes, incomingRadius[:])
		return true
	}
	return false
}

func (p *PortalProtocol) processClientInfo(id enode.ID, payload *pingext.ClientInfoAndCapabilitiesPayload) {
	nodeIdStr := id.String()
	nodeIdBytes := []byte(nodeIdStr)
	// --- Compare and Update Radius ---
	updated := p.updateRadiusCacheIfNeeded(id, nodeIdBytes, payload.DataRadius)

	// --- Compare and Update Capabilities ---
	incomingCapBytes, err := payload.Capabilities.MarshalSSZ()
	if err != nil {
		p.Log.Error("Failed to marshal incoming capabilities", "node", id, "err", err)
	} else {
		cachedCapBytes := p.capabilitiesCache.Get(nil, nodeIdBytes)

		capabilitiesMatch := false
		if cachedCapBytes != nil {
			if bytes.Equal(incomingCapBytes, cachedCapBytes) {
				capabilitiesMatch = true
			}
		}

		if !capabilitiesMatch {
			p.Log.Debug("Capabilities mismatch or cache miss, updating capabilities cache", "node", id, "payloadCaps", hexutil.Encode(incomingCapBytes), "cachedCaps", hexutil.Encode(cachedCapBytes))
			p.capabilitiesCache.Set(nodeIdBytes, incomingCapBytes)
			updated = true
		}
	}

	if !updated {
		p.Log.Trace("Radius and Capabilities match cached values", "node", id)
	}
}

func (p *PortalProtocol) processBasicRadius(id enode.ID, payload *pingext.BasicRadiusPayload) {
	nodeIdStr := id.String()
	nodeIdBytes := []byte(nodeIdStr)

	// --- Compare and Update Radius ---
	updated := p.updateRadiusCacheIfNeeded(id, nodeIdBytes, payload.DataRadius)

	if !updated {
		p.Log.Trace("Basic Radius match cached values", "node", id)
	}
}

func (p *PortalProtocol) processHistoryRadius(id enode.ID, payload *pingext.HistoryRadiusPayload) {
	nodeIdStr := id.String()
	nodeIdBytes := []byte(nodeIdStr)

	// --- Compare and Update Radius ---
	updated := p.updateRadiusCacheIfNeeded(id, nodeIdBytes, payload.DataRadius)

	// --- Compare and Update Ephemeral Header Count ---
	cachedEphemeralHeaderCountBytes := p.ephemeralHeaderCountCache.Get(nil, nodeIdBytes)
	ephemeralHeaderCountMatch := false

	if cachedEphemeralHeaderCountBytes != nil {
		cachedView := new(view.Uint16View)
		err := cachedView.Decode(cachedEphemeralHeaderCountBytes)
		if err == nil {
			if uint16(*cachedView) == uint16(payload.EphemeralHeaderCount) {
				ephemeralHeaderCountMatch = true
			}
		} else {
			p.Log.Warn("Failed to decode cached ephemeral header count", "node", id, "err", err)
		}
	}

	if !ephemeralHeaderCountMatch {
		p.Log.Debug("Ephemeral header count mismatch or cache miss, updating cache", "node", id, "payloadCount", payload.EphemeralHeaderCount)
		newCountBytes, err := payload.EphemeralHeaderCount.Encode()
		if err != nil {
			p.Log.Error("Failed to marshal new ephemeral header count", "node", id, "err", err)
		} else {
			p.ephemeralHeaderCountCache.Set(nodeIdBytes, newCountBytes)
			updated = true
		}
	}

	if !updated {
		p.Log.Trace("History Radius match cached values", "node", id)
	}
}

func (p *PortalProtocol) handleClientInfo() (Pong, error) {
	radiusBytes, err := p.storage.Radius().MarshalSSZ()
	if err != nil {
		return Pong{}, err
	}
	pongPayload := pingext.NewClientInfoAndCapabilitiesPayload(radiusBytes, p.PingExtensions.Extensions())
	pongPayloadBytes, err := pongPayload.MarshalSSZ()
	if err != nil {
		return Pong{}, err
	}
	return p.createPong(pingext.ClientInfo, pongPayloadBytes), nil
}

func (p *PortalProtocol) handleBasicRadius() (Pong, error) {
	radius, err := p.Radius().MarshalSSZ()
	if err != nil {
		return Pong{}, err
	}
	return p.createPong(pingext.BasicRadius, radius), nil
}

func (p *PortalProtocol) handleHistoryRadius() (Pong, error) {
	radius, err := p.Radius().MarshalSSZ()
	if err != nil {
		return Pong{}, err
	}
	payload := pingext.NewHistoryRadiusPayload(radius, 0)
	payloadBytes, err := payload.MarshalSSZ()
	if err != nil {
		return Pong{}, err
	}
	return p.createPong(pingext.HistoryRadius, payloadBytes), nil
}

func (p *PortalProtocol) createPong(payloadType uint16, payload []byte) Pong {
	return Pong{
		EnrSeq:      p.Self().Seq(),
		PayloadType: payloadType,
		Payload:     payload,
	}
}

func (p *PortalProtocol) handleFindNodes(fromAddr *net.UDPAddr, request *FindNodes) ([]byte, error) {
	distances := make([]uint, len(request.Distances))
	for i, distance := range request.Distances {
		distances[i] = uint(ssz.UnmarshallUint16(distance[:]))
	}

	nodes := p.collectTableNodes(fromAddr.IP, distances, portalFindnodesResultLimit)

	nodesOverhead := 1 + 1 + 4 // msg id + total + container offset
	maxPayloadSize := maxPacketSize - talkRespOverhead - nodesOverhead
	enrOverhead := 4 // per added ENR, 4 bytes offset overhead

	enrs := p.truncateNodes(nodes, maxPayloadSize, enrOverhead)

	nodesMsg := &Nodes{
		// https://github.com/ethereum/portal-network-specs/blob/master/portal-wire-protocol.md
		// total: The total number of Nodes response messages being sent. Currently fixed to only 1 response message.
		Total: 1,
		Enrs:  enrs,
	}

	p.Log.Trace(">> NODES/"+p.protocolName, "protocol", p.protocolName, "source", fromAddr, "nodes.size", len(nodesMsg.Enrs))
	if metrics.Enabled() {
		p.portalMetrics.messagesSentNodes.Mark(1)
	}
	nodesMsgBytes, err := nodesMsg.MarshalSSZ()
	if err != nil {
		return nil, err
	}

	talkRespBytes := make([]byte, 0, len(nodesMsgBytes)+1)
	talkRespBytes = append(talkRespBytes, NODES)
	talkRespBytes = append(talkRespBytes, nodesMsgBytes...)

	return talkRespBytes, nil
}

func (p *PortalProtocol) handleFindContent(n *enode.Node, addr *net.UDPAddr, request *FindContent) ([]byte, error) {
	contentOverhead := 1 + 1 // msg id + SSZ Union selector
	maxPayloadSize := maxPacketSize - talkRespOverhead - contentOverhead
	enrOverhead := 4 // per added ENR, 4 bytes offset overhead
	var err error
	contentKey := request.ContentKey
	contentId := p.toContentId(contentKey)
	if contentId == nil {
		return nil, ErrNilContentKey
	}

	var content []byte
	content, err = p.storage.Get(contentKey, contentId)
	if err != nil && !errors.Is(err, ErrContentNotFound) {
		return nil, err
	}

	if errors.Is(err, ErrContentNotFound) {
		closestNodes := p.findNodesCloseToContent(contentId, portalFindnodesResultLimit)
		for i, closeNode := range closestNodes {
			if closeNode.ID() == n.ID() {
				closestNodes = append(closestNodes[:i], closestNodes[i+1:]...)
				break
			}
		}

		enrs := p.truncateNodes(closestNodes, maxPayloadSize, enrOverhead)
		// TODO fix when no content and no enrs found
		if len(enrs) == 0 {
			enrs = nil
		}

		enrsMsg := &Enrs{
			Enrs: enrs,
		}

		p.Log.Trace(">> CONTENT_ENRS/"+p.protocolName, "protocol", p.protocolName, "source", addr, "enrs.size", len(enrsMsg.Enrs))
		if metrics.Enabled() {
			p.portalMetrics.messagesSentContent.Mark(1)
		}
		var enrsMsgBytes []byte
		enrsMsgBytes, err = enrsMsg.MarshalSSZ()
		if err != nil {
			return nil, err
		}

		contentMsgBytes := make([]byte, 0, len(enrsMsgBytes)+1)
		contentMsgBytes = append(contentMsgBytes, ContentEnrsSelector)
		contentMsgBytes = append(contentMsgBytes, enrsMsgBytes...)

		talkRespBytes := make([]byte, 0, len(contentMsgBytes)+1)
		talkRespBytes = append(talkRespBytes, CONTENT)
		talkRespBytes = append(talkRespBytes, contentMsgBytes...)

		return talkRespBytes, nil
	} else if len(content) <= maxPayloadSize {
		rawContentMsg := &Content{
			Content: content,
		}

		p.Log.Trace(">> CONTENT_RAW/"+p.protocolName, "protocol", p.protocolName, "source", addr, "content", rawContentMsg)
		if metrics.Enabled() {
			p.portalMetrics.messagesSentContent.Mark(1)
		}

		var rawContentMsgBytes []byte
		rawContentMsgBytes, err = rawContentMsg.MarshalSSZ()
		if err != nil {
			return nil, err
		}

		contentMsgBytes := make([]byte, 0, len(rawContentMsgBytes)+1)
		contentMsgBytes = append(contentMsgBytes, ContentRawSelector)
		contentMsgBytes = append(contentMsgBytes, rawContentMsgBytes...)

		talkRespBytes := make([]byte, 0, len(contentMsgBytes)+1)
		talkRespBytes = append(talkRespBytes, CONTENT)
		talkRespBytes = append(talkRespBytes, contentMsgBytes...)

		return talkRespBytes, nil
	} else {
		connectionId := p.Utp.CidWithAddr(n, addr, false)

		go func(bctx context.Context, connId *utp.ConnectionId) {
			var conn *utp.UtpStream
			var connectCtx context.Context
			var cancel context.CancelFunc
			for {
				select {
				case <-bctx.Done():
					return
				default:
					p.Log.Debug("will accept find content conn from: ", "nodeId", n.ID().String(), "source", addr, "connId", connId)
					connectCtx, cancel = context.WithTimeout(bctx, defaultUTPConnectTimeout)
					defer cancel()
					conn, err = p.Utp.AcceptWithCid(connectCtx, connectionId)
					if err != nil {
						if metrics.Enabled() {
							p.portalMetrics.utpOutFailConn.Inc(1)
						}
						p.Log.Error("failed to accept utp connection for handle find content", "connId", connectionId.Send, "err", err)
						return
					}

					writeCtx, writeCancel := context.WithTimeout(bctx, defaultUTPWriteTimeout)
					defer writeCancel()
					content, err = p.encodeUtpContent(n, content)
					if err != nil {
						if metrics.Enabled() {
							p.portalMetrics.utpOutFailConn.Inc(1)
						}
						p.Log.Error("encode utp content failed", "err", err)
						return
					}
					var n int
					n, err = conn.Write(writeCtx, content)
					conn.Close()
					if err != nil {
						if metrics.Enabled() {
							p.portalMetrics.utpOutFailWrite.Inc(1)
						}
						p.Log.Error("failed to write content to utp connection", "err", err)
						return
					}

					if metrics.Enabled() {
						p.portalMetrics.utpOutSuccess.Inc(1)
					}
					p.Log.Trace("wrote content size to utp connection", "n", n)
					return
				}
			}
		}(p.closeCtx, connectionId)

		idBuffer := make([]byte, 2)
		binary.BigEndian.PutUint16(idBuffer, connectionId.Send)
		connIdMsg := &ConnectionId{
			Id: idBuffer,
		}

		p.Log.Trace(">> CONTENT_CONNECTION_ID/"+p.protocolName, "protocol", p.protocolName, "source", addr, "connId", connIdMsg)
		if metrics.Enabled() {
			p.portalMetrics.messagesSentContent.Mark(1)
		}
		var connIdMsgBytes []byte
		connIdMsgBytes, err = connIdMsg.MarshalSSZ()
		if err != nil {
			return nil, err
		}

		contentMsgBytes := make([]byte, 0, len(connIdMsgBytes)+1)
		contentMsgBytes = append(contentMsgBytes, ContentConnIdSelector)
		contentMsgBytes = append(contentMsgBytes, connIdMsgBytes...)

		talkRespBytes := make([]byte, 0, len(contentMsgBytes)+1)
		talkRespBytes = append(talkRespBytes, CONTENT)
		talkRespBytes = append(talkRespBytes, contentMsgBytes...)

		return talkRespBytes, nil
	}
}

func (p *PortalProtocol) handleOffer(node *enode.Node, addr *net.UDPAddr, request *Offer) ([]byte, error) {
	var err error
	version, err := p.getOrStoreHighestVersion(node)
	if err != nil {
		return nil, err
	}
	accept, contentKeys, err := p.filterContentKeys(request, version)
	if err != nil {
		return nil, err
	}
	idBuffer := make([]byte, 2)
	idValue := uint16(0)
	if len(contentKeys) > 0 {
		permit, getPermit := p.Utp.GetInboundPermit()
		if !getPermit {
			p.Log.Debug("utp rate limited")
			if acceptV1, isV1 := accept.(*AcceptV1); isV1 {
				keysLen := len(acceptV1.ContentKeys)
				var limitRate = make([]uint8, keysLen)
				for i := 0; i < keysLen; i++ {
					limitRate[i] = uint8(RateLimited)
				}
				acceptV1.ContentKeys = limitRate
			}
		} else {
			connectionId := p.Utp.CidWithAddr(node, addr, false)
			go func(bctx context.Context, connId *utp.ConnectionId, releasePermit Permit) {
				defer releasePermit.Release()
				defer p.deleteTransferringContentKeys(contentKeys)
				var conn *utp.UtpStream
				for {
					select {
					case <-bctx.Done():
						return
					default:
						p.cacheTransferringKeys(contentKeys)
						p.Log.Debug("will accept offer conn from: ", "source", addr, "connId", connId)
						connectCtx, cancel := context.WithTimeout(bctx, defaultUTPConnectTimeout)
						defer cancel()
						conn, err = p.Utp.AcceptWithCid(connectCtx, connectionId)
						if err != nil {
							if metrics.Enabled() {
								p.portalMetrics.utpInFailConn.Inc(1)
							}
							p.Log.Error("failed to accept utp connection for handle offer", "connId", connectionId.Send, "err", err)
							return
						}

						// Read ALL the data from the connection until EOF and return it
						var data []byte
						var n int
						readCtx, readCancel := context.WithTimeout(bctx, defaultUTPReadTimeout)
						defer readCancel()
						n, err = conn.ReadToEOF(readCtx, &data)
						conn.Close()
						// release permit fast
						releasePermit.Release()
						p.Log.Trace("<< OFFER_CONTENT/"+p.protocolName, "id", node.ID(), "size", n, "data", data)
						if metrics.Enabled() {
							p.portalMetrics.messagesReceivedContent.Mark(1)
						}

						err = p.handleOfferedContents(node.ID(), contentKeys, data)
						if err != nil {
							p.Log.Error("failed to handle offered Contents", "err", err)
							return
						}

						if metrics.Enabled() {
							p.portalMetrics.utpInSuccess.Inc(1)
						}
					}
				}
			}(p.closeCtx, connectionId, permit)
			idValue = connectionId.Send
		}
	}
	binary.BigEndian.PutUint16(idBuffer, idValue)
	accept.SetConnectionId(idBuffer)
	acceptMsgBytes, err := accept.MarshalSSZ()
	if err != nil {
		return nil, err
	}

	p.Log.Trace(">> ACCEPT/"+p.protocolName, "protocol", p.protocolName, "source", addr, "accept", acceptMsgBytes)
	if metrics.Enabled() {
		p.portalMetrics.messagesSentAccept.Mark(1)
	}

	talkRespBytes := make([]byte, 0, len(acceptMsgBytes)+1)
	talkRespBytes = append(talkRespBytes, ACCEPT)
	talkRespBytes = append(talkRespBytes, acceptMsgBytes...)

	return talkRespBytes, nil
}

func (p *PortalProtocol) handleOfferedContents(id enode.ID, keys [][]byte, payload []byte) error {
	contents, err := decodeContents(payload)
	if err != nil {
		if metrics.Enabled() {
			p.portalMetrics.contentDecodedFalse.Inc(1)
		}
		return err
	}

	keyLen := len(keys)
	contentLen := len(contents)
	if keyLen != contentLen {
		if metrics.Enabled() {
			p.portalMetrics.contentDecodedFalse.Inc(1)
		}
		return fmt.Errorf("content keys len %d doesn't match content values len %d", keyLen, contentLen)
	}

	contentElement := &ContentElement{
		Node:        id,
		ContentKeys: keys,
		Contents:    contents,
	}

	select {
	case p.contentQueue <- contentElement:
		if metrics.Enabled() {
			p.portalMetrics.contentDecodedTrue.Inc(1)
		}
	default:
		if metrics.Enabled() {
			p.portalMetrics.contentDiscard.Inc(1)
		}
	}
	return nil
}

func (p *PortalProtocol) Self() *enode.Node {
	return p.localNode.Node()
}

func (p *PortalProtocol) RequestENR(n *enode.Node) (*enode.Node, error) {
	nodes, err := p.findNodes(n, []uint{0})
	if err != nil {
		return nil, err
	}
	if len(nodes) != 1 {
		return nil, fmt.Errorf("%d nodes in response for distance zero", len(nodes))
	}
	return nodes[0], nil
}

func (p *PortalProtocol) verifyResponseNode(sender *enode.Node, r *enr.Record, distances []uint, seen map[enode.ID]struct{}) (*enode.Node, error) {
	n, err := enode.New(p.validSchemes, r)
	if err != nil {
		return nil, err
	}
	if err = netutil.CheckRelayIP(sender.IP(), n.IP()); err != nil {
		return nil, err
	}
	if p.NetRestrict != nil && !p.NetRestrict.Contains(n.IP()) {
		return nil, errors.New("not contained in netrestrict list")
	}
	if n.UDP() <= 1024 {
		return nil, errLowPort
	}
	if distances != nil {
		nd := enode.LogDist(sender.ID(), n.ID())
		if !slices.Contains(distances, uint(nd)) {
			return nil, errors.New("does not match any requested distance")
		}
	}
	if _, ok := seen[n.ID()]; ok {
		return nil, fmt.Errorf("duplicate record")
	}
	seen[n.ID()] = struct{}{}
	return n, nil
}

// LookupRandom looks up a random target.
// This is needed to satisfy the transport interface.
func (p *PortalProtocol) lookupRandom() []*enode.Node {
	return p.newRandomLookup(p.closeCtx).run()
}

// LookupSelf looks up our own node ID.
// This is needed to satisfy the transport interface.
func (p *PortalProtocol) lookupSelf() []*enode.Node {
	return p.newLookup(p.closeCtx, p.Self().ID()).run()
}

func (p *PortalProtocol) newRandomLookup(ctx context.Context) *lookup {
	var target enode.ID
	_, _ = crand.Read(target[:])
	return p.newLookup(ctx, target)
}

func (p *PortalProtocol) newLookup(ctx context.Context, target enode.ID) *lookup {
	return newLookup(ctx, p.table, target, func(n *enode.Node) ([]*enode.Node, error) {
		return p.lookupWorker(n, target)
	})
}

// lookupWorker performs FINDNODE calls against a single node during lookup.
func (p *PortalProtocol) lookupWorker(destNode *enode.Node, target enode.ID) ([]*enode.Node, error) {
	var (
		dists = lookupDistances(target, destNode.ID())
		nodes = nodesByDistance{target: target}
		err   error
	)
	var r []*enode.Node

	r, err = p.findNodes(destNode, dists)
	if errors.Is(err, errClosed) {
		return nil, err
	}
	for _, n := range r {
		if n.ID() != p.Self().ID() {
			isAdded := p.table.addFoundNode(n, false)
			if isAdded {
				log.Debug("Node added to bucket", "protocol", p.protocolName, "node", n.IP(), "port", n.UDP())
			} else {
				log.Debug("Node added to replacements list", "protocol", p.protocolName, "node", n.IP(), "port", n.UDP())
			}
			nodes.push(n, portalFindnodesResultLimit)
		}
	}
	return nodes.entries, err
}

func (p *PortalProtocol) offerWorker() {
	for {
		select {
		case <-p.closeCtx.Done():
			return
		case offerRequestWithNode := <-p.offerQueue:
			p.Log.Trace("offerWorker", "offerRequestWithNode", offerRequestWithNode)
			_, err := p.offer(offerRequestWithNode.Node, offerRequestWithNode.Request, offerRequestWithNode.permit)
			if err != nil {
				p.Log.Error("failed to offer", "err", err)
			}
		}
	}
}

func (p *PortalProtocol) truncateNodes(nodes []*enode.Node, maxSize int, enrOverhead int) [][]byte {
	res := make([][]byte, 0)
	totalSize := 0
	for _, n := range nodes {
		enrBytes, err := rlp.EncodeToBytes(n.Record())
		if err != nil {
			p.Log.Error("failed to encode n", "err", err)
			continue
		}

		if totalSize+len(enrBytes)+enrOverhead > maxSize {
			break
		} else {
			res = append(res, enrBytes)
			totalSize = totalSize + len(enrBytes) + enrOverhead
		}
	}
	return res
}

func (p *PortalProtocol) findNodesCloseToContent(contentId []byte, limit int) []*enode.Node {
	allNodes := p.table.nodeList()
	sort.Slice(allNodes, func(i, j int) bool {
		return enode.LogDist(allNodes[i].ID(), enode.ID(contentId)) < enode.LogDist(allNodes[j].ID(), enode.ID(contentId))
	})

	if len(allNodes) > limit {
		allNodes = allNodes[:limit]
	}

	return allNodes
}

// Lookup performs a recursive lookup for the given target.
// It returns the closest nodes to target.
func (p *PortalProtocol) Lookup(target enode.ID) []*enode.Node {
	return p.newLookup(p.closeCtx, target).run()
}

// Resolve searches for a specific Node with the given ID and tries to get the most recent
// version of the Node record for it. It returns n if the Node could not be resolved.
func (p *PortalProtocol) Resolve(n *enode.Node) *enode.Node {
	if intable := p.table.getNode(n.ID()); intable != nil && intable.Seq() > n.Seq() {
		n = intable
	}
	// Try asking directly. This works if the Node is still responding on the endpoint we have.
	if resp, err := p.RequestENR(n); err == nil {
		return resp
	}
	// Otherwise do a network lookup.
	result := p.Lookup(n.ID())
	for _, rn := range result {
		if rn.ID() == n.ID() && rn.Seq() > n.Seq() {
			return rn
		}
	}
	return n
}

// ResolveNodeId searches for a specific Node with the given ID.
// It returns nil if the nodeId could not be resolved.
func (p *PortalProtocol) ResolveNodeId(id enode.ID) *enode.Node {
	if id == p.Self().ID() {
		p.Log.Debug("Resolve Self Id", "id", id.String())
		return p.Self()
	}

	n := p.table.getNode(id)
	if n != nil {
		p.Log.Debug("found Id in table and will request enr from the node", "id", id.String())
		// Try asking directly. This works if the Node is still responding on the endpoint we have.
		if resp, err := p.RequestENR(n); err == nil {
			return resp
		}
	}

	// Otherwise do a network lookup.
	result := p.Lookup(id)
	for _, rn := range result {
		if rn.ID() == id {
			if n != nil && rn.Seq() <= n.Seq() {
				return n
			} else {
				return rn
			}
		}
	}

	return n
}

func (p *PortalProtocol) collectTableNodes(rip net.IP, distances []uint, limit int) []*enode.Node {
	var bn []*enode.Node
	var nodes []*enode.Node
	var processed = make(map[uint]struct{})
	for _, dist := range distances {
		// Reject duplicate / invalid distances.
		_, seen := processed[dist]
		if seen || dist > 256 {
			continue
		}
		processed[dist] = struct{}{}

		checkLive := !p.table.cfg.NoFindnodeLivenessCheck
		for _, n := range p.table.appendBucketNodes(dist, bn[:0], checkLive) {
			// Apply some pre-checks to avoid sending invalid nodes.
			// Note liveness is checked by appendLiveNodes.
			if netutil.CheckRelayIP(rip, n.IP()) != nil {
				continue
			}
			nodes = append(nodes, n)
			if len(nodes) >= limit {
				return nodes
			}
		}
	}
	return nodes
}

func (p *PortalProtocol) ContentLookup(contentKey, contentId []byte) ([]byte, bool, error) {
	lookupContext, cancel := context.WithCancel(context.Background())
	defer cancel()

	resChan := make(chan *traceContentInfoResp, alpha)
	hasResult := int32(0)

	result := ContentInfoResp{}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for res := range resChan {
			if res.Flag != ContentEnrsSelector {
				result.Content = res.Content.([]byte)
				result.UtpTransfer = res.UtpTransfer
			}
		}
	}()

	newLookup(lookupContext, p.table, enode.ID(contentId), func(n *enode.Node) ([]*enode.Node, error) {
		return p.contentLookupWorker(n, contentKey, resChan, cancel, &hasResult)
	}).run()
	close(resChan)

	wg.Wait()
	if hasResult == 1 {
		return result.Content, result.UtpTransfer, nil
	}

	return nil, false, ErrContentNotFound
}

func (p *PortalProtocol) TraceContentLookup(contentKey, contentId []byte) (*TraceContentResult, error) {
	lookupContext, cancel := context.WithCancel(context.Background())
	// resp channel
	resChan := make(chan *traceContentInfoResp, alpha)

	hasResult := int32(0)

	traceContentRes := &TraceContentResult{}

	selfHexId := "0x" + p.Self().ID().String()

	trace := &Trace{
		Origin:      selfHexId,
		TargetId:    hexutil.Encode(contentId),
		StartedAtMs: int(time.Now().UnixMilli()),
		Responses:   make(map[string]RespByNode),
		Metadata:    make(map[string]*NodeMetadata),
		Cancelled:   make([]string, 0),
	}

	nodes := p.table.findnodeByID(enode.ID(contentId), bucketSize, false)

	localResponse := make([]string, 0, len(nodes.entries))
	for _, node := range nodes.entries {
		id := "0x" + node.ID().String()
		localResponse = append(localResponse, id)
	}
	trace.Responses[selfHexId] = RespByNode{
		DurationMs:    0,
		RespondedWith: localResponse,
	}

	dis := p.Distance(p.Self().ID(), enode.ID(contentId))

	trace.Metadata[selfHexId] = &NodeMetadata{
		Enr:      p.Self().String(),
		Distance: hexutil.Encode(dis[:]),
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for res := range resChan {
			node := res.Node
			hexId := "0x" + node.ID().String()
			dis := p.Distance(node.ID(), enode.ID(contentId))
			p.Log.Debug("reveice res", "id", hexId, "flag", res.Flag)
			trace.Metadata[hexId] = &NodeMetadata{
				Enr:      node.String(),
				Distance: hexutil.Encode(dis[:]),
			}
			// no content return
			if traceContentRes.Content == "" {
				if res.Flag == ContentRawSelector || res.Flag == ContentConnIdSelector {
					trace.ReceivedFrom = hexId
					content := res.Content.([]byte)
					traceContentRes.Content = hexutil.Encode(content)
					traceContentRes.UtpTransfer = res.UtpTransfer
					trace.Responses[hexId] = RespByNode{}
				} else {
					nodes := res.Content.([]*enode.Node)
					respByNode := RespByNode{
						RespondedWith: make([]string, 0, len(nodes)),
					}
					for _, node := range nodes {
						idInner := "0x" + node.ID().String()
						respByNode.RespondedWith = append(respByNode.RespondedWith, idInner)
						if _, ok := trace.Metadata[idInner]; !ok {
							dis := p.Distance(node.ID(), enode.ID(contentId))
							trace.Metadata[idInner] = &NodeMetadata{
								Enr:      node.String(),
								Distance: hexutil.Encode(dis[:]),
							}
						}
						trace.Responses[hexId] = respByNode
					}
				}
			} else {
				trace.Cancelled = append(trace.Cancelled, hexId)
			}
		}
	}()

	lookup := newLookup(lookupContext, p.table, enode.ID(contentId), func(n *enode.Node) ([]*enode.Node, error) {
		return p.contentLookupWorker(n, contentKey, resChan, cancel, &hasResult)
	})
	lookup.run()
	close(resChan)

	wg.Wait()
	if hasResult == 0 {
		cancel()
	}
	traceContentRes.Trace = *trace

	return traceContentRes, nil
}

func (p *PortalProtocol) contentLookupWorker(n *enode.Node, contentKey []byte, resChan chan<- *traceContentInfoResp, cancel context.CancelFunc, done *int32) ([]*enode.Node, error) {
	wrapedNode := make([]*enode.Node, 0)
	flag, content, err := p.findContent(n, contentKey)
	if err != nil {
		return nil, err
	}
	p.Log.Debug("traceContentLookupWorker reveice response", "ip", n.IP().String(), "flag", flag)

	switch flag {
	case ContentRawSelector, ContentConnIdSelector:
		content, ok := content.([]byte)
		if !ok {
			return wrapedNode, fmt.Errorf("failed to assert to raw content, value is: %v", content)
		}
		res := &traceContentInfoResp{
			Node:        n,
			Flag:        flag,
			Content:     content,
			UtpTransfer: false,
		}
		if flag == ContentConnIdSelector {
			res.UtpTransfer = true
		}
		if atomic.CompareAndSwapInt32(done, 0, 1) {
			p.Log.Debug("contentLookupWorker find content", "ip", n.IP().String(), "port", n.UDP())
			resChan <- res
			cancel()
		}
		return wrapedNode, err
	case ContentEnrsSelector:
		nodes, ok := content.([]*enode.Node)
		if !ok {
			return wrapedNode, fmt.Errorf("failed to assert to enrs content, value is: %v", content)
		}
		resChan <- &traceContentInfoResp{
			Node:        n,
			Flag:        flag,
			Content:     content,
			UtpTransfer: false,
		}
		return nodes, nil
	}
	return wrapedNode, nil
}

func (p *PortalProtocol) ToContentId(contentKey []byte) []byte {
	return p.toContentId(contentKey)
}

func (p *PortalProtocol) InRange(contentId []byte) bool {
	return inRange(p.Self().ID(), p.Radius(), contentId)
}

func (p *PortalProtocol) Get(contentKey []byte, contentId []byte) ([]byte, error) {
	content, err := p.storage.Get(contentKey, contentId)
	p.Log.Trace("get local storage", "contentKey", hexutil.Encode(contentKey), "contentId", hexutil.Encode(contentId), "err", err)
	return content, err
}

func (p *PortalProtocol) Put(contentKey []byte, contentId []byte, content []byte) error {
	err := p.storage.Put(contentKey, contentId, content)
	p.Log.Trace("put local storage", "contentKey", hexutil.Encode(contentKey), "contentId", hexutil.Encode(contentId), "err", err)
	return err
}

func (p *PortalProtocol) GetContent() chan *ContentElement {
	return p.contentQueue
}

// GossipAndReturnPeers sends the content to the closest nodes and returns the nodes that received the content.
func (p *PortalProtocol) GossipAndReturnPeers(srcNodeId *enode.ID, contentKeys [][]byte, content [][]byte) ([]*enode.Node, error) {
	if len(content) == 0 {
		return nil, errors.New("empty content")
	}

	contentList := make([]*ContentEntry, 0, ContentKeysLimit)
	for i := 0; i < len(content); i++ {
		contentEntry := &ContentEntry{
			ContentKey: contentKeys[i],
			Content:    content[i],
		}
		contentList = append(contentList, contentEntry)
	}

	contentId := p.toContentId(contentKeys[0])
	if contentId == nil {
		return nil, ErrNilContentKey
	}

	maxClosestNodes := 4
	maxFartherNodes := 4
	closestLocalNodes := p.findNodesCloseToContent(contentId, 32)
	p.Log.Debug("closest local nodes", "count", len(closestLocalNodes))

	gossipNodes := make([]*enode.Node, 0)
	for _, n := range closestLocalNodes {
		radius, found := p.radiusCache.HasGet(nil, []byte(n.ID().String()))
		if found {
			p.Log.Debug("found closest local nodes", "nodeId", n.ID(), "addr", n.IPAddr().String())
			nodeRadius := new(uint256.Int)
			err := nodeRadius.UnmarshalSSZ(radius)
			if err != nil {
				return nil, err
			}
			if inRange(n.ID(), nodeRadius, contentId) {
				if srcNodeId == nil {
					gossipNodes = append(gossipNodes, n)
				} else if n.ID() != *srcNodeId {
					gossipNodes = append(gossipNodes, n)
				}
			}
		}
	}

	if len(gossipNodes) == 0 {
		return gossipNodes, nil
	}

	var finalGossipNodes []*enode.Node
	if len(gossipNodes) > maxClosestNodes {
		fartherNodes := gossipNodes[maxClosestNodes:]
		p.table.rand.Shuffle(len(fartherNodes), func(i, j int) {
			fartherNodes[i], fartherNodes[j] = fartherNodes[j], fartherNodes[i]
		})
		finalGossipNodes = append(gossipNodes[:maxClosestNodes], fartherNodes[:min(maxFartherNodes, len(fartherNodes))]...)
	} else {
		finalGossipNodes = gossipNodes
	}

	for _, n := range finalGossipNodes {
		permit, ok := p.Utp.GetOutboundPermit()
		if !ok {
			p.Log.Debug("reached utp conn limit, will drop this content", "network", p.protocolName, "nodeId", n.ID(), "addr", n.IPAddr().String())
			continue
		}
		transientOfferRequest := &TransientOfferRequest{
			Contents: contentList,
		}

		offerRequest := &OfferRequest{
			Kind:    TransientOfferRequestKind,
			Request: transientOfferRequest,
		}

		offerRequestWithNode := &OfferRequestWithNode{
			Node:    n,
			Request: offerRequest,
			permit:  permit,
		}
		select {
		case p.offerQueue <- offerRequestWithNode:
		default:
			p.Log.Warn("offer queue is full, drop offer request", "network", p.protocolName, "nodeId", n.ID(), "addr", n.IPAddr().String())
			if metrics.Enabled() {
				p.portalMetrics.gossipDropCount.Inc(1)
			}
		}
	}

	return finalGossipNodes, nil
}

// Gossip sends the content to the closest nodes and returns the number of nodes that received the content.
func (p *PortalProtocol) Gossip(srcNodeId *enode.ID, contentKeys [][]byte, content [][]byte) (int, error) {
	nodes, err := p.GossipAndReturnPeers(srcNodeId, contentKeys, content)
	if err != nil {
		return 0, err
	}

	return len(nodes), nil
}

// ShouldStore if the content is not in range, return false; else store the content and return true
func (p *PortalProtocol) ShouldStore(contentKey []byte, content []byte) (bool, error) {
	err := p.storage.Put(contentKey, p.toContentId(contentKey), content)
	if errors.Is(err, storage.ErrInsufficientRadius) {
		return false, nil
	}
	return true, nil
}

func (p *PortalProtocol) Distance(a, b enode.ID) enode.ID {
	res := [32]byte{}
	for i := range a {
		res[i] = a[i] ^ b[i]
	}
	return res
}

func inRange(nodeId enode.ID, nodeRadius *uint256.Int, contentId []byte) bool {
	distance := enode.LogDist(nodeId, enode.ID(contentId))
	disBig := new(big.Int).SetInt64(int64(distance))
	return nodeRadius.CmpBig(disBig) > 0
}

func encodeContents(contents [][]byte) []byte {
	contentsBytes := make([]byte, 0)
	for _, content := range contents {
		encodedContent := encodeSingleContent(content)
		contentsBytes = append(contentsBytes, encodedContent...)
	}

	return contentsBytes
}

func decodeContents(payload []byte) ([][]byte, error) {
	contents := make([][]byte, 0)
	remainingData := payload

	for len(remainingData) > 0 {
		content, remaining, err := decodeSingleContent(remainingData)
		if err != nil {
			return nil, err
		}
		contents = append(contents, content)
		remainingData = remaining
	}
	return contents, nil
}

func getContentKeys(request *OfferRequest) [][]byte {
	switch request.Kind {
	case TransientOfferRequestKind:
		contentKeys := make([][]byte, 0)
		contents := request.Request.(*TransientOfferRequest).Contents
		for _, content := range contents {
			contentKeys = append(contentKeys, content.ContentKey)
		}

		return contentKeys
	case TransientOfferRequestWithResultKind:
		content := request.Request.(*TransientOfferRequestWithResult).Content
		return [][]byte{content.ContentKey}
	default:
		return request.Request.(*PersistOfferRequest).ContentKeys
	}
}

// lookupDistances computes the distance parameter for FINDNODE calls to dest.
// It chooses distances adjacent to logdist(target, dest), e.g. for a target
// with logdist(target, dest) = 255 the result is [255, 256, 254].
func lookupDistances(target, dest enode.ID) (dists []uint) {
	td := enode.LogDist(target, dest)
	dists = append(dists, uint(td))
	for i := 1; len(dists) < lookupRequestLimit; i++ {
		if td+i <= 256 {
			dists = append(dists, uint(td+i))
		}
		if td-i > 0 {
			dists = append(dists, uint(td-i))
		}
	}
	return dists
}
