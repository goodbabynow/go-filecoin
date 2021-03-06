package submodule

import (
	"context"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldbridge"
	gsnet "github.com/ipfs/go-graphsync/network"
	gsstoreutil "github.com/ipfs/go-graphsync/storeutil"
	"github.com/pkg/errors"

	"github.com/filecoin-project/go-filecoin/internal/pkg/block"
	"github.com/filecoin-project/go-filecoin/internal/pkg/chainsync"
	"github.com/filecoin-project/go-filecoin/internal/pkg/chainsync/fetcher"
	"github.com/filecoin-project/go-filecoin/internal/pkg/clock"
	"github.com/filecoin-project/go-filecoin/internal/pkg/consensus"
	"github.com/filecoin-project/go-filecoin/internal/pkg/net"
	"github.com/filecoin-project/go-filecoin/internal/pkg/net/pubsub"
	"github.com/filecoin-project/go-filecoin/internal/pkg/proofs"
	"github.com/filecoin-project/go-filecoin/internal/pkg/slashing"
)

// SyncerSubmodule enhances the node with chain syncing capabilities
type SyncerSubmodule struct {
	BlockTopic       *pubsub.Topic
	BlockSub         pubsub.Subscription
	ChainSelector    nodeChainSelector
	Consensus        consensus.Protocol
	FaultDetector    slashing.ConsensusFaultDetector
	ChainSyncManager *chainsync.Manager

	// cancelChainSync cancels the context for chain sync subscriptions and handlers.
	CancelChainSync context.CancelFunc
	// faultCh receives detected consensus faults
	faultCh chan slashing.ConsensusFault
}

type syncerConfig interface {
	GenesisCid() cid.Cid
	BlockTime() time.Duration
	ChainClock() clock.ChainEpochClock
}

type nodeChainSelector interface {
	Weight(context.Context, block.TipSet, cid.Cid) (uint64, error)
	IsHeavier(ctx context.Context, a, b block.TipSet, aStateID, bStateID cid.Cid) (bool, error)
}

// NewSyncerSubmodule creates a new chain submodule.
func NewSyncerSubmodule(ctx context.Context, config syncerConfig, repo chainRepo, blockstore *BlockstoreSubmodule, network *NetworkSubmodule, discovery *DiscoverySubmodule, chn *ChainSubmodule, nodePoster *proofs.ElectionPoster) (SyncerSubmodule, error) {
	// setup block validation
	// TODO when #2961 is resolved do the needful here.
	blkValid := consensus.NewDefaultBlockValidator(config.ChainClock())

	// register block validation on floodsub
	btv := net.NewBlockTopicValidator(blkValid)
	if err := network.pubsub.RegisterTopicValidator(btv.Topic(network.NetworkName), btv.Validator(), btv.Opts()...); err != nil {
		return SyncerSubmodule{}, errors.Wrap(err, "failed to register block validator")
	}

	// setup topic.
	topic, err := network.pubsub.Join(net.BlockTopic(network.NetworkName))
	if err != nil {
		return SyncerSubmodule{}, err
	}

	// set up consensus
	nodeConsensus := consensus.NewExpected(blockstore.CborStore, blockstore.Blockstore, chn.Processor, chn.ActorState, config.BlockTime(), consensus.ElectionMachine{}, consensus.TicketMachine{}, nodePoster)
	nodeChainSelector := consensus.NewChainSelector(blockstore.CborStore, chn.ActorState, config.GenesisCid())

	// setup fecher
	graphsyncNetwork := gsnet.NewFromLibp2pHost(network.Host)
	bridge := ipldbridge.NewIPLDBridge()
	loader := gsstoreutil.LoaderForBlockstore(blockstore.Blockstore)
	storer := gsstoreutil.StorerForBlockstore(blockstore.Blockstore)
	gsync := graphsync.New(ctx, graphsyncNetwork, bridge, loader, storer)
	fetcher := fetcher.NewGraphSyncFetcher(ctx, gsync, blockstore.Blockstore, blkValid, config.ChainClock(), discovery.PeerTracker)
	faultCh := make(chan slashing.ConsensusFault)
	faultDetector := slashing.NewConsensusFaultDetector(faultCh)

	chainSyncManager, err := chainsync.NewManager(nodeConsensus, blkValid, nodeChainSelector, chn.ChainReader, chn.MessageStore, fetcher, config.ChainClock(), faultDetector)
	if err != nil {
		return SyncerSubmodule{}, err
	}

	return SyncerSubmodule{
		BlockTopic: pubsub.NewTopic(topic),
		// BlockSub: nil,
		Consensus:        nodeConsensus,
		ChainSelector:    nodeChainSelector,
		ChainSyncManager: &chainSyncManager,
		// cancelChainSync: nil,
		faultCh: faultCh,
	}, nil
}

type syncerNode interface {
}

// Start starts the syncer submodule for a node.
func (s *SyncerSubmodule) Start(ctx context.Context, _node syncerNode) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.faultCh:
				// TODO #3690 connect this up to a slasher that sends messages
				// to outbound queue to carry out penalization
			}
		}
	}()
	return s.ChainSyncManager.Start(ctx)
}
