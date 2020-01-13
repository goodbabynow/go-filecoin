package submodule

import (
	"context"
	"errors"
	"math"

	"github.com/filecoin-project/go-filecoin/internal/pkg/consensus"

	"github.com/filecoin-project/go-filecoin/internal/app/go-filecoin/plumbing/msg"

	"github.com/filecoin-project/go-storage-miner"
	"github.com/ipfs/go-cid"
	"github.com/polydawn/refmt/cbor"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-filecoin/internal/pkg/block"
	"github.com/filecoin-project/go-filecoin/internal/pkg/chain"
	"github.com/filecoin-project/go-filecoin/internal/pkg/types"
	"github.com/filecoin-project/go-filecoin/internal/pkg/vm/abi"
	"github.com/filecoin-project/go-filecoin/internal/pkg/vm/actor/builtin/storagemarket"
	"github.com/filecoin-project/go-filecoin/internal/pkg/vm/address"
)

// ChainSampler is a function which samples randomness from the chain at the
// given height.
type ChainSampler func(ctx context.Context, sampleHeight *types.BlockHeight) ([]byte, error)

type heightThresholdListener struct {
	target    uint64
	targetHit bool

	seedCh    chan storage.SealSeed
	errCh     chan error
	invalidCh chan struct{}
	doneCh    chan struct{}
}

// handle a chain update by sending appropriate status messages back to the channels.
// newChain is all the tipsets that are new since the last head update.
// Normally, this will be a single tipset, but in the case of a re-org it will contain
// All the common ancestors of the new tipset to the greatest common ancestor.
// Returns false if this handler is no longer valid
func (l heightThresholdListener) handle(ctx context.Context, chain []block.TipSet, sampler ChainSampler) (bool, error) {
	if len(chain) < 1 {
		return true, nil
	}

	h, err := chain[0].Height()
	if err != nil {
		return true, err
	}

	// check if we've hit finality and should stop listening
	if h >= l.target+consensus.FinalityEpochs {
		return false, nil
	}

	lcaHeight, err := chain[len(chain)-1].Height()
	if err != nil {
		return true, err
	}

	// if we have already seen a target tipset
	if l.targetHit {
		// if we've completely reverted
		if h < l.target {
			l.invalidCh <- struct{}{}
			l.targetHit = false
			// if we've re-orged to a point before the target
		} else if lcaHeight < l.target {
			l.invalidCh <- struct{}{}
			err := l.sendRandomness(ctx, chain, sampler)
			if err != nil {
				return true, err
			}
		}
		return true, nil
	}

	// otherwise send randomness if we've hit the height
	if h >= l.target {
		l.targetHit = true
		err := l.sendRandomness(ctx, chain, sampler)
		if err != nil {
			return true, err
		}
	}
	return true, nil
}

func (l heightThresholdListener) sendRandomness(ctx context.Context, chain []block.TipSet, sampler ChainSampler) error {
	// assume chain not empty and first tipset height greater than target
	firstTargetTipset := chain[0]
	for _, ts := range chain {
		h, err := ts.Height()
		if err != nil {
			return err
		}

		if h < l.target {
			break
		}
		firstTargetTipset = ts
	}

	tsHeight, err := firstTargetTipset.Height()
	if err != nil {
		return err
	}

	randomness, err := sampler(ctx, types.NewBlockHeight(tsHeight))
	if err != nil {
		return err
	}

	l.seedCh <- storage.SealSeed{
		BlockHeight: tsHeight,
		TicketBytes: randomness,
	}
	return nil
}

// StorageMinerNodeAdapter is a struct which satisfies the go-storage-miner
// needs of "the node," e.g. interacting with the blockchain, persisting sector
// states to disk, and so forth.
type StorageMinerNodeAdapter struct {
	minerAddr  address.Address
	workerAddr address.Address

	newListener     chan heightThresholdListener
	heightListeners []heightThresholdListener
	listenerDone    chan struct{}

	chain     *ChainSubmodule
	messaging *MessagingSubmodule
	msgWaiter *msg.Waiter
	wallet    *WalletSubmodule
}

var _ storage.NodeAPI = new(StorageMinerNodeAdapter)

// NewStorageMinerNodeAdapter produces a StorageMinerNodeAdapter, which adapts
// types in this codebase to the interface representing "the node" which is
// expected by the go-storage-miner project.
func NewStorageMinerNodeAdapter(minerAddress address.Address, workerAddress address.Address, c *ChainSubmodule, m *MessagingSubmodule, mw *msg.Waiter, w *WalletSubmodule) *StorageMinerNodeAdapter {
	return &StorageMinerNodeAdapter{
		minerAddr:    minerAddress,
		workerAddr:   workerAddress,
		listenerDone: make(chan struct{}),
		chain:        c,
		messaging:    m,
		msgWaiter:    mw,
		wallet:       w,
	}
}

// StartHeightListener starts the scheduler that manages height listeners.
func (m *StorageMinerNodeAdapter) StartHeightListener(ctx context.Context, htc <-chan interface{}) {
	go func() {
		var previousHead block.TipSet
		for {
			select {
			case <-htc:
				head, err := m.handleNewTipSet(ctx, previousHead)
				if err != nil {
					log.Warn("failed to handle new tipset")
				} else {
					previousHead = head
				}
			case heightListener := <-m.newListener:
				m.heightListeners = append(m.heightListeners, heightListener)
			case <-m.listenerDone:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// StopHeightListener stops the scheduler that manages height listeners.
func (m *StorageMinerNodeAdapter) StopHeightListener() {
	m.listenerDone <- struct{}{}
}

func (m *StorageMinerNodeAdapter) handleNewTipSet(ctx context.Context, previousHead block.TipSet) (block.TipSet, error) {
	newHeadKey := m.chain.ChainReader.GetHead()
	newHead, err := m.chain.ChainReader.GetTipSet(newHeadKey)
	if err != nil {
		return block.TipSet{}, err
	}

	_, newTips, err := chain.CollectTipsToCommonAncestor(ctx, m.chain.ChainReader, previousHead, newHead)
	if err != nil {
		return block.TipSet{}, err
	}

	newListeners := make([]heightThresholdListener, len(m.heightListeners))
	for _, listener := range m.heightListeners {
		valid, err := listener.handle(ctx, newTips, m.chain.State.SampleRandomness)
		if err != nil {
			log.Error("Error checking storage miner chain listener", err)
		}

		if valid {
			newListeners = append(newListeners, listener)
		}
	}
	m.heightListeners = newListeners

	return newHead, nil
}

// SendSelfDeals creates self-deals, places the resulting message in the node's
// mempool and returns the identity of that message (or an error, if
// encountered).
func (m *StorageMinerNodeAdapter) SendSelfDeals(ctx context.Context, pieces ...storage.PieceInfo) (cid.Cid, error) {
	proposals := make([]types.StorageDealProposal, len(pieces))
	for i, piece := range pieces {
		proposals[i] = types.StorageDealProposal{
			PieceRef:             piece.CommP[:],
			PieceSize:            types.Uint64(piece.Size),
			Client:               m.workerAddr,
			Provider:             m.minerAddr,
			ProposalExpiration:   math.MaxUint64,
			Duration:             math.MaxUint64 / 2, // /2 because overflows
			StoragePricePerEpoch: 0,
			StorageCollateral:    0,
			ProposerSignature:    nil,
		}

		proposalBytes, err := cbor.Marshal(proposals[i])
		if err != nil {
			return cid.Undef, err
		}

		sig, err := m.wallet.Wallet.SignBytes(proposalBytes, m.workerAddr)
		if err != nil {
			return cid.Undef, err
		}

		proposals[i].ProposerSignature = &sig
	}

	dealParams, err := abi.ToEncodedValues(proposals)
	if err != nil {
		return cid.Undef, err
	}

	mcid, cerr, err := m.messaging.Outbox.Send(
		ctx,
		m.workerAddr,
		address.StorageMarketAddress,
		types.ZeroAttoFIL,
		types.NewGasPrice(1),
		types.NewGasUnits(300),
		true,
		storagemarket.PublishStorageDeals,
		dealParams,
	)
	if err != nil {
		return cid.Undef, err
	}

	err = <-cerr
	if err != nil {
		return cid.Undef, err
	}

	return mcid, nil
}

// WaitForSelfDeals blocks until the provided storage deal-publishing message is
// mined into a block, producing a slice of deal IDs and an exit code when it is
// mined into a block (or an error, if encountered).
func (m *StorageMinerNodeAdapter) WaitForSelfDeals(ctx context.Context, mcid cid.Cid) ([]uint64, uint8, error) {
	receiptChan := make(chan *types.MessageReceipt)
	errChan := make(chan error)

	go func() {
		err := m.msgWaiter.Wait(ctx, mcid, func(b *block.Block, message *types.SignedMessage, r *types.MessageReceipt) error {
			receiptChan <- r
			return nil
		})
		if err != nil {
			errChan <- err
		}
	}()

	select {
	case receipt := <-receiptChan:
		if receipt.ExitCode != 0 {
			return nil, receipt.ExitCode, nil
		}

		dealIDValues, err := abi.Deserialize(receipt.Return[0], abi.UintArray)
		if err != nil {
			return nil, 0, err
		}

		dealIds, ok := dealIDValues.Val.([]uint64)
		if !ok {
			return nil, 0, errors.New("Decoded deal ids are not a []uint64")
		}

		return dealIds, 0, nil
	case err := <-errChan:
		return nil, 0, err
	case <-ctx.Done():
		return nil, 0, errors.New("context ended prematurely")
	}
}

// SendPreCommitSector creates a pre-commit sector message and places it into
// the nodes message pool and returns its identity (or an error, if one is
// encountered).
func (m *StorageMinerNodeAdapter) SendPreCommitSector(ctx context.Context, sectorID uint64, commR []byte, ticket storage.SealTicket, pieces ...storage.Piece) (cid.Cid, error) {
	dealIds := make([]types.Uint64, len(pieces))
	for i, piece := range pieces {
		dealIds[i] = types.Uint64(piece.DealID)
	}

	info := &types.SectorPreCommitInfo{
		SectorNumber: types.Uint64(sectorID),

		CommR:     commR,
		SealEpoch: types.Uint64(ticket.BlockHeight),
		DealIDs:   dealIds,
	}

	precommitParams, err := abi.ToEncodedValues(info)
	if err != nil {
		return cid.Undef, err
	}

	mcid, cerr, err := m.messaging.Outbox.Send(
		ctx,
		m.workerAddr,
		m.minerAddr,
		types.ZeroAttoFIL,
		types.NewGasPrice(1),
		types.NewGasUnits(300),
		true,
		storagemarket.PreCommitSector,
		precommitParams,
	)
	if err != nil {
		return cid.Undef, err
	}

	err = <-cerr
	if err != nil {
		return cid.Undef, err
	}

	return mcid, nil
}

// WaitForPreCommitSector blocks until the pre-commit sector message is mined
// into a block, returning the block's height and message's exit code (or an
// error if one is encountered).
func (m *StorageMinerNodeAdapter) WaitForPreCommitSector(ctx context.Context, mcid cid.Cid) (uint64, uint8, error) {
	return m.waitForMessageHeight(ctx, mcid)
}

// SendProveCommitSector creates a commit sector message and places it into the
// nodes message pool and returns its identity (or an error, if one is
// encountered).
func (m *StorageMinerNodeAdapter) SendProveCommitSector(ctx context.Context, sectorID uint64, proof []byte, deals ...uint64) (cid.Cid, error) {
	dealIds := make([]types.Uint64, len(deals))
	for i, deal := range deals {
		dealIds[i] = types.Uint64(deal)
	}

	info := &types.SectorProveCommitInfo{
		Proof:    proof,
		SectorID: types.Uint64(sectorID),
		DealIDs:  dealIds,
	}

	commitParams, err := abi.ToEncodedValues(info)
	if err != nil {
		return cid.Undef, err
	}

	mcid, cerr, err := m.messaging.Outbox.Send(
		ctx,
		m.workerAddr,
		m.minerAddr,
		types.ZeroAttoFIL,
		types.NewGasPrice(1),
		types.NewGasUnits(300),
		true,
		storagemarket.CommitSector,
		commitParams,
	)
	if err != nil {
		return cid.Undef, err
	}

	err = <-cerr
	if err != nil {
		return cid.Undef, err
	}

	return mcid, nil
}

// WaitForProveCommitSector blocks until the provided pre-commit message has
// been mined into the chain, producing the height of the block in which the
// message was mined (and the message's exit code) or an error if any is
// encountered.
func (m *StorageMinerNodeAdapter) WaitForProveCommitSector(ctx context.Context, mcid cid.Cid) (uint64, uint8, error) {
	return m.waitForMessageHeight(ctx, mcid)
}

// GetSealTicket produces the seal ticket used when pre-committing a sector at
// the moment it is called
func (m *StorageMinerNodeAdapter) GetSealTicket(ctx context.Context) (storage.SealTicket, error) {
	ts, err := m.chain.ChainReader.GetTipSet(m.chain.ChainReader.GetHead())
	if err != nil {
		return storage.SealTicket{}, xerrors.Errorf("getting head ts for SealTicket failed: %w", err)
	}

	h, err := ts.Height()
	if err != nil {
		return storage.SealTicket{}, err
	}

	r, err := m.chain.State.SampleRandomness(ctx, types.NewBlockHeight(h-consensus.FinalityEpochs))
	if err != nil {
		return storage.SealTicket{}, xerrors.Errorf("getting randomness for SealTicket failed: %w", err)
	}

	return storage.SealTicket{
		BlockHeight: h,
		TicketBytes: r,
	}, nil
}

// GetSealSeed is used to acquire the seal seed for the provided pre-commit
// message, and provides channels to accommodate chain re-orgs. The caller is
// responsible for choosing an interval-value, which is a quantity of blocks to
// wait (after the block in which the pre-commit message is mined) before
// computing and sampling a seed.
func (m *StorageMinerNodeAdapter) GetSealSeed(ctx context.Context, preCommitMsg cid.Cid, interval uint64) (seed <-chan storage.SealSeed, err <-chan error, invalidated <-chan struct{}, done <-chan struct{}) {
	sc := make(chan storage.SealSeed)
	ec := make(chan error)
	ic := make(chan struct{})
	dc := make(chan struct{})

	go func() {
		h, exitCode, err := m.waitForMessageHeight(ctx, preCommitMsg)
		if err != nil {
			ec <- err
			return
		}

		if exitCode != 0 {
			ec <- xerrors.Errorf("non-zero exit code for pre-commit message %d", exitCode)
			return
		}

		m.newListener <- heightThresholdListener{
			target:    h + interval,
			targetHit: false,
			seedCh:    sc,
			errCh:     ec,
			invalidCh: ic,
			doneCh:    dc,
		}
	}()

	return sc, ec, ic, dc
}

type heightAndExitCode struct {
	exitCode uint8
	height   types.Uint64
}

func (m *StorageMinerNodeAdapter) waitForMessageHeight(ctx context.Context, mcid cid.Cid) (uint64, uint8, error) {
	height := make(chan heightAndExitCode)
	errChan := make(chan error)

	go func() {
		err := m.msgWaiter.Wait(ctx, mcid, func(b *block.Block, message *types.SignedMessage, r *types.MessageReceipt) error {
			height <- heightAndExitCode{
				height:   b.Height,
				exitCode: r.ExitCode,
			}
			return nil
		})
		if err != nil {
			errChan <- err
		}
	}()

	select {
	case h := <-height:
		return uint64(h.height), h.exitCode, nil
	case err := <-errChan:
		return 0, 0, err
	case <-ctx.Done():
		return 0, 0, errors.New("context ended prematurely")
	}
}
