package miner

import (
	"context"
	"encoding/json"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin"
	"github.com/filecoin-project/lotus/chain/types"
	"golang.org/x/xerrors"
)

// Warmup epochs that used by the attacker before it starts
// sending equivocated blocks.
const WARMUP_EPOCHS = 7

// Number of equivocated blocks kept in the pool each round for
// its use for the attack.
const EQ_BLK_POOL = 10

// random valid message to be included in equivocated blocks.
func (m *Miner) msgForEquivocatedBlk(ctx context.Context, i int64) (*types.SignedMessage, error) {
	addr, err := m.api.WalletDefaultAddress(ctx)
	if err != nil {
		return nil, err
	}
	nonce, err := m.api.MpoolGetNonce(ctx, addr)
	if err != nil {
		return nil, err
	}
	msg := &types.Message{
		From:  addr,
		To:    builtin.BurntFundsActorAddr,
		Value: abi.NewTokenAmount(i),

		Method: builtin.MethodSend,
		Params: nil,
		Nonce:  nonce,
	}

	msg.GasPremium = types.NewInt(0)
	msg.GasFeeCap = types.NewInt(0)
	msg.GasLimit = 0

	cp := *msg
	msg = &cp
	inMsg := *msg

	fromA, err := m.api.StateAccountKey(ctx, msg.From, types.EmptyTSK)
	if err != nil {
		return nil, xerrors.Errorf("getting key address: %w", err)
	}
	// if msg.Nonce != 0 {
	// 	return nil, xerrors.Errorf("MpoolPushMessage expects message nonce to be 0, was %d", msg.Nonce)
	// }

	msg, err = m.api.GasEstimateMessageGas(ctx, msg, nil, types.EmptyTSK)
	if err != nil {
		return nil, xerrors.Errorf("GasEstimateMessageGas error: %w", err)
	}

	if msg.GasPremium.GreaterThan(msg.GasFeeCap) {
		inJson, _ := json.Marshal(inMsg)
		outJson, _ := json.Marshal(msg)
		return nil, xerrors.Errorf("After estimation, GasPremium is greater than GasFeeCap, inmsg: %s, outmsg: %s",
			inJson, outJson)
	}

	if msg.From.Protocol() == address.ID {
		log.Warnf("Push from ID address (%s), adjusting to %s", msg.From, fromA)
		msg.From = fromA
	}

	b, err := m.api.WalletBalance(ctx, msg.From)
	if err != nil {
		return nil, xerrors.Errorf("mpool push: getting origin balance: %w", err)
	}

	requiredFunds := big.Add(msg.Value, msg.RequiredFunds())
	if b.LessThan(requiredFunds) {
		return nil, xerrors.Errorf("mpool push: not enough funds: %s < %s", b, requiredFunds)
	}

	// Sign and push the message
	return m.api.WalletSignMessage(ctx, addr, msg)
}

// Create a new equivocated block with a random message to publish in parallel
// to the real block miner (and kept private).
func (m *Miner) eqBlock(ctx context.Context, blk *types.BlockMsg, i int) (*types.FullBlock, error) {
	// make a copy of the header
	hcp := *blk.Header
	var secpkMessages []*types.SignedMessage
	var blsMessages []*types.Message

	eqMsg, err := m.msgForEquivocatedBlk(ctx, int64(i))
	if err != nil {
		return nil, err
	}
	blsMessages = append(blsMessages, &eqMsg.Message)
	blockMsg, err := m.api.MinerCreateBlock(context.TODO(), &api.BlockTemplate{
		Miner:            hcp.Miner,
		Parents:          types.NewTipSetKey(hcp.Parents...),
		Ticket:           hcp.Ticket,
		Eproof:           hcp.ElectionProof,
		BeaconValues:     hcp.BeaconEntries,
		Messages:         []*types.SignedMessage{eqMsg},
		Epoch:            hcp.Height,
		Timestamp:        hcp.Timestamp,
		WinningPoStProof: hcp.WinPoStProof,
	})
	if err != nil {
		return nil, err
	}

	return &types.FullBlock{
		Header:        blockMsg.Header,
		BlsMessages:   blsMessages,
		SecpkMessages: secpkMessages,
	}, nil
}

func (m *Miner) equivocatedBlocks(ctx context.Context, blk *types.BlockMsg, num int) ([]*types.FullBlock, error) {
	blks := make([]*types.FullBlock, 0)
	for i := 0; i < num; i++ {
		b, err := m.eqBlock(ctx, blk, i)
		if err != nil {
			return nil, err
		}
		blks = append(blks, b)
	}
	return blks, nil

}
