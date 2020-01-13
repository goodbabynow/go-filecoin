package porcelain

import (
	"context"
	"io"

	"github.com/filecoin-project/go-filecoin/internal/pkg/piecemanager"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"

	"github.com/pkg/errors"
)

type pmPlumbing interface {
	PieceManager() piecemanager.PieceManager
	DAGImportData(context.Context, io.Reader) (ipld.Node, error)
	DAGCat(context.Context, cid.Cid) (io.Reader, error)
	DAGGetFileSize(ctx context.Context, c cid.Cid) (uint64, error)
}

// SealPieceIntoNewSector writes the provided piece-bytes into a new sector.
func SealPieceIntoNewSector(ctx context.Context, p pmPlumbing, dealID uint64, pieceSize uint64, pieceReader io.Reader) error {
	if p.PieceManager() == nil {
		return errors.New("must be mining to add piece")
	}

	return p.PieceManager().SealPieceIntoNewSector(ctx, dealID, pieceSize, pieceReader)
}
