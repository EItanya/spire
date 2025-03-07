package datastore

import (
	"context"
	"crypto"
	"time"

	"github.com/spiffe/spire/pkg/server/datastore"
	"github.com/spiffe/spire/proto/spire/common"
)

// WithBundleUpdateCallback wraps a datastore interface and provides updates to
// bundle publishers in operations that modify the local bundle.
func WithBundleUpdateCallback(ds datastore.DataStore, bundleUpdated func()) datastore.DataStore {
	return datastoreWrapper{
		DataStore:     ds,
		bundleUpdated: bundleUpdated,
	}
}

type datastoreWrapper struct {
	datastore.DataStore
	bundleUpdated func()
}

func (w datastoreWrapper) AppendBundle(ctx context.Context, bundle *common.Bundle) (*common.Bundle, error) {
	b, err := w.DataStore.AppendBundle(ctx, bundle)
	if err == nil {
		w.bundleUpdated()
	}
	return b, err
}

func (w datastoreWrapper) PruneBundle(ctx context.Context, trustDomainID string, expiresBefore time.Time) (bool, error) {
	changed, err := w.DataStore.PruneBundle(ctx, trustDomainID, expiresBefore)
	if err == nil && changed {
		w.bundleUpdated()
	}
	return changed, err
}

func (w datastoreWrapper) RevokeX509CA(ctx context.Context, trustDomainID string, publicKey crypto.PublicKey) error {
	err := w.DataStore.RevokeX509CA(ctx, trustDomainID, publicKey)
	if err == nil {
		w.bundleUpdated()
	}
	return err
}

func (w datastoreWrapper) RevokeJWTKey(ctx context.Context, trustDomainID string, keyID string) (*common.PublicKey, error) {
	pubKey, err := w.DataStore.RevokeJWTKey(ctx, trustDomainID, keyID)
	if err == nil {
		w.bundleUpdated()
	}
	return pubKey, err
}
