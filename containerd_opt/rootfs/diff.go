package rootfs

import (
	"context"
	"fmt"

	"github.com/containerd/containerd/diff"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/pkg/cleanup"
	"github.com/containerd/containerd/snapshots"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// CreateDiff creates a layer diff for the given snapshot identifier from the
// parent of the snapshot. A content ref is provided to track the progress of
// the content creation and the provided snapshotter and mount differ are used
// for calculating the diff. The descriptor for the layer diff is returned.
func CreateDiff(ctx context.Context, snapshotID string, sn snapshots.Snapshotter, d diff.Comparer, opts ...diff.Opt) (ocispec.Descriptor, error) {
	info, err := sn.Stat(ctx, snapshotID)
	if err != nil {
		return ocispec.Descriptor{}, err
	}

	lowerKey := fmt.Sprintf("%s-parent-view-%s", info.Parent, uniquePart())
	lower, err := sn.View(ctx, lowerKey, info.Parent)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	defer cleanup.Do(ctx, func(ctx context.Context) {
		sn.Remove(ctx, lowerKey)
	})

	var upper []mount.Mount
	if info.Kind == snapshots.KindActive {
		upper, err = sn.Mounts(ctx, snapshotID)
		if err != nil {
			return ocispec.Descriptor{}, err
		}
	} else {
		upperKey := fmt.Sprintf("%s-view-%s", snapshotID, uniquePart())
		upper, err = sn.View(ctx, upperKey, snapshotID)
		if err != nil {
			return ocispec.Descriptor{}, err
		}
		defer cleanup.Do(ctx, func(ctx context.Context) {
			sn.Remove(ctx, upperKey)
		})
	}

	return d.Compare(ctx, lower, upper, opts...)
}
