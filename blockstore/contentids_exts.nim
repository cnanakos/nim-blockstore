## LogosStorage content ID extensions for libp2p CID
import libp2p/multicodec

const ContentIdsExts* = @[
  multiCodec("logos-storage-manifest"),
  multiCodec("logos-storage-block"),
  multiCodec("logos-storage-tree"),
]
