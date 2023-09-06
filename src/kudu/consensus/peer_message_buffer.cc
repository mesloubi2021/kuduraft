// Copyright (c) Meta Platforms, Inc. and affiliates.

#include "kudu/consensus/peer_message_buffer.h"
#include "kudu/util/flag_tags.h"

DEFINE_int64(
    max_buffer_fill_size_bytes,
    2 * 1024 * 1024,
    "The maximum size to fill the peer buffer each attempt.");
TAG_FLAG(max_buffer_fill_size_bytes, advanced);

namespace kudu {
namespace consensus {

void BufferData::resetBuffer(bool for_proxy, int64_t last_index) {
  msg_buffer_refs.clear();
  last_buffered = last_index;
  preceding_opid = {};
  buffered_for_proxying = for_proxy;
}

Status BufferData::readFromCache(
    const ReadContext& read_context,
    LogCache& log_cache) {
  bool buffer_empty = msg_buffer_refs.empty();
  OpId preceding_id;
  Status s = log_cache.ReadOps(
      last_buffered,
      FLAGS_max_buffer_fill_size_bytes,
      read_context,
      &msg_buffer_refs,
      &preceding_id);

  if (s.ok()) {
    if (!msg_buffer_refs.empty()) {
      last_buffered = msg_buffer_refs.back()->get()->id().index();
      buffered_for_proxying = read_context.route_via_proxy;
    }
    if (buffer_empty) {
      preceding_opid = std::move(preceding_id);
    }
  } else {
    resetBuffer();
  }

  return s;
}

} // namespace consensus
} // namespace kudu
