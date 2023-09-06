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
  msg_buffer_refs = {};
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
  } else if (!s.IsIncomplete()) { // Incomplete is returned op is pending
                                  // append, we don't need to reset
    resetBuffer();
  }

  return s;
}

BufferData BufferData::moveDataAndReset() {
  BufferData return_data;
  return_data.last_buffered = last_buffered;
  return_data.preceding_opid = std::move(preceding_opid);
  return_data.msg_buffer_refs = std::move(msg_buffer_refs);
  return_data.buffered_for_proxying = buffered_for_proxying;

  resetBuffer(buffered_for_proxying, last_buffered);

  return return_data;
}

PeerMessageBuffer::LockedBufferHandle::LockedBufferHandle(
    PeerMessageBuffer& message_buffer,
    SynchronizedBufferData::TryLockedPtr&& locked_ptr)
    : SynchronizedBufferData::TryLockedPtr(std::move(locked_ptr)),
      message_buffer_(message_buffer) {}

std::optional<int64_t>
PeerMessageBuffer::LockedBufferHandle::getIndexForHandoff() {
  return message_buffer_.getIndexForHandoff();
}

bool PeerMessageBuffer::LockedBufferHandle::proxyRequirementSatisfied() const {
  const LockedBufferHandle& self = (*this);
  return message_buffer_.getProxyOpsNeeded() == self->for_proxying();
}

void PeerMessageBuffer::LockedBufferHandle::fulfillPromiseWithBuffer(Status s) {
  LockedBufferHandle& self = (*this);
  message_buffer_.handoff_promise_.set_value(
      {std::move(s), self->moveDataAndReset()});
}

PeerMessageBuffer::LockedBufferHandle PeerMessageBuffer::tryLock() {
  return LockedBufferHandle(*this, data_.tryLock());
}

std::optional<int64_t> PeerMessageBuffer::getIndexForHandoff() {
  int64_t initial_index = handoff_initial_index_.exchange(-1);

  if (initial_index == -1) {
    return std::nullopt;
  } else {
    return initial_index;
  }
}

bool PeerMessageBuffer::getProxyOpsNeeded() const {
  return proxy_ops_needed_;
}

} // namespace consensus
} // namespace kudu
