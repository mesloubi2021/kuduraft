// Copyright (c) Meta Platforms, Inc. and affiliates.

#pragma once

#include <folly/Synchronized.h>
#include <gflags/gflags.h>
#include <future>
#include <optional>
#include <vector>

#include "kudu/consensus/log.h"
#include "kudu/consensus/log_cache.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/ref_counted_replicate.h"
#include "kudu/util/debouncer.h"
#include "kudu/util/status.h"

namespace kudu {
namespace consensus {

/**
 * A basic data structure to hold pointers to the buffered replicates, as well
 * as some markers on where the buffer starts.
 */
class BufferData {
 public:
  /**
   * Clears the buffer and resets its metadata.
   *
   * @param last_index The index to reset the buffer to
   */
  void resetBuffer(bool for_proxy = false, int64_t last_index = -1);

  /**
   * Reads ops from the LogCache into this buffer.
   *
   * This method will continue reading from last_index() onwards unless the
   * buffer is not initialized (last_index() == -1)
   *
   * @param read_context Context on where to start reading and if we need
   *                     proxying
   * @param log_cache A reference to the log cache
   * @return OK if we read everything, Incomplete if the first index requested
   *         is not in the cache yet
   */
  Status readFromCache(const ReadContext& read_context, LogCache& log_cache);

  /**
   * First index in the buffer.
   *
   * @return the first index or -1 if buffer is empty
   */
  int64_t first_index() const {
    return msg_buffer_refs.empty()
        ? -1
        : msg_buffer_refs.front()->get()->id().index();
  }

  /**
   * The last index we've bufferred until. This index may not be in the message
   * buffer itself, but it's where we'll pick up bufferring from.
   *
   * @return The last bufferred index or -1 if nothing has been bufferred yet
   */
  int64_t last_index() const {
    return last_buffered;
  }

  /**
   * If the buffer is empty.
   *
   * @return true if buffer is empty
   */
  bool empty() const {
    return last_buffered == -1 || msg_buffer_refs.empty();
  }

  /**
   * If we bufferred ops that are meant for proxying.
   *
   * Proxyed ops may not be compressed and may not have checksums.
   *
   * @return true if the bufferred data was meant for proxying
   */
  bool for_proxying() const {
    return buffered_for_proxying;
  }

 protected:
  /**
   * The vector of bufferred replicates.
   */
  std::vector<ReplicateRefPtr> msg_buffer_refs;
  /**
   * The preceding_opid (contains both term and index) of the replicate before
   * the start of the buffer.
   */
  OpId preceding_opid;
  /**
   * The last index we buffered. We should pick up buffering from here. Will be
   * -1 at the start. Could refer to a previous buffer point if the buffer was
   * flushed out to a RPC.
   */
  int64_t last_buffered = -1;
  /**
   * If we buffered data that was meant for proxying.
   */
  bool buffered_for_proxying = false;
};

/**
 * BufferData synchronized on a debouncer.
 */
using PeerMessageBuffer = folly::Synchronized<BufferData, MutexDebouncer>;

} // namespace consensus
} // namespace kudu
