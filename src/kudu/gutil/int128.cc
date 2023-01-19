// Copyright 2004 Google Inc.
// All Rights Reserved.
//
//

#include "kudu/gutil/int128.h"
#include <iostream>
#include "kudu/gutil/integral_types.h"

namespace kudu {

const uint128_pod kuint128max = {
    static_cast<uint64>(GG_LONGLONG(0xFFFFFFFFFFFFFFFF)),
    static_cast<uint64>(GG_LONGLONG(0xFFFFFFFFFFFFFFFF))};

std::ostream& operator<<(std::ostream& o, const kudu::uint128& b) {
  return (o << b.hi_ << "::" << b.lo_);
}

} // namespace kudu
