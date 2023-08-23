// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#ifndef KUDU_FLAGS_H
#define KUDU_FLAGS_H

#define FLAGS_HANDLER(m) m
#define DEFINE_HANDLER(type, name, val, help) DEFINE_##type(name, val, help)

#define DFLAGS_HANDLER(m) m
#define DEFINE_DHANDLER(type, name, val, help) DEFINE_##type(name, val, help)

#endif
