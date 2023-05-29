// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/tserver/tablet_server.h"

#include <cstddef>
#include <ostream>
#include <type_traits>
#include <utility>

#include <glog/logging.h>

#ifdef FB_DO_NOT_REMOVE
#include "kudu/cfile/block_cache.h" // @manual
#endif
#include "kudu/consensus/consensus.service.h"
#include "kudu/fs/error_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/service_pool.h"
#ifdef FB_DO_NOT_REMOVE
#include "kudu/tserver/heartbeater.h" // @manual
#include "kudu/tserver/scanners.h" // @manual
#include "kudu/tserver/tablet_copy_service.h" // @manual
#endif
#include "kudu/tserver/consensus_service.h"
#include "kudu/tserver/simple_tablet_manager.h"
#ifdef FB_DO_NOT_REMOVE
#include "kudu/tserver/tserver_path_handlers.h" // @manual
#include "kudu/util/maintenance_manager.h" // @manual
#endif
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

using kudu::rpc::ServiceIf;
using std::string;
using std::unique_ptr;

namespace kudu {
namespace tserver {

std::string RaftConsensusServerIf::ConsensusServiceRpcQueueToString() const {
  const kudu::rpc::ServicePool* pool = rpc_server_->service_pool(
      kudu::consensus::ConsensusServiceIf::static_service_name());
  if (pool) {
    return pool->RpcServiceQueueToString();
  }
  return "";
}

/*static*/ Status RaftConsensusServerIf::ShowKuduThreadStatus(
    std::vector<ThreadDescriptor>* threads) {
  return GlobalShowThreadStatus(threads);
}

// Change thread priority for a particular category, this not only changes the
// current threads belong to that category, but also future threads spawned in
// that category.
//
// @param category In the other words, thread pool name
// @param priority thread priority based on nice. Should be -20 to 19
// @return Status:OK if succeed
/*static*/ Status RaftConsensusServerIf::ChangeKuduThreadPriority(
    const std::string& pool,
    int priority) {
  return GlobalChangeThreadPriority(pool, priority);
}

RaftConsensusServerIf::RaftConsensusServerIf(
    const std::string& name,
    const server::ServerBaseOptions& opts,
    const std::string& metrics_namespace)
    : kserver::KuduServer(name, opts, metrics_namespace) {}

TabletServer::TabletServer(const TabletServerOptions& opts)
    : RaftConsensusServerIf("TabletServer", opts, "kudu.tabletserver"),
      initted_(false),
#ifdef FB_DO_NOT_REMOVE
      fail_heartbeats_for_tests_(false),
#endif
      opts_(opts),
      tablet_manager_(new TSTabletManager(this))

#ifdef FB_DO_NOT_REMOVE
          scanner_manager_(new ScannerManager(metric_entity())),
      path_handlers_(new TabletServerPathHandlers(this))
#endif
{
}

TabletServer::TabletServer(
    const TabletServerOptions& opts,
    const std::function<std::unique_ptr<TabletManagerIf>(TabletServer&)>&
        factory)
    : RaftConsensusServerIf("TabletServer", opts, "kudu.tabletserver"),
      initted_(false),
      opts_(opts),
      tablet_manager_(factory(*this)) {}

TabletServer::~TabletServer() {
  Shutdown();
}

string TabletServer::ToString() const {
  // TODO: include port numbers, etc.
  return "TabletServer";
}

#ifdef FB_DO_NOT_REMOVE
Status TabletServer::ValidateMasterAddressResolution() const {
  for (const HostPort& master_addr : opts_.master_addresses) {
    RETURN_NOT_OK_PREPEND(
        master_addr.ResolveAddresses(NULL),
        strings::Substitute(
            "Couldn't resolve master service address '$0'",
            master_addr.ToString()));
  }
  return Status::OK();
}
#endif

Status TabletServer::Init() {
  CHECK(!initted_);

#ifdef FB_DO_NOT_REMOVE
  cfile::BlockCache::GetSingleton()->StartInstrumentation(metric_entity());

  // Validate that the passed master address actually resolves.
  // We don't validate that we can connect at this point -- it should
  // be allowed to start the TS and the master in whichever order --
  // our heartbeat thread will loop until successfully connecting.
  RETURN_NOT_OK(ValidateMasterAddressResolution());
#endif

  // This pool will be used to wait for Raft
  RETURN_NOT_OK(
      ThreadPoolBuilder("init").set_max_threads(1).Build(&init_pool_));

  // Initialize FS, rpc_server, rpc messenger and Raft pool
  RETURN_NOT_OK(KuduServer::Init());

#ifdef FB_DO_NOT_REMOVE
  if (web_server_) {
    RETURN_NOT_OK(path_handlers_->Register(web_server_.get()));
  }

  maintenance_manager_.reset(new MaintenanceManager(
      MaintenanceManager::kDefaultOptions, fs_manager_->uuid()));

  heartbeater_.reset(new Heartbeater(opts_, this));
  RETURN_NOT_OK_PREPEND(
      scanner_manager_->StartRemovalThread(),
      "Could not start expired Scanner removal thread");
#endif

  // Moving registration of consensus service and RPC server
  // start to Init. This allows us to create a barebones Raft
  // distributed config. We need the service to be here, because
  // Raft::create makes remote GetNodeInstance RPC calls.
  unique_ptr<ServiceIf> consensus_service(
      new ConsensusServiceImpl(this, *tablet_manager_));
  RETURN_NOT_OK(RegisterService(std::move(consensus_service)));
  RETURN_NOT_OK(KuduServer::Start());

  // Moving tablet manager initialization to Init phase of
  // tablet server
  if (tablet_manager_->IsInitialized()) {
    return Status::IllegalState("Catalog manager is already initialized");
  }
  RETURN_NOT_OK_PREPEND(
      tablet_manager_->Init(is_first_run_),
      "Unable to initialize catalog manager");

  google::FlushLogFiles(google::INFO); // Flush the startup messages.
  initted_ = true;
  return Status::OK();
}

Status TabletServer::Start() {
  CHECK(initted_);

#ifdef FB_DO_NOT_REMOVE
  fs_manager_->SetErrorNotificationCb(
      ErrorHandlerType::DISK_ERROR,
      Bind(
          &TSTabletManager::FailTabletsInDataDir,
          Unretained(tablet_manager_.get())));
  fs_manager_->SetErrorNotificationCb(
      ErrorHandlerType::CFILE_CORRUPTION,
      Bind(
          &TSTabletManager::FailTabletAndScheduleShutdown,
          Unretained(tablet_manager_.get())));

  unique_ptr<ServiceIf> ts_service(new TabletServiceImpl(this));
  unique_ptr<ServiceIf> admin_service(new TabletServiceAdminImpl(this));

  unique_ptr<ServiceIf> tablet_copy_service(
      new TabletCopyServiceImpl(this, tablet_manager_.get()));

  RETURN_NOT_OK(RegisterService(std::move(ts_service)));
  RETURN_NOT_OK(RegisterService(std::move(admin_service)));

  RETURN_NOT_OK(RegisterService(std::move(tablet_copy_service)));

  RETURN_NOT_OK(heartbeater_->Start());
  RETURN_NOT_OK(maintenance_manager_->Start());
#endif

  if (!tablet_manager_->IsInitialized()) {
    return Status::IllegalState("Tablet manager is not initialized");
  }

  RETURN_NOT_OK_PREPEND(
      tablet_manager_->Start(is_first_run_),
      "Unable to start raft in tablet manager");
  google::FlushLogFiles(google::INFO); // Flush the startup messages.
  return Status::OK();
}

void TabletServer::Shutdown() {
  if (initted_) {
    string name = ToString();
    LOG(INFO) << name << " shutting down...";

    // 1. Stop accepting new RPCs.
    UnregisterAllServices();

#ifdef FB_DO_NOT_REMOVE
    // 2. Shut down the tserver's subsystems.
    maintenance_manager_->Shutdown();
    WARN_NOT_OK(heartbeater_->Stop(), "Failed to stop TS Heartbeat thread");
    fs_manager_->UnsetErrorNotificationCb(ErrorHandlerType::DISK_ERROR);
    fs_manager_->UnsetErrorNotificationCb(ErrorHandlerType::CFILE_CORRUPTION);
#endif
    tablet_manager_->Shutdown();

    // 3. Shut down generic subsystems.
    KuduServer::Shutdown();
    LOG(INFO) << name << " shutdown complete.";
    initted_ = false;
  }
}

} // namespace tserver
} // namespace kudu
