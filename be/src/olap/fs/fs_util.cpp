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

#include "olap/fs/fs_util.h"

#include "common/status.h"
#include "env/env.h"
#include "olap/fs/file_block_manager.h"

namespace doris {
namespace fs {
namespace fs_util {

BlockManager* file_block_mgr(Env* env, BlockManagerOptions opts) {
    return new FileBlockManager(env, std::move(opts));
}

BlockManager* block_mgr_for_ut() {
    fs::BlockManagerOptions bm_opts;
    bm_opts.read_only = false;
    return file_block_mgr(Env::Default(), bm_opts);
}

} // namespace fs_util
} // namespace fs
} // namespace doris
