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

#include <string>
#include <sstream>
#include <fstream>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "olap/olap_meta.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/rowset/alpha_rowset.h"
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/txn_manager.h"
#include "olap/new_status.h"
#include "boost/filesystem.hpp"
#include "json2pb/json_to_pb.h"

#ifndef BE_TEST
#define BE_TEST
#endif

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

const std::string rowset_meta_path = "./be/test/olap/test_data/rowset_meta.json";
const std::string rowset_meta_path_2 = "./be/test/olap/test_data/rowset_meta2.json";

class TxnManagerTest : public testing::Test {
public:
    virtual void SetUp() {
        std::string meta_path = "./meta";
        boost::filesystem::remove_all("./meta");
        ASSERT_TRUE(boost::filesystem::create_directory(meta_path));
        _meta = new(std::nothrow) OlapMeta(meta_path);
        ASSERT_NE(nullptr, _meta);
        OLAPStatus st = _meta->init();
        ASSERT_TRUE(st == OLAP_SUCCESS);
        ASSERT_TRUE(boost::filesystem::exists("./meta"));
        load_id.set_hi(0);
        load_id.set_lo(0);
        // init rowset meta 1
        std::ifstream infile(rowset_meta_path);
        char buffer[1024];
        while (!infile.eof()) {
            infile.getline(buffer, 1024);
            _json_rowset_meta = _json_rowset_meta + buffer + "\n";
        }
        _json_rowset_meta = _json_rowset_meta.substr(0, _json_rowset_meta.size() - 1);

        uint64_t rowset_id = 10000;
        RowsetMetaSharedPtr rowset_meta(new AlphaRowsetMeta());
        rowset_meta->init_from_json(_json_rowset_meta);
        ASSERT_EQ(rowset_meta->rowset_id(), rowset_id);
        _alpha_rowset.reset(new AlphaRowset(nullptr, rowset_meta_path, nullptr, rowset_meta));
        _alpha_rowset_same_id.reset(new AlphaRowset(nullptr, rowset_meta_path, nullptr, rowset_meta));

        // init rowset meta 2
        _json_rowset_meta = "";
        std::ifstream infile2(rowset_meta_path_2);
        char buffer2[1024];
        while (!infile2.eof()) {
            infile2.getline(buffer2, 1024);
            _json_rowset_meta = _json_rowset_meta + buffer2 + "\n";
            std::cout << _json_rowset_meta << std::endl;
        }
        _json_rowset_meta = _json_rowset_meta.substr(0, _json_rowset_meta.size() - 1); 
        rowset_id = 10001;
        RowsetMetaSharedPtr rowset_meta2(new AlphaRowsetMeta());
        rowset_meta2->init_from_json(_json_rowset_meta);
        ASSERT_EQ(rowset_meta2->rowset_id(), rowset_id);
        _alpha_rowset_diff_id.reset(new AlphaRowset(nullptr, rowset_meta_path_2, nullptr, rowset_meta2));
        _tablet_uid = TabletUid(10, 10);
    }

    virtual void TearDown() {
        delete _meta;
        ASSERT_TRUE(boost::filesystem::remove_all("./meta"));
    }

private:
    OlapMeta* _meta;
    std::string _json_rowset_meta;
    TxnManager _txn_mgr;
    TPartitionId partition_id = 1123;
    TTransactionId transaction_id = 111;
    TTabletId tablet_id = 222;
    SchemaHash schema_hash = 333;
    TabletUid _tablet_uid;
    PUniqueId load_id;
    RowsetSharedPtr _alpha_rowset;
    RowsetSharedPtr _alpha_rowset_same_id;
    RowsetSharedPtr _alpha_rowset_diff_id;
};

TEST_F(TxnManagerTest, PrepareNewTxn) {
    OLAPStatus status = _txn_mgr.prepare_txn(partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid, load_id);
    ASSERT_TRUE(status == OLAP_SUCCESS);
}

// 1. prepare txn
// 2. commit txn
// 3. should be success
TEST_F(TxnManagerTest, CommitTxnWithPrepare) {
    OLAPStatus status = _txn_mgr.prepare_txn(partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid, load_id);
    _txn_mgr.commit_txn(_meta, partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid, load_id, _alpha_rowset, false);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    RowsetMetaSharedPtr rowset_meta(new AlphaRowsetMeta());
    status = RowsetMetaManager::get_rowset_meta(_meta, _tablet_uid, _alpha_rowset->rowset_id(), rowset_meta);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    ASSERT_TRUE(rowset_meta->rowset_id() == _alpha_rowset->rowset_id());
}

// 1. commit without prepare
// 2. should success
TEST_F(TxnManagerTest, CommitTxnWithNoPrepare) {
    OLAPStatus status = _txn_mgr.commit_txn(_meta, partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid, load_id, _alpha_rowset, false);
    ASSERT_TRUE(status == OLAP_SUCCESS);
}

// 1. commit twice with different rowset id
// 2. should failed
TEST_F(TxnManagerTest, CommitTxnTwiceWithDiffRowsetId) {
    OLAPStatus status = _txn_mgr.commit_txn(_meta, partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid, load_id, _alpha_rowset, false);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    status = _txn_mgr.commit_txn(_meta, partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid, load_id, _alpha_rowset_diff_id, false);
    ASSERT_TRUE(status != OLAP_SUCCESS);
}

// 1. commit twice with same rowset id
// 2. should success
TEST_F(TxnManagerTest, CommitTxnTwiceWithSameRowsetId) {
    OLAPStatus status = _txn_mgr.commit_txn(_meta, partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid, load_id, _alpha_rowset, false);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    status = _txn_mgr.commit_txn(_meta, partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid, load_id, _alpha_rowset_same_id, false);
    ASSERT_TRUE(status == OLAP_SUCCESS);
}

// 1. prepare twice should be success
TEST_F(TxnManagerTest, PrepareNewTxnTwice) {
    OLAPStatus status = _txn_mgr.prepare_txn(partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid, load_id);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    status = _txn_mgr.prepare_txn(partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid, load_id);
    ASSERT_TRUE(status == OLAP_SUCCESS);
}

// 1. txn could be rollbacked if it is not committed
TEST_F(TxnManagerTest, RollbackNotCommittedTxn) {
    OLAPStatus status = _txn_mgr.prepare_txn(partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid, load_id);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    status = _txn_mgr.rollback_txn(partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    RowsetMetaSharedPtr rowset_meta(new AlphaRowsetMeta());
    status = RowsetMetaManager::get_rowset_meta(_meta, _tablet_uid, _alpha_rowset->rowset_id(), rowset_meta);
    ASSERT_TRUE(status != OLAP_SUCCESS);
}

// 1. txn could not be rollbacked if it is committed
TEST_F(TxnManagerTest, RollbackCommittedTxn) {
    OLAPStatus status = _txn_mgr.commit_txn(_meta, partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid, load_id, _alpha_rowset, false);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    status = _txn_mgr.rollback_txn(partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid);
    ASSERT_FALSE(status == OLAP_SUCCESS);
    RowsetMetaSharedPtr rowset_meta(new AlphaRowsetMeta());
    status = RowsetMetaManager::get_rowset_meta(_meta, _tablet_uid, _alpha_rowset->rowset_id(), rowset_meta);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    ASSERT_TRUE(rowset_meta->rowset_id() == _alpha_rowset->rowset_id());
}

// 1. publish version success
TEST_F(TxnManagerTest, PublishVersionSuccessful) {
    OLAPStatus status = _txn_mgr.commit_txn(_meta, partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid, load_id, _alpha_rowset, false);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    Version new_version(10,11);
    VersionHash new_versionhash = 123;
    status = _txn_mgr.publish_txn(_meta, partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid, new_version, new_versionhash);
    ASSERT_TRUE(status == OLAP_SUCCESS);

    RowsetMetaSharedPtr rowset_meta(new AlphaRowsetMeta());
    status = RowsetMetaManager::get_rowset_meta(_meta, _tablet_uid, _alpha_rowset->rowset_id(), rowset_meta);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    ASSERT_TRUE(rowset_meta->rowset_id() == _alpha_rowset->rowset_id());
    ASSERT_TRUE(rowset_meta->start_version() == 10);
    ASSERT_TRUE(rowset_meta->end_version() == 11);
}

// 1. publish version failed if not found related txn and rowset
TEST_F(TxnManagerTest, PublishNotExistedTxn) {
    Version new_version(10,11);
    VersionHash new_versionhash = 123;
    OLAPStatus status = _txn_mgr.publish_txn(_meta, partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid, new_version, new_versionhash);
    ASSERT_TRUE(status != OLAP_SUCCESS);
}

TEST_F(TxnManagerTest, DeletePreparedTxn) {
    OLAPStatus status = _txn_mgr.prepare_txn(partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid, load_id);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    status = _txn_mgr.delete_txn(_meta, partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid);
    ASSERT_TRUE(status == OLAP_SUCCESS);
}

TEST_F(TxnManagerTest, DeleteCommittedTxn) {
    OLAPStatus status = _txn_mgr.commit_txn(_meta, partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid, load_id, _alpha_rowset, false);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    RowsetMetaSharedPtr rowset_meta(new AlphaRowsetMeta());
    status = RowsetMetaManager::get_rowset_meta(_meta, _tablet_uid, _alpha_rowset->rowset_id(), rowset_meta);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    status = _txn_mgr.delete_txn(_meta, partition_id, transaction_id, 
        tablet_id, schema_hash, _tablet_uid);
    ASSERT_TRUE(status == OLAP_SUCCESS);
    RowsetMetaSharedPtr rowset_meta2(new AlphaRowsetMeta());
    status = RowsetMetaManager::get_rowset_meta(_meta, _tablet_uid, _alpha_rowset->rowset_id(), rowset_meta2);
    ASSERT_TRUE(status != OLAP_SUCCESS);
}

}  // namespace doris

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
