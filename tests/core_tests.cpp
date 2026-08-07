#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <rocksdb/options.h>
#include <sys/wait.h>
#include <unistd.h>

#include "object_record.pb.h"
#include "server/server_config.h"
#include "service/grpc_services.h"
#include "service/open_event_core.h"
#include "storage/encoding.h"
#include "storage/io_error.h"
#include "storage/object_file_store.h"
#include "storage/unified_storage.h"

namespace {

void Check(bool condition, const std::string& message)
{
    if (!condition) {
        std::cerr << "FAILED: " << message << "\n";
        std::exit(1);
    }
}

std::unique_ptr<openevent::OpenEventCore> MakeCore(const std::filesystem::path& root,
                                                   size_t max_payload_bytes = 1024,
                                                   uint64_t max_scan_records = 10000,
                                                   size_t response_soft_limit_bytes = 0,
                                                   openevent::OpenEventCore::FatalErrorHandler fatal_handler = {},
                                                   openevent::StorageFaultInjector fault_injector = {})
{
    auto storage = openevent::UnifiedStorage::Open(
        (root / "data").string(), std::move(fault_injector));
    Check(storage.ok(), storage.status().message());
    return std::make_unique<openevent::OpenEventCore>(
        std::move(storage.value()),
        max_payload_bytes,
        max_scan_records,
        response_soft_limit_bytes,
        std::move(fatal_handler));
}

rocksdb::WriteOptions SyncWriteOptions()
{
    rocksdb::WriteOptions options;
    options.sync = true;
    return options;
}

void CreatePartialStorage(const std::filesystem::path& path,
                          bool write_marker,
                          bool create_messages_cf,
                          bool create_objects_cf = false)
{
    std::error_code ec;
    std::filesystem::create_directories(path / "db", ec);
    Check(!ec, "create partial storage database directory: " + ec.message());
    std::filesystem::create_directory(path / "objects", ec);
    Check(!ec, "create partial storage object directory: " + ec.message());

    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* raw_db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(options, (path / "db").string(), &raw_db);
    Check(status.ok(), status.ToString());
    std::unique_ptr<rocksdb::DB> db(raw_db);

    if (write_marker) {
        status = db->Put(SyncWriteOptions(), "meta:init_state", openevent::EncodeUint64(1));
        Check(status.ok(), status.ToString());
    }
    if (create_messages_cf) {
        rocksdb::ColumnFamilyHandle* messages = nullptr;
        status = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "messages", &messages);
        Check(status.ok(), status.ToString());
        status = db->DestroyColumnFamilyHandle(messages);
        Check(status.ok(), status.ToString());
    }
    if (create_objects_cf) {
        rocksdb::ColumnFamilyHandle* objects = nullptr;
        status = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "objects", &objects);
        Check(status.ok(), status.ToString());
        status = db->DestroyColumnFamilyHandle(objects);
        Check(status.ok(), status.ToString());
    }
}

void PutRawRecord(const std::filesystem::path& path,
                  const std::string& column_family,
                  const std::string& key,
                  const std::string& value)
{
    rocksdb::Options list_options;
    std::vector<std::string> names;
    rocksdb::Status status = rocksdb::DB::ListColumnFamilies(list_options, path.string(), &names);
    Check(status.ok(), status.ToString());

    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
    for (const auto& name : names) {
        descriptors.emplace_back(name, rocksdb::ColumnFamilyOptions());
    }
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::DB* raw_db = nullptr;
    status = rocksdb::DB::Open(rocksdb::DBOptions(), path.string(), descriptors, &handles, &raw_db);
    Check(status.ok(), status.ToString());
    std::unique_ptr<rocksdb::DB> db(raw_db);

    auto name_it = std::find(names.begin(), names.end(), column_family);
    Check(name_it != names.end(), "column family not found: " + column_family);
    const size_t index = static_cast<size_t>(std::distance(names.begin(), name_it));
    status = db->Put(SyncWriteOptions(), handles[index], key, value);
    Check(status.ok(), status.ToString());

    for (auto* handle : handles) {
        status = db->DestroyColumnFamilyHandle(handle);
        Check(status.ok(), status.ToString());
    }
}

uint64_t NowMs()
{
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

std::string AddToken(openevent::OpenEventCore& core, uint64_t principal)
{
    openevent::AddTokenRequest request;
    request.set_target_principal(principal);
    openevent::AddTokenResponse response;
    openevent::Status status = core.AddToken(request, &response);
    Check(status.ok(), status.message());
    return response.binding().token();
}

uint64_t CreateChannel(openevent::OpenEventCore& core, uint64_t principal, const std::string& token,
                       openevent::Visibility visibility)
{
    openevent::CreateChannelRequest request;
    request.set_principal(principal);
    request.set_token(token);
    request.set_name("test");
    request.set_visibility(visibility);
    request.set_protocol("raw");

    openevent::CreateChannelResponse response;
    openevent::Status status = core.CreateChannel(request, &response);
    Check(status.ok(), status.message());
    return response.channel().channel_id();
}

void PublishAuto(openevent::OpenEventCore& core, uint64_t principal, const std::string& token,
                 uint64_t channel_id, const std::string& payload)
{
    openevent::PublishAutoSeqRequest request;
    request.set_principal(principal);
    request.set_token(token);
    request.set_channel_id(channel_id);
    request.set_payload(payload);
    openevent::PublishAutoSeqResponse response;
    openevent::Status status = core.PublishAutoSeq(request, &response);
    Check(status.ok(), status.message());
}

openevent::WriteObjectRequest MakeWriteObjectRequest(uint64_t principal,
                                                     const std::string& token,
                                                     const std::string& data)
{
    openevent::WriteObjectRequest request;
    request.set_principal(principal);
    request.set_token(token);
    request.set_name("object.bin");
    request.set_type("application/octet-stream");
    request.set_description("test object");
    request.set_data(data);
    return request;
}

openevent::WriteObjectResponse WriteObject(openevent::OpenEventCore& core,
                                           uint64_t principal,
                                           const std::string& token,
                                           const std::string& data)
{
    openevent::WriteObjectRequest request = MakeWriteObjectRequest(principal, token, data);
    openevent::WriteObjectResponse response;
    openevent::Status status = core.WriteObject(request, &response);
    Check(status.ok(), status.message());
    return response;
}

void CheckRecoveredObjectState(const std::filesystem::path& root,
                               bool expected_committed,
                               uint64_t expected_next_id,
                               const std::string& expected_data = "fault-data")
{
    auto storage = openevent::UnifiedStorage::Open((root / "data").string());
    Check(storage.ok(), storage.status().message());
    auto snapshot = storage.value()->CreateSnapshot();
    Check(snapshot.ok(), snapshot.status().message());
    auto preparing = storage.value()->GetPreparingObject(snapshot.value(), 1);
    auto committed = storage.value()->GetCommittedObject(snapshot.value(), 1);
    auto next_id = storage.value()->GetNextObjectId(snapshot.value());
    Check(preparing.ok() && !preparing.value().has_value(),
          "recovery must remove PREPARING metadata");
    Check(committed.ok() && committed.value().has_value() == expected_committed,
          "recovered committed state does not match the injected persistence point");
    Check(next_id.ok() && next_id.value() == expected_next_id,
          "object ID watermark does not match the injected persistence point");
    const auto final_path = root / "data" / "objects" / "1";
    const auto temporary_path = root / "data" / "objects" / ".tmp.1";
    Check(!std::filesystem::exists(temporary_path), "recovery must remove the temporary object file");
    Check(std::filesystem::exists(final_path) == expected_committed,
          "recovered object file visibility must match COMMITTED metadata");
    if (expected_committed) {
        auto data = storage.value()->ReadObjectFile(
            committed.value().value(), 0, committed.value().value().nbytes);
        Check(data.ok() && data.value() == expected_data,
              "committed object must remain intact after an injected failure or crash");
    }
}

void TestPublishFetch()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_publish_fetch";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);

    std::string token = AddToken(*core, 100);
    uint64_t channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);
    const uint64_t before_publish_ms = NowMs();
    PublishAuto(*core, 100, token, channel_id, "hello");
    const uint64_t after_publish_ms = NowMs();

    openevent::FetchRequest fetch;
    fetch.set_principal(100);
    fetch.set_token(token);
    fetch.set_from_seq(1);
    fetch.set_limit(10);

    openevent::FetchResponse response;
    openevent::Status status = core->Fetch(fetch, &response);
    Check(status.ok(), status.message());
    Check(response.messages_size() == 1, "expected one fetched message");
    Check(response.messages(0).seq() == 1, "expected seq 1");
    Check(response.messages(0).payload() == "hello", "expected payload");
    Check(response.messages(0).ts_ms() >= before_publish_ms,
          "expected server timestamp to be captured after request receipt");
    Check(response.messages(0).ts_ms() <= after_publish_ms,
          "expected server timestamp to be captured before publish returned");
    Check(response.next_seq() == 2, "expected next_seq 2");
    Check(response.last_seq() == 1, "expected last_seq 1");
    Check(response.next_seq() > response.last_seq(), "expected no more data");

    std::filesystem::remove_all(root);
}

void TestCasAbort()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_cas_abort";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);

    std::string token = AddToken(*core, 100);
    uint64_t channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);

    openevent::PublishRequest publish;
    publish.set_principal(100);
    publish.set_token(token);
    publish.set_channel_id(channel_id);
    publish.set_seq(2);
    publish.set_payload("bad");
    openevent::PublishResponse response;
    openevent::Status status = core->Publish(publish, &response);
    Check(!status.ok() && status.code() == grpc::StatusCode::ABORTED, "expected aborted CAS publish");

    std::filesystem::remove_all(root);
}

void TestPrivateAcl()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_private_acl";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);

    std::string owner_token = AddToken(*core, 100);
    std::string other_token = AddToken(*core, 200);
    uint64_t channel_id = CreateChannel(*core, 100, owner_token, openevent::VISIBILITY_PRIVATE);
    PublishAuto(*core, 100, owner_token, channel_id, "secret");

    openevent::FetchRequest fetch;
    fetch.set_principal(200);
    fetch.set_token(other_token);
    fetch.set_from_seq(1);
    fetch.set_limit(10);
    openevent::FetchResponse response;
    openevent::Status status = core->Fetch(fetch, &response);
    Check(status.ok(), status.message());
    Check(response.messages_size() == 0, "private message must be filtered");
    Check(response.next_seq() == 2, "next_seq should still advance globally");
    Check(response.last_seq() == 1, "last_seq should report committed tail");

    fetch.add_channels(channel_id);
    response.Clear();
    status = core->Fetch(fetch, &response);
    Check(!status.ok() && status.code() == grpc::StatusCode::PERMISSION_DENIED,
          "explicit unreadable Fetch channel must be rejected");

    google::protobuf::RepeatedField<uint64_t> subscription_channels;
    subscription_channels.Add(channel_id);
    uint64_t subscription_max_seq = 0;
    status = core->GetSubscriptionMaxSeq(200, other_token, subscription_channels, &subscription_max_seq);
    Check(!status.ok() && status.code() == grpc::StatusCode::PERMISSION_DENIED,
          "explicit unreadable subscription channel must be rejected at startup");
    response.Clear();
    status = core->FetchSubscriptionBatch(
        200, other_token, 1, 10, false, subscription_channels, &response);
    Check(!status.ok() && status.code() == grpc::StatusCode::PERMISSION_DENIED,
          "explicit unreadable subscription channel must be rejected");

    openevent::AddMemberRequest add;
    add.set_principal(100);
    add.set_token(owner_token);
    add.set_channel_id(channel_id);
    add.set_target_principal(200);
    openevent::AddMemberResponse add_response;
    status = core->AddMember(add, &add_response);
    Check(status.ok(), status.message());

    response.Clear();
    status = core->Fetch(fetch, &response);
    Check(status.ok(), status.message());
    Check(response.messages_size() == 1, "member should see private message");

    fetch.clear_channels();

    std::filesystem::remove_all(root);
}

void TestFetchChannelFilter()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_fetch_channels";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);

    std::string token = AddToken(*core, 100);
    uint64_t first_channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);
    uint64_t second_channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);
    PublishAuto(*core, 100, token, first_channel_id, "first-channel");
    PublishAuto(*core, 100, token, second_channel_id, "second-channel");

    openevent::FetchRequest fetch;
    fetch.set_principal(100);
    fetch.set_token(token);
    fetch.set_from_seq(1);
    fetch.set_limit(10);
    fetch.add_channels(first_channel_id);

    openevent::FetchResponse response;
    openevent::Status status = core->Fetch(fetch, &response);
    Check(status.ok(), status.message());
    Check(response.messages_size() == 1, "channel filter should return one message");
    Check(response.messages(0).channel_id() == first_channel_id, "channel filter should keep first channel");
    Check(response.messages(0).payload() == "first-channel", "channel filter should keep first payload");
    Check(response.next_seq() == 3, "channel filter should still advance globally");
    Check(response.last_seq() == 2, "last_seq should report committed tail");

    google::protobuf::RepeatedField<uint64_t> subscription_channels;
    subscription_channels.Add(second_channel_id);
    response.Clear();
    status = core->FetchSubscriptionBatch(
        100, token, 1, 10, false, subscription_channels, &response);
    Check(status.ok(), status.message());
    Check(response.messages_size() == 1,
          "subscription channel filter should return one message");
    Check(response.messages(0).channel_id() == second_channel_id,
          "subscription channel filter should keep the selected channel");
    Check(response.messages(0).payload() == "second-channel",
          "subscription channel filter should keep the selected payload");
    Check(response.next_seq() == 3,
          "subscription channel filter should advance past filtered messages");
    Check(response.last_seq() == 2,
          "subscription channel filter should report the committed tail");

    fetch.clear_channels();
    response.Clear();
    status = core->Fetch(fetch, &response);
    Check(status.ok(), status.message());
    Check(response.messages_size() == 2, "empty channel filter should return all visible messages");
    Check(response.last_seq() == 2, "empty channel filter last_seq");

    fetch.clear_channels();
    fetch.add_channels(std::numeric_limits<uint64_t>::max());
    response.Clear();
    status = core->Fetch(fetch, &response);
    Check(!status.ok() && status.code() == grpc::StatusCode::NOT_FOUND,
          "explicit missing Fetch channel must be rejected");

    std::filesystem::remove_all(root);
}

void TestSharedSubscriptionSnapshotRefresh()
{
    const auto root =
        std::filesystem::temp_directory_path() / "openevent_shared_subscription_snapshot";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);
    const std::string token = AddToken(*core, 100);
    const uint64_t channel_id =
        CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);

    uint64_t max_seq = 0;
    google::protobuf::RepeatedField<uint64_t> no_channels;
    openevent::Status status = core->GetSubscriptionMaxSeq(100, token, no_channels, &max_seq);
    Check(status.ok() && max_seq == 0,
          "subscription startup must wait for the shared snapshot to cover prior commits");
    const uint64_t settled_version = core->SubscriptionSnapshotVersion();
    Check(!core->WaitForSubscriptionSnapshot(settled_version, std::chrono::milliseconds(150)),
          "the server must not publish a new subscription snapshot without a commit");

    PublishAuto(*core, 100, token, channel_id, "shared-snapshot");
    Check(core->WaitForSubscriptionSnapshot(settled_version, std::chrono::seconds(1)),
          "a successful commit must be published by the periodic snapshot refresh");

    google::protobuf::RepeatedField<uint64_t> channels;
    openevent::FetchResponse response;
    uint64_t used_version = 0;
    status = core->FetchSubscriptionBatch(
        100, token, 1, 10, false, channels, &response, &used_version);
    Check(status.ok() && response.messages_size() == 1 &&
              response.messages(0).payload() == "shared-snapshot" &&
              used_version != settled_version,
          "all subscription reads must use the refreshed shared snapshot");

    std::filesystem::remove_all(root);
}

void TestRecipientFilter()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_recipient";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);

    std::string token = AddToken(*core, 100);
    uint64_t channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);

    openevent::AddMemberRequest add;
    add.set_principal(100);
    add.set_token(token);
    add.set_channel_id(channel_id);
    add.set_target_principal(200);
    openevent::AddMemberResponse add_response;
    openevent::Status status = core->AddMember(add, &add_response);
    Check(status.ok(), status.message());

    openevent::PublishAutoSeqRequest request;
    request.set_principal(100);
    request.set_token(token);
    request.set_channel_id(channel_id);
    request.add_recipients(200);
    request.set_payload("direct");
    openevent::PublishAutoSeqResponse publish_response;
    status = core->PublishAutoSeq(request, &publish_response);
    Check(status.ok(), status.message());

    openevent::FetchRequest fetch;
    fetch.set_principal(100);
    fetch.set_token(token);
    fetch.set_from_seq(1);
    fetch.set_limit(10);
    fetch.set_only_my_recipient(true);
    openevent::FetchResponse response;
    status = core->Fetch(fetch, &response);
    Check(status.ok(), status.message());
    Check(response.messages_size() == 0, "recipient filter should hide non-recipient message");

    std::filesystem::remove_all(root);
}

void TestRecipientMustBeChannelMember()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_recipient_membership";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);

    std::string token = AddToken(*core, 100);
    uint64_t channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);

    openevent::PublishAutoSeqRequest auto_request;
    auto_request.set_principal(100);
    auto_request.set_token(token);
    auto_request.set_channel_id(channel_id);
    auto_request.add_recipients(200);
    auto_request.set_payload("direct");
    openevent::PublishAutoSeqResponse auto_response;
    openevent::Status status = core->PublishAutoSeq(auto_request, &auto_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "recipient outside channel members should be rejected");

    openevent::PublishRequest publish;
    publish.set_principal(100);
    publish.set_token(token);
    publish.set_channel_id(channel_id);
    publish.set_seq(1);
    publish.add_recipients(200);
    publish.set_payload("direct");
    openevent::PublishResponse publish_response;
    status = core->Publish(publish, &publish_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "CAS publish recipient outside channel members should be rejected");

    auto max_seq = core->MaxSeq();
    Check(max_seq.ok(), max_seq.status().message());
    Check(max_seq.value() == 0, "invalid recipient must not advance max_seq");

    std::filesystem::remove_all(root);
}

void TestPayloadLimit()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_payload_limit";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root, 4);

    std::string token = AddToken(*core, 100);
    uint64_t channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);

    openevent::PublishAutoSeqRequest auto_request;
    auto_request.set_principal(100);
    auto_request.set_token(token);
    auto_request.set_channel_id(channel_id);
    auto_request.set_payload("1234");
    openevent::PublishAutoSeqResponse auto_response;
    openevent::Status status = core->PublishAutoSeq(auto_request, &auto_response);
    Check(status.ok(), status.message());
    Check(auto_response.seq() == 1, "payload at max limit should publish");

    auto_request.set_payload("12345");
    auto_response.Clear();
    status = core->PublishAutoSeq(auto_request, &auto_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::RESOURCE_EXHAUSTED,
          "oversized PublishAutoSeq payload should return RESOURCE_EXHAUSTED");

    openevent::PublishRequest publish;
    publish.set_principal(100);
    publish.set_token(token);
    publish.set_channel_id(channel_id);
    publish.set_seq(2);
    publish.set_payload("12345");
    openevent::PublishResponse publish_response;
    status = core->Publish(publish, &publish_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::RESOURCE_EXHAUSTED,
          "oversized Publish payload should return RESOURCE_EXHAUSTED");

    auto max_seq = core->MaxSeq();
    Check(max_seq.ok(), max_seq.status().message());
    Check(max_seq.value() == 1, "oversized payload must not advance max_seq");

    std::filesystem::remove_all(root);
}

void TestGuaranteedNonCommitStatuses()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_non_commit_statuses";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root, 4);

    std::string owner_token = AddToken(*core, 100);
    std::string outsider_token = AddToken(*core, 200);
    uint64_t channel_id = CreateChannel(*core, 100, owner_token, openevent::VISIBILITY_PROTECTED);

    openevent::PublishAutoSeqRequest request;
    request.set_principal(100);
    request.set_token("invalid-token");
    request.set_channel_id(channel_id);
    request.set_payload("ok");
    openevent::PublishAutoSeqResponse response;
    openevent::Status status = core->PublishAutoSeq(request, &response);
    Check(!status.ok() && status.code() == grpc::StatusCode::UNAUTHENTICATED,
          "invalid token should guarantee non-commit");

    request.set_principal(200);
    request.set_token(outsider_token);
    status = core->PublishAutoSeq(request, &response);
    Check(!status.ok() && status.code() == grpc::StatusCode::PERMISSION_DENIED,
          "channel ACL denial should guarantee non-commit");

    request.set_principal(100);
    request.set_token(owner_token);
    request.set_channel_id(channel_id + 1000);
    status = core->PublishAutoSeq(request, &response);
    Check(!status.ok() && status.code() == grpc::StatusCode::NOT_FOUND,
          "missing channel should guarantee non-commit");

    request.set_channel_id(channel_id);
    request.add_recipients(200);
    status = core->PublishAutoSeq(request, &response);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "invalid recipient should guarantee non-commit");

    request.clear_recipients();
    request.set_payload("12345");
    status = core->PublishAutoSeq(request, &response);
    Check(!status.ok() && status.code() == grpc::StatusCode::RESOURCE_EXHAUSTED,
          "oversized payload should guarantee non-commit");

    openevent::PublishRequest cas;
    cas.set_principal(100);
    cas.set_token(owner_token);
    cas.set_channel_id(channel_id);
    cas.set_seq(2);
    cas.set_payload("ok");
    openevent::PublishResponse cas_response;
    status = core->Publish(cas, &cas_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::ABORTED,
          "CAS conflict should guarantee non-commit");

    auto max_seq = core->MaxSeq();
    Check(max_seq.ok() && max_seq.value() == 0,
          "guaranteed non-commit statuses must not create a message or advance max_seq");

    std::filesystem::remove_all(root);
}

void TestListTokensPagination()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_list_tokens_pagination";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);

    std::vector<std::string> expected{
        AddToken(*core, 100),
        AddToken(*core, 200),
        AddToken(*core, 300),
    };
    std::sort(expected.begin(), expected.end());

    std::vector<std::string> actual;
    std::set<std::string> cursors;
    std::string page_token;
    for (;;) {
        openevent::ListTokensRequest request;
        request.set_page_token(page_token);
        request.set_limit(1);
        openevent::ListTokensResponse response;
        openevent::Status status = core->ListTokens(request, &response);
        Check(status.ok(), status.message());
        Check(response.bindings_size() == 1, "each token page should contain one binding");
        actual.push_back(response.bindings(0).token());

        page_token = response.next_page_token();
        if (page_token.empty()) {
            break;
        }
        Check(cursors.insert(page_token).second, "token page cursor must advance");
    }
    Check(actual == expected, "token pagination should return every binding in key order");

    openevent::ListTokensRequest invalid_limit;
    openevent::ListTokensResponse response;
    openevent::Status status = core->ListTokens(invalid_limit, &response);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "zero ListTokens limit should be rejected");
    invalid_limit.set_limit(1001);
    status = core->ListTokens(invalid_limit, &response);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "oversized ListTokens limit should be rejected");

    openevent::ListTokensRequest invalid_cursor;
    invalid_cursor.set_page_token("not-a-server-cursor");
    invalid_cursor.set_limit(1);
    status = core->ListTokens(invalid_cursor, &response);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "malformed ListTokens cursor should be rejected");

    std::filesystem::remove_all(root);
}

void TestListMessagesPagination()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_list_messages";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);

    std::string token = AddToken(*core, 100);
    uint64_t public_channel = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);
    uint64_t private_channel = CreateChannel(*core, 100, token, openevent::VISIBILITY_PRIVATE);
    PublishAuto(*core, 100, token, public_channel, "public-message");
    PublishAuto(*core, 100, token, private_channel, "private-message");

    std::vector<std::string> payloads;
    uint64_t next_seq = 0;
    for (;;) {
        openevent::ListMessagesRequest request;
        request.set_from_seq(next_seq);
        request.set_limit(1);
        openevent::ListMessagesResponse response;
        openevent::Status status = core->ListMessages(request, &response);
        Check(status.ok(), status.message());
        Check(response.messages_size() == 1, "admin message page should contain one message");
        payloads.push_back(response.messages(0).payload());
        next_seq = response.next_seq();
        if (next_seq > response.last_seq()) {
            break;
        }
    }
    Check(payloads == std::vector<std::string>({"public-message", "private-message"}),
          "admin message scan should include public and private messages in seq order");

    openevent::ListMessagesRequest invalid;
    openevent::ListMessagesResponse response;
    openevent::Status status = core->ListMessages(invalid, &response);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "zero ListMessages limit should be rejected");
    invalid.set_limit(1001);
    status = core->ListMessages(invalid, &response);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "oversized ListMessages limit should be rejected");

    std::filesystem::remove_all(root);
}

void TestFetchScanBudgetAdvancesCursor()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_fetch_scan_budget";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root, 1024, 2);

    std::string owner_token = AddToken(*core, 100);
    std::string outsider_token = AddToken(*core, 200);
    uint64_t channel_id = CreateChannel(*core, 100, owner_token, openevent::VISIBILITY_PRIVATE);
    PublishAuto(*core, 100, owner_token, channel_id, "first-hidden");
    PublishAuto(*core, 100, owner_token, channel_id, "second-hidden");
    PublishAuto(*core, 100, owner_token, channel_id, "third-hidden");

    openevent::FetchRequest request;
    request.set_principal(200);
    request.set_token(outsider_token);
    request.set_from_seq(1);
    request.set_limit(10);
    openevent::FetchResponse response;
    openevent::Status status = core->Fetch(request, &response);
    Check(status.ok(), status.message());
    Check(response.messages_size() == 0, "scan-budget page may be empty when all records are hidden");
    Check(response.next_seq() == 3, "empty scan-budget page must advance past checked records");
    Check(response.last_seq() == 3, "scan-budget page should keep the snapshot tail");

    request.set_from_seq(response.next_seq());
    response.Clear();
    status = core->Fetch(request, &response);
    Check(status.ok(), status.message());
    Check(response.messages_size() == 0, "remaining hidden record should stay filtered");
    Check(response.next_seq() == 4, "second scan-budget page should reach the tail");

    std::filesystem::remove_all(root);
}

void TestResponseSoftBudgetPagination()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_response_budget";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root, 1024, 10000, 1);

    std::string token = AddToken(*core, 100);
    uint64_t channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);
    PublishAuto(*core, 100, token, channel_id, "first");
    PublishAuto(*core, 100, token, channel_id, "second");

    openevent::FetchRequest fetch;
    fetch.set_principal(100);
    fetch.set_token(token);
    fetch.set_from_seq(1);
    fetch.set_limit(10);
    openevent::FetchResponse fetch_response;
    openevent::Status status = core->Fetch(fetch, &fetch_response);
    Check(status.ok(), status.message());
    Check(fetch_response.messages_size() == 1 && fetch_response.messages(0).seq() == 1,
          "Fetch should allow one message to exceed the soft response budget");
    Check(fetch_response.next_seq() == 2, "Fetch must not consume a message deferred by byte budget");

    fetch.set_from_seq(fetch_response.next_seq());
    fetch_response.Clear();
    status = core->Fetch(fetch, &fetch_response);
    Check(status.ok(), status.message());
    Check(fetch_response.messages_size() == 1 && fetch_response.messages(0).seq() == 2,
          "Fetch should return the deferred message on the next page");
    Check(fetch_response.next_seq() == 3, "Fetch response-budget pagination should reach the tail");

    openevent::ListMessagesRequest list;
    list.set_from_seq(1);
    list.set_limit(10);
    openevent::ListMessagesResponse list_response;
    status = core->ListMessages(list, &list_response);
    Check(status.ok(), status.message());
    Check(list_response.messages_size() == 1 && list_response.messages(0).seq() == 1,
          "ListMessages should allow one message to exceed the soft response budget");
    Check(list_response.next_seq() == 2,
          "ListMessages must not consume a message deferred by byte budget");

    list.set_from_seq(list_response.next_seq());
    list_response.Clear();
    status = core->ListMessages(list, &list_response);
    Check(status.ok(), status.message());
    Check(list_response.messages_size() == 1 && list_response.messages(0).seq() == 2,
          "ListMessages should return the deferred message on the next page");
    Check(list_response.next_seq() == 3,
          "ListMessages response-budget pagination should reach the tail");

    std::filesystem::remove_all(root);
}

void TestDeleteTokenOrdersBeforePublish()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_delete_token_publish";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);

    std::string token = AddToken(*core, 100);
    uint64_t channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);
    openevent::DeleteTokenRequest delete_request;
    delete_request.set_target_token(token);
    openevent::DeleteTokenResponse delete_response;
    openevent::Status status = core->DeleteToken(delete_request, &delete_response);
    Check(status.ok(), status.message());

    openevent::PublishAutoSeqRequest publish;
    publish.set_principal(100);
    publish.set_token(token);
    publish.set_channel_id(channel_id);
    publish.set_payload("must-not-commit");
    openevent::PublishAutoSeqResponse publish_response;
    status = core->PublishAutoSeq(publish, &publish_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::UNAUTHENTICATED,
          "publish ordered after token deletion must be unauthenticated");
    auto max_seq = core->MaxSeq();
    Check(max_seq.ok() && max_seq.value() == 0, "unauthenticated publish must not advance max_seq");

    std::filesystem::remove_all(root);
}

void TestObjectWriteMetadataReadAndReopen()
{
    constexpr size_t kMaxObjectBytes = 4 * 1024 * 1024;
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_object_round_trip";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);
    const std::string token = AddToken(*core, 100);

    auto expect_write_status = [&](const openevent::WriteObjectRequest& request,
                                   grpc::StatusCode expected_code) {
        openevent::WriteObjectResponse response;
        openevent::Status status = core->WriteObject(request, &response);
        Check(!status.ok() && status.code() == expected_code,
              "unexpected WriteObject validation status: " + status.message());
    };

    openevent::WriteObjectRequest invalid = MakeWriteObjectRequest(100, token, "data");
    invalid.clear_name();
    expect_write_status(invalid, grpc::StatusCode::INVALID_ARGUMENT);
    invalid = MakeWriteObjectRequest(100, token, "data");
    invalid.set_name(std::string(256, 'n'));
    expect_write_status(invalid, grpc::StatusCode::INVALID_ARGUMENT);
    invalid = MakeWriteObjectRequest(100, token, "data");
    invalid.clear_type();
    expect_write_status(invalid, grpc::StatusCode::INVALID_ARGUMENT);
    invalid = MakeWriteObjectRequest(100, token, "data");
    invalid.set_type(std::string(256, 't'));
    expect_write_status(invalid, grpc::StatusCode::INVALID_ARGUMENT);
    invalid = MakeWriteObjectRequest(100, token, "data");
    invalid.set_description(std::string(4097, 'd'));
    expect_write_status(invalid, grpc::StatusCode::INVALID_ARGUMENT);
    invalid = MakeWriteObjectRequest(100, token, "");
    expect_write_status(invalid, grpc::StatusCode::INVALID_ARGUMENT);
    invalid = MakeWriteObjectRequest(100, token, std::string(kMaxObjectBytes + 1, 'x'));
    expect_write_status(invalid, grpc::StatusCode::INVALID_ARGUMENT);
    invalid = MakeWriteObjectRequest(100, "wrong-token", "data");
    expect_write_status(invalid, grpc::StatusCode::UNAUTHENTICATED);

    const std::string data("ab\0cdef", 7);
    openevent::WriteObjectRequest write = MakeWriteObjectRequest(100, token, data);
    write.set_name("binary");
    write.set_type("application/test");
    write.set_description("");
    openevent::WriteObjectResponse first;
    openevent::Status status = core->WriteObject(write, &first);
    Check(status.ok(), status.message());
    Check(first.object_id() == 1, "invalid writes must not consume an object ID");
    Check(first.object_token().size() == 43, "object token must be 43 Base64URL characters");
    Check(std::filesystem::is_regular_file(root / "data" / "objects" / "1"),
          "committed object file must use its decimal ID as the file name");

    openevent::GetObjectMetadataRequest metadata_request;
    metadata_request.set_object_id(first.object_id());
    metadata_request.set_object_token(first.object_token());
    openevent::GetObjectMetadataResponse metadata;
    status = core->GetObjectMetadata(metadata_request, &metadata);
    Check(status.ok(), status.message());
    Check(metadata.name() == "binary" && metadata.type() == "application/test" &&
              metadata.description().empty() && metadata.nbytes() == data.size(),
          "GetObjectMetadata must return committed metadata exactly");

    openevent::GetObjectMetadataRequest invalid_key = metadata_request;
    invalid_key.set_object_token("wrong-token");
    status = core->GetObjectMetadata(invalid_key, &metadata);
    Check(!status.ok() && status.code() == grpc::StatusCode::NOT_FOUND,
          "wrong object token must be indistinguishable from a missing object");
    invalid_key = metadata_request;
    invalid_key.set_object_id(9999);
    status = core->GetObjectMetadata(invalid_key, &metadata);
    Check(!status.ok() && status.code() == grpc::StatusCode::NOT_FOUND,
          "missing object must return NOT_FOUND");
    invalid_key = metadata_request;
    invalid_key.set_object_id(0);
    status = core->GetObjectMetadata(invalid_key, &metadata);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "object ID zero must be rejected");
    invalid_key = metadata_request;
    invalid_key.clear_object_token();
    status = core->GetObjectMetadata(invalid_key, &metadata);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "empty object token must be rejected");

    openevent::ReadObjectRequest read;
    read.set_object_id(first.object_id());
    read.set_object_token(first.object_token());
    read.set_nbytes(std::numeric_limits<uint64_t>::max());
    openevent::ReadObjectResponse read_response;
    status = core->ReadObject(read, &read_response);
    Check(status.ok() && read_response.data() == data,
          "large nbytes must return the complete object without overflow");
    read.set_offset(2);
    read.set_nbytes(3);
    status = core->ReadObject(read, &read_response);
    Check(status.ok() && read_response.data() == data.substr(2, 3),
          "partial object read must return the requested range");
    read.set_offset(data.size() - 1);
    read.set_nbytes(100);
    status = core->ReadObject(read, &read_response);
    Check(status.ok() && read_response.data() == data.substr(data.size() - 1),
          "object read must truncate a range at EOF");
    read.set_offset(data.size());
    status = core->ReadObject(read, &read_response);
    Check(status.ok() && read_response.data().empty(), "offset at EOF must return empty data");
    read.set_offset(data.size() + 1);
    status = core->ReadObject(read, &read_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::OUT_OF_RANGE,
          "offset beyond EOF must return OUT_OF_RANGE");
    read.set_offset(0);
    read.set_nbytes(0);
    status = core->ReadObject(read, &read_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "zero-length object read must be rejected");

    openevent::WriteObjectRequest largest = MakeWriteObjectRequest(
        100, token, std::string(kMaxObjectBytes, 'm'));
    largest.set_name(std::string(255, 'n'));
    largest.set_type(std::string(255, 't'));
    largest.set_description(std::string(4096, 'd'));
    openevent::WriteObjectResponse second;
    status = core->WriteObject(largest, &second);
    Check(status.ok(), status.message());
    Check(second.object_id() == 2, "committed object IDs must increase globally");

    openevent::DeleteTokenRequest delete_token;
    delete_token.set_target_token(token);
    openevent::DeleteTokenResponse delete_response;
    status = core->DeleteToken(delete_token, &delete_response);
    Check(status.ok(), status.message());
    read.set_object_id(first.object_id());
    read.set_object_token(first.object_token());
    read.set_offset(0);
    read.set_nbytes(data.size());
    status = core->ReadObject(read, &read_response);
    Check(status.ok() && read_response.data() == data,
          "ObjectKey authorization must survive deletion of the creator token");

    core.reset();
    core = MakeCore(root);
    read.set_object_id(first.object_id());
    read.set_object_token(first.object_token());
    read.set_offset(0);
    read.set_nbytes(data.size());
    status = core->ReadObject(read, &read_response);
    Check(status.ok() && read_response.data() == data, "committed object must remain readable after reopen");

    std::filesystem::remove_all(root);
}

void TestMessageObjectReferences()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_object_references";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);
    const std::string token = AddToken(*core, 100);
    const uint64_t channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);
    const auto first = WriteObject(*core, 100, token, "first");
    const auto second = WriteObject(*core, 100, token, "second");

    openevent::PublishAutoSeqRequest publish;
    publish.set_principal(100);
    publish.set_token(token);
    publish.set_channel_id(channel_id);
    publish.set_payload("references");
    for (const auto* key : {&first, &second, &first}) {
        auto* object_key = publish.add_object_keys();
        object_key->set_object_id(key->object_id());
        object_key->set_object_token(key->object_token());
    }
    openevent::PublishAutoSeqResponse publish_response;
    openevent::Status status = core->PublishAutoSeq(publish, &publish_response);
    Check(status.ok(), status.message());

    openevent::FetchRequest fetch;
    fetch.set_principal(100);
    fetch.set_token(token);
    fetch.set_from_seq(publish_response.seq());
    fetch.set_limit(1);
    openevent::FetchResponse fetched;
    status = core->Fetch(fetch, &fetched);
    Check(status.ok() && fetched.messages_size() == 1, "referenced message must be fetchable");
    const auto& keys = fetched.messages(0).object_keys();
    Check(keys.size() == 3 && keys.Get(0).object_id() == first.object_id() &&
              keys.Get(1).object_id() == second.object_id() &&
              keys.Get(2).object_id() == first.object_id(),
          "message ObjectKeys must retain duplicates and request order");

    openevent::FetchResponse subscription_batch;
    google::protobuf::RepeatedField<uint64_t> subscription_channels;
    status = core->FetchSubscriptionBatch(100,
                                          token,
                                          publish_response.seq(),
                                          1,
                                          false,
                                          subscription_channels,
                                          &subscription_batch);
    Check(status.ok() && subscription_batch.messages_size() == 1 &&
              subscription_batch.messages(0).object_keys_size() == 3,
          "subscription batches must retain object references");
    openevent::ListMessagesRequest list;
    list.set_from_seq(publish_response.seq());
    list.set_limit(1);
    openevent::ListMessagesResponse listed;
    status = core->ListMessages(list, &listed);
    Check(status.ok() && listed.messages_size() == 1 && listed.messages(0).object_keys_size() == 3,
          "administrative message reads must retain object references");

    publish.clear_object_keys();
    for (size_t i = 0; i < 1024; ++i) {
        auto* object_key = publish.add_object_keys();
        object_key->set_object_id(first.object_id());
        object_key->set_object_token(first.object_token());
    }
    status = core->PublishAutoSeq(publish, &publish_response);
    Check(status.ok(), "a message with exactly 1024 ObjectKeys must commit");
    const uint64_t watermark = publish_response.seq();

    auto* too_many = publish.add_object_keys();
    too_many->set_object_id(first.object_id());
    too_many->set_object_token(first.object_token());
    status = core->PublishAutoSeq(publish, &publish_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "a message with 1025 ObjectKeys must be rejected");
    auto max_seq = core->MaxSeq();
    Check(max_seq.ok() && max_seq.value() == watermark,
          "too many ObjectKeys must not advance the message watermark");

    publish.clear_object_keys();
    auto* valid_key = publish.add_object_keys();
    valid_key->set_object_id(first.object_id());
    valid_key->set_object_token(first.object_token());
    auto* invalid_key = publish.add_object_keys();
    invalid_key->set_object_id(second.object_id());
    invalid_key->set_object_token("wrong-token");
    status = core->PublishAutoSeq(publish, &publish_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::NOT_FOUND,
          "one invalid ObjectKey must reject the complete message");
    max_seq = core->MaxSeq();
    Check(max_seq.ok() && max_seq.value() == watermark,
          "an invalid ObjectKey must not partially commit or advance max_seq");

    publish.clear_object_keys();
    valid_key = publish.add_object_keys();
    valid_key->set_object_id(first.object_id());
    valid_key->set_object_token(first.object_token());
    invalid_key = publish.add_object_keys();
    invalid_key->set_object_id(first.object_id());
    invalid_key->set_object_token("wrong-token");
    status = core->PublishAutoSeq(publish, &publish_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::NOT_FOUND,
          "duplicate object IDs must validate every supplied token");
    max_seq = core->MaxSeq();
    Check(max_seq.ok() && max_seq.value() == watermark,
          "a bad token on a duplicate object ID must not advance max_seq");

    publish.clear_object_keys();
    invalid_key = publish.add_object_keys();
    invalid_key->set_object_id(0);
    invalid_key->set_object_token(first.object_token());
    status = core->PublishAutoSeq(publish, &publish_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "a malformed ObjectKey must be rejected before commit");

    std::filesystem::remove_all(root);
}

void TestObjectWriteInjectedFailures()
{
    struct FailureCase {
        const char* name;
        openevent::StorageFaultPoint point;
        int skip_matches;
        grpc::StatusCode code;
        bool committed;
        uint64_t next_id;
        bool fatal;
    };
    const std::vector<FailureCase> cases{
        {"preparing-commit", openevent::StorageFaultPoint::kBeforeCommit, 0,
         grpc::StatusCode::UNAVAILABLE, false, 1, false},
        {"preparing-after-commit", openevent::StorageFaultPoint::kAfterCommit, 0,
         grpc::StatusCode::UNAVAILABLE, false, 2, false},
        {"file-write", openevent::StorageFaultPoint::kBeforeTemporaryFileWrite, 0,
         grpc::StatusCode::RESOURCE_EXHAUSTED, false, 2, false},
        {"file-fsync", openevent::StorageFaultPoint::kBeforeFileFsync, 0,
         grpc::StatusCode::RESOURCE_EXHAUSTED, false, 2, false},
        {"rename", openevent::StorageFaultPoint::kBeforeRename, 0,
         grpc::StatusCode::UNAVAILABLE, false, 2, false},
        {"directory-fsync", openevent::StorageFaultPoint::kBeforeDirectoryFsync, 0,
         grpc::StatusCode::RESOURCE_EXHAUSTED, false, 2, false},
        {"committed-before-write", openevent::StorageFaultPoint::kBeforeCommit, 1,
         grpc::StatusCode::UNAVAILABLE, false, 2, true},
        {"committed-after-write", openevent::StorageFaultPoint::kAfterCommit, 1,
         grpc::StatusCode::UNAVAILABLE, true, 2, true},
    };

    for (const auto& test_case : cases) {
        const auto root = std::filesystem::temp_directory_path() /
                          (std::string("openevent_object_failure_") + test_case.name);
        std::filesystem::remove_all(root);
        struct State {
            bool active = false;
            int skip_matches = 0;
        };
        auto state = std::make_shared<State>();
        auto injector = [state, test_case](openevent::StorageFaultPoint point) {
            if (!state->active || point != test_case.point) {
                return openevent::Status::Ok();
            }
            if (state->skip_matches > 0) {
                --state->skip_matches;
                return openevent::Status::Ok();
            }
            state->active = false;
            return openevent::Status(test_case.code, "injected storage failure");
        };
        int fatal_calls = 0;
        auto core = MakeCore(root,
                             1024,
                             10000,
                             0,
                             [&](const openevent::Status&) { ++fatal_calls; },
                             injector);
        const std::string token = AddToken(*core, 100);
        state->skip_matches = test_case.skip_matches;
        state->active = true;

        openevent::WriteObjectRequest request = MakeWriteObjectRequest(100, token, "fault-data");
        openevent::WriteObjectResponse response;
        openevent::Status status = core->WriteObject(request, &response);
        Check(!status.ok() && status.code() == test_case.code,
              std::string("unexpected injected failure result for ") + test_case.name);
        Check(fatal_calls == (test_case.fatal ? 1 : 0),
              std::string("unexpected fatal shutdown result for ") + test_case.name);
        core.reset();
        CheckRecoveredObjectState(root, test_case.committed, test_case.next_id);
        std::filesystem::remove_all(root);
    }
}

void TestRpcStorageFailureTriggersFatalShutdown()
{
    const auto run_case = [](const std::string& name, const openevent::Status& injected_status) {
        const auto root = std::filesystem::temp_directory_path() / ("openevent_rpc_" + name);
        std::filesystem::remove_all(root);
        auto inject_failure = std::make_shared<bool>(false);
        auto injector = [inject_failure, injected_status](openevent::StorageFaultPoint point) {
            if (*inject_failure && point == openevent::StorageFaultPoint::kBeforeCommit) {
                return injected_status;
            }
            return openevent::Status::Ok();
        };
        int fatal_calls = 0;
        auto unique_core = MakeCore(root,
                                    1024,
                                    10000,
                                    0,
                                    [&](const openevent::Status& status) {
                                        Check(status.code() == injected_status.code(),
                                              "fatal RPC storage error must preserve its gRPC code");
                                        ++fatal_calls;
                                    },
                                    injector);
        const std::string token = AddToken(*unique_core, 100);
        auto core = std::shared_ptr<openevent::OpenEventCore>(std::move(unique_core));
        openevent::ChannelServiceImpl service(core);

        openevent::CreateChannelRequest request;
        request.set_principal(100);
        request.set_token(token);
        request.set_name(name);
        request.set_visibility(openevent::VISIBILITY_PUBLIC);
        openevent::CreateChannelResponse response;
        *inject_failure = true;
        grpc::Status status = service.CreateChannel(nullptr, &request, &response);
        Check(!status.ok() && status.error_code() == injected_status.code() && fatal_calls == 1,
              "fatal storage failure from a non-object RPC must trigger shutdown");

        *inject_failure = false;
        response.Clear();
        status = service.CreateChannel(nullptr, &request, &response);
        Check(!status.ok() && status.error_code() == grpc::StatusCode::UNAVAILABLE &&
                  fatal_calls == 1,
              "RPCs after a fatal storage failure must be rejected without another storage access");

        core.reset();
        std::filesystem::remove_all(root);
    };

    run_case("data_loss",
             openevent::Status(grpc::StatusCode::DATA_LOSS, "injected stored-data corruption"));
    run_case("rocksdb_unavailable",
             openevent::Status::Fatal(grpc::StatusCode::UNAVAILABLE,
                                      "injected RocksDB availability failure"));
}

void TestObjectWriteCrashRecoveryBoundaries()
{
    struct CrashCase {
        const char* name;
        openevent::StorageFaultPoint point;
        int skip_matches;
        bool committed;
    };
    const std::vector<CrashCase> cases{
        {"after-preparing", openevent::StorageFaultPoint::kAfterCommit, 0, false},
        {"after-temp-create", openevent::StorageFaultPoint::kAfterTemporaryFileCreate, 0, false},
        {"after-temp-write", openevent::StorageFaultPoint::kAfterTemporaryFileWrite, 0, false},
        {"after-file-fsync", openevent::StorageFaultPoint::kAfterFileFsync, 0, false},
        {"after-rename", openevent::StorageFaultPoint::kAfterRename, 0, false},
        {"after-directory-fsync", openevent::StorageFaultPoint::kAfterDirectoryFsync, 0, false},
        {"before-committed", openevent::StorageFaultPoint::kBeforeCommit, 1, false},
        {"after-committed", openevent::StorageFaultPoint::kAfterCommit, 1, true},
    };
    constexpr int kInjectedCrashExitCode = 73;

    for (const auto& test_case : cases) {
        const auto root = std::filesystem::temp_directory_path() /
                          (std::string("openevent_object_crash_") + test_case.name);
        std::filesystem::remove_all(root);
        std::string token;
        {
            auto core = MakeCore(root);
            token = AddToken(*core, 100);
        }

        const pid_t child = ::fork();
        Check(child >= 0, "fork object crash test process");
        if (child == 0) {
            int skip_matches = test_case.skip_matches;
            auto injector = [test_case, &skip_matches](openevent::StorageFaultPoint point) {
                if (point != test_case.point) {
                    return openevent::Status::Ok();
                }
                if (skip_matches > 0) {
                    --skip_matches;
                    return openevent::Status::Ok();
                }
                ::_exit(kInjectedCrashExitCode);
            };
            auto core = MakeCore(root, 1024, 10000, 0, {}, injector);
            openevent::WriteObjectRequest request = MakeWriteObjectRequest(100, token, "fault-data");
            openevent::WriteObjectResponse response;
            core->WriteObject(request, &response);
            ::_exit(74);
        }

        int child_status = 0;
        Check(::waitpid(child, &child_status, 0) == child, "wait for injected object crash");
        Check(WIFEXITED(child_status) && WEXITSTATUS(child_status) == kInjectedCrashExitCode,
              std::string("child did not exit at injected point: ") + test_case.name);
        CheckRecoveredObjectState(root, test_case.committed, 2);
        std::filesystem::remove_all(root);
    }
}

void TestConcurrentObjectWritesCanCommitOutOfIdOrder()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_object_concurrent_commit";
    std::filesystem::remove_all(root);
    struct State {
        std::mutex mutex;
        std::condition_variable cv;
        bool active = false;
        bool first_blocked = false;
        bool release_first = false;
        int file_creates = 0;
    };
    auto state = std::make_shared<State>();
    auto injector = [state](openevent::StorageFaultPoint point) {
        if (point != openevent::StorageFaultPoint::kAfterTemporaryFileCreate) {
            return openevent::Status::Ok();
        }
        std::unique_lock<std::mutex> lock(state->mutex);
        if (!state->active || state->file_creates++ != 0) {
            return openevent::Status::Ok();
        }
        state->first_blocked = true;
        state->cv.notify_all();
        state->cv.wait(lock, [&]() { return state->release_first; });
        return openevent::Status::Ok();
    };
    auto core = MakeCore(root, 1024, 10000, 0, {}, injector);
    const std::string token = AddToken(*core, 100);
    state->active = true;

    openevent::WriteObjectResponse first_response;
    openevent::WriteObjectResponse second_response;
    openevent::Status first_status;
    openevent::Status second_status;
    std::thread first([&]() {
        auto request = MakeWriteObjectRequest(100, token, "first");
        first_status = core->WriteObject(request, &first_response);
    });
    {
        std::unique_lock<std::mutex> lock(state->mutex);
        state->cv.wait(lock, [&]() { return state->first_blocked; });
    }
    std::thread second([&]() {
        auto request = MakeWriteObjectRequest(100, token, "second");
        second_status = core->WriteObject(request, &second_response);
    });
    second.join();
    Check(second_status.ok() && second_response.object_id() == 2,
          "second object must commit while the lower ID is still in file I/O");
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->release_first = true;
    }
    state->cv.notify_all();
    first.join();
    Check(first_status.ok() && first_response.object_id() == 1,
          "concurrent object IDs must follow PREPARING allocation order");
    core.reset();
    std::filesystem::remove_all(root);
}

void TestObjectWriteReauthenticatesBeforeCommit()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_object_final_auth";
    std::filesystem::remove_all(root);
    struct State {
        std::mutex mutex;
        std::condition_variable cv;
        bool active = false;
        bool blocked = false;
        bool release = false;
    };
    auto state = std::make_shared<State>();
    auto injector = [state](openevent::StorageFaultPoint point) {
        if (point != openevent::StorageFaultPoint::kAfterTemporaryFileCreate || !state->active) {
            return openevent::Status::Ok();
        }
        std::unique_lock<std::mutex> lock(state->mutex);
        state->active = false;
        state->blocked = true;
        state->cv.notify_all();
        state->cv.wait(lock, [&]() { return state->release; });
        return openevent::Status::Ok();
    };
    auto core = MakeCore(root, 1024, 10000, 0, {}, injector);
    const std::string token = AddToken(*core, 100);
    state->active = true;

    openevent::Status write_status;
    std::thread writer([&]() {
        auto request = MakeWriteObjectRequest(100, token, "fault-data");
        openevent::WriteObjectResponse response;
        write_status = core->WriteObject(request, &response);
    });
    {
        std::unique_lock<std::mutex> lock(state->mutex);
        state->cv.wait(lock, [&]() { return state->blocked; });
    }
    openevent::DeleteTokenRequest delete_request;
    delete_request.set_target_token(token);
    openevent::DeleteTokenResponse delete_response;
    Check(core->DeleteToken(delete_request, &delete_response).ok(), "delete token during object file I/O");
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->release = true;
    }
    state->cv.notify_all();
    writer.join();
    Check(!write_status.ok() && write_status.code() == grpc::StatusCode::UNAUTHENTICATED,
          "object write must fail final authentication after token deletion");
    core.reset();
    CheckRecoveredObjectState(root, false, 2);
    std::filesystem::remove_all(root);
}

void TestCancelledObjectWriteContinuesAfterPreparing()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_object_cancel_after_preparing";
    std::filesystem::remove_all(root);
    struct State {
        std::mutex mutex;
        std::condition_variable cv;
        bool active = false;
        int commits = 0;
        bool preparing_committed = false;
        bool release = false;
        bool object_committed = false;
    };
    auto state = std::make_shared<State>();
    auto injector = [state](openevent::StorageFaultPoint point) {
        if (point != openevent::StorageFaultPoint::kAfterCommit || !state->active) {
            return openevent::Status::Ok();
        }
        std::unique_lock<std::mutex> lock(state->mutex);
        ++state->commits;
        if (state->commits == 1) {
            state->preparing_committed = true;
            state->cv.notify_all();
            state->cv.wait(lock, [&]() { return state->release; });
        } else if (state->commits == 2) {
            state->object_committed = true;
            state->cv.notify_all();
        }
        return openevent::Status::Ok();
    };
    auto unique_core = MakeCore(root, 1024, 10000, 0, {}, injector);
    const std::string token = AddToken(*unique_core, 100);
    auto core = std::shared_ptr<openevent::OpenEventCore>(std::move(unique_core));
    state->active = true;

    {
        openevent::ObjectStorageServiceImpl service(core);
        grpc::ServerBuilder builder;
        int port = 0;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(&service);
        std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
        Check(server != nullptr && port > 0, "start cancellation test gRPC server");
        auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                                           grpc::InsecureChannelCredentials());
        auto stub = openevent::ObjectStorageService::NewStub(channel);
        grpc::ClientContext context;
        grpc::Status rpc_status;
        std::thread client([&]() {
            auto request = MakeWriteObjectRequest(100, token, "fault-data");
            openevent::WriteObjectResponse response;
            rpc_status = stub->WriteObject(&context, request, &response);
        });
        {
            std::unique_lock<std::mutex> lock(state->mutex);
            state->cv.wait(lock, [&]() { return state->preparing_committed; });
        }
        context.TryCancel();
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->release = true;
        }
        state->cv.notify_all();
        {
            std::unique_lock<std::mutex> lock(state->mutex);
            state->cv.wait(lock, [&]() { return state->object_committed; });
        }
        client.join();
        Check(rpc_status.error_code() == grpc::StatusCode::CANCELLED,
              "cancelled object RPC must report CANCELLED to the client");
        server->Shutdown();
        server->Wait();
    }
    core.reset();
    CheckRecoveredObjectState(root, true, 2);
    std::filesystem::remove_all(root);
}

void TestPreparingRecoveryAndNoDirectoryScan()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_object_recovery";
    const auto storage_path = root / "data";
    const auto objects_path = storage_path / "objects";
    std::filesystem::remove_all(root);
    {
        auto storage = openevent::UnifiedStorage::Open(storage_path.string());
        Check(storage.ok(), storage.status().message());
        openevent::StoredObject object;
        object.object_id = 1;
        object.object_token = std::string(43, 'A');
        object.creator_principal = 100;
        object.name = "preparing";
        object.type = "application/octet-stream";
        object.nbytes = 4;

        rocksdb::WriteBatch batch;
        openevent::Status status = storage.value()->PutPreparingObject(&batch, object);
        Check(status.ok(), status.message());
        status = storage.value()->SetNextObjectId(&batch, 2);
        Check(status.ok(), status.message());
        status = storage.value()->Commit(&batch);
        Check(status.ok(), status.message());
        status = storage.value()->WriteObjectFile(1, "data");
        Check(status.ok(), status.message());
        std::ofstream temporary(objects_path / ".tmp.1", std::ios::binary);
        temporary << "partial";
        Check(temporary.good(), "create recovery temporary file");
    }
    {
        std::ofstream orphan(objects_path / "999", std::ios::binary);
        orphan << "orphan";
        Check(orphan.good(), "create orphan object file");
    }

    auto reopened = openevent::UnifiedStorage::Open(storage_path.string());
    Check(reopened.ok(), reopened.status().message());
    Check(!std::filesystem::exists(objects_path / "1") &&
              !std::filesystem::exists(objects_path / ".tmp.1"),
          "startup recovery must remove both known PREPARING paths");
    Check(std::filesystem::exists(objects_path / "999"),
          "startup recovery must not scan or remove unrelated object files");
    auto snapshot = reopened.value()->CreateSnapshot();
    Check(snapshot.ok(), snapshot.status().message());
    auto preparing = reopened.value()->GetPreparingObject(snapshot.value(), 1);
    Check(preparing.ok() && !preparing.value().has_value(),
          "startup recovery must delete PREPARING metadata");
    auto next_object_id = reopened.value()->GetNextObjectId(snapshot.value());
    Check(next_object_id.ok() && next_object_id.value() == 2,
          "startup recovery must not reuse a PREPARING object ID");

    snapshot.value() = openevent::ReadSnapshot();
    reopened.value().reset();
    std::filesystem::remove_all(root);
}

void TestCommittedObjectCorruptionIsFatal()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_object_corruption";
    std::filesystem::remove_all(root);
    int fatal_calls = 0;
    auto core = MakeCore(root,
                         1024,
                         10000,
                         0,
                         [&](const openevent::Status& status) {
                             Check(status.code() == grpc::StatusCode::DATA_LOSS,
                                   "fatal object corruption must report DATA_LOSS");
                             ++fatal_calls;
                         });
    const std::string token = AddToken(*core, 100);
    const uint64_t channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);
    const auto object = WriteObject(*core, 100, token, "original");
    {
        std::ofstream corrupt(root / "data" / "objects" / std::to_string(object.object_id()),
                              std::ios::binary | std::ios::trunc);
        corrupt << "modified";
        Check(corrupt.good(), "replace committed object content");
    }

    openevent::GetObjectMetadataRequest metadata_request;
    metadata_request.set_object_id(object.object_id());
    metadata_request.set_object_token(object.object_token());
    openevent::GetObjectMetadataResponse metadata;
    openevent::Status status = core->GetObjectMetadata(metadata_request, &metadata);
    Check(status.ok() && fatal_calls == 0,
          "metadata lookup must not inspect a committed object file");

    openevent::PublishAutoSeqRequest publish;
    publish.set_principal(100);
    publish.set_token(token);
    publish.set_channel_id(channel_id);
    auto* object_key = publish.add_object_keys();
    object_key->set_object_id(object.object_id());
    object_key->set_object_token(object.object_token());
    openevent::PublishAutoSeqResponse publish_response;
    status = core->PublishAutoSeq(publish, &publish_response);
    Check(status.ok() && fatal_calls == 0,
          "publishing an ObjectKey must validate metadata without reading object data");

    openevent::ReadObjectRequest read;
    read.set_object_id(object.object_id());
    read.set_object_token(object.object_token());
    read.set_nbytes(8);
    openevent::ReadObjectResponse response;
    status = core->ReadObject(read, &response);
    Check(status.ok() && response.data() == "modified" && fatal_calls == 0,
          "same-size content replacement is outside the retained file integrity checks");
    {
        std::ofstream corrupt(root / "data" / "objects" / std::to_string(object.object_id()),
                              std::ios::binary | std::ios::trunc);
        corrupt << "short";
        Check(corrupt.good(), "truncate committed object content");
    }
    read.set_nbytes(1);
    status = core->ReadObject(read, &response);
    Check(!status.ok() && status.code() == grpc::StatusCode::DATA_LOSS && fatal_calls == 1,
          "committed object size corruption must return DATA_LOSS and trigger fatal shutdown once");
    status = core->ReadObject(read, &response);
    Check(!status.ok() && status.code() == grpc::StatusCode::UNAVAILABLE && fatal_calls == 1,
          "object reads after a fatal storage error must be rejected without another callback");

    std::filesystem::remove_all(root);
}

void TestObjectIoErrorClassificationAndPaths()
{
    Check(openevent::ObjectIoStatus("write object", ENOSPC).code() ==
              grpc::StatusCode::RESOURCE_EXHAUSTED,
          "ENOSPC should report RESOURCE_EXHAUSTED");
    Check(openevent::ObjectIoStatus("write object", EDQUOT).code() ==
              grpc::StatusCode::RESOURCE_EXHAUSTED,
          "EDQUOT should report RESOURCE_EXHAUSTED");
    Check(openevent::ObjectIoStatus("write object", EIO).code() ==
              grpc::StatusCode::UNAVAILABLE,
          "other object I/O errors should report UNAVAILABLE");

    const auto root =
        std::filesystem::temp_directory_path() / "openevent_object_io_error_paths";
    const auto objects_path = root / "objects";
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(objects_path);

    {
        auto store = openevent::ObjectFileStore::Open(objects_path.string());
        Check(store.ok(), store.status().message());
        {
            std::ofstream temporary(objects_path / ".tmp.1", std::ios::binary);
            temporary << "collision";
            Check(temporary.good(), "create colliding temporary object file");
        }
        openevent::Status status = store.value()->Write(1, "data");
        Check(!status.ok() && status.code() == grpc::StatusCode::DATA_LOSS,
              "existing temporary object file should report DATA_LOSS");
    }

    std::filesystem::remove(objects_path / ".tmp.1");
    {
        std::ofstream final_file(objects_path / "2", std::ios::binary);
        final_file << "collision";
        Check(final_file.good(), "create colliding final object file");
    }
    {
        auto store = openevent::ObjectFileStore::Open(objects_path.string());
        Check(store.ok(), store.status().message());
        openevent::Status status = store.value()->Write(2, "data");
        Check(!status.ok() && status.code() == grpc::StatusCode::DATA_LOSS,
              "existing final object file should report DATA_LOSS");
    }

    auto missing = openevent::ObjectFileStore::Open((root / "missing").string());
    Check(!missing.ok() && missing.status().code() == grpc::StatusCode::UNAVAILABLE,
          "missing object directory should report UNAVAILABLE");

    std::filesystem::remove_all(root);
}

void TestUnifiedStorageReopen()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_unified_reopen";
    std::filesystem::remove_all(root);
    uint64_t published_seq = 0;
    {
        auto core = MakeCore(root);
        std::string token = AddToken(*core, 100);
        uint64_t channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);
        openevent::PublishAutoSeqRequest publish;
        publish.set_principal(100);
        publish.set_token(token);
        publish.set_channel_id(channel_id);
        publish.set_payload("persisted");
        openevent::PublishAutoSeqResponse response;
        openevent::Status status = core->PublishAutoSeq(publish, &response);
        Check(status.ok(), status.message());
        published_seq = response.seq();
    }

    auto reopened = MakeCore(root);
    auto max_seq = reopened->MaxSeq();
    Check(max_seq.ok() && max_seq.value() == published_seq,
          "reopened unified storage should retain the committed watermark");

    openevent::ListMessagesRequest request;
    request.set_from_seq(published_seq);
    request.set_limit(1);
    openevent::ListMessagesResponse response;
    openevent::Status status = reopened->ListMessages(request, &response);
    Check(status.ok(), status.message());
    Check(response.messages_size() == 1 && response.messages(0).payload() == "persisted",
          "reopened unified storage should retain the committed message");

    std::filesystem::remove_all(root);
}

void TestUnifiedStorageLayout()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_unified_layout";
    const auto storage_path = root / "data";
    const auto database_path = storage_path / "db";
    std::filesystem::remove_all(root);
    {
        auto core = MakeCore(root);
        std::string token = AddToken(*core, 100);
        uint64_t channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);
        PublishAuto(*core, 100, token, channel_id, "layout");
    }

    rocksdb::Options list_options;
    std::vector<std::string> names;
    Check(std::filesystem::is_directory(storage_path / "objects"),
          "unified storage should create the objects directory");
    rocksdb::Status rocks_status = rocksdb::DB::ListColumnFamilies(list_options, database_path.string(), &names);
    Check(rocks_status.ok(), rocks_status.ToString());
    Check(std::set<std::string>(names.begin(), names.end()) ==
              std::set<std::string>({rocksdb::kDefaultColumnFamilyName, "messages", "objects"}),
          "unified storage should contain default, messages, and objects column families");

    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors{
        {rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()},
        {"messages", rocksdb::ColumnFamilyOptions()},
        {"objects", rocksdb::ColumnFamilyOptions()},
    };
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::DB* raw_db = nullptr;
    rocks_status = rocksdb::DB::Open(
        rocksdb::DBOptions(), database_path.string(), descriptors, &handles, &raw_db);
    Check(rocks_status.ok(), rocks_status.ToString());
    std::unique_ptr<rocksdb::DB> db(raw_db);

    std::unique_ptr<rocksdb::Iterator> meta_it(db->NewIterator(rocksdb::ReadOptions(), handles[0]));
    for (meta_it->SeekToFirst(); meta_it->Valid(); meta_it->Next()) {
        const std::string key = meta_it->key().ToString();
        Check(key.find("pend_") == std::string::npos && key.find("seq:") == std::string::npos &&
                  key.find("offset:") == std::string::npos && key.find("ch_last:") == std::string::npos &&
                  key != "meta:min_seq",
              "unified metadata must not contain legacy pending, offset, or derived keys");
    }
    Check(meta_it->status().ok(), meta_it->status().ToString());

    std::unique_ptr<rocksdb::Iterator> message_it(db->NewIterator(rocksdb::ReadOptions(), handles[1]));
    message_it->SeekToFirst();
    Check(message_it->Valid(), "messages column family should contain the published message");
    Check(message_it->key().size() == std::strlen("msg/") + sizeof(uint64_t),
          "message key should use msg/ plus fixed-width BE64 seq");
    Check(message_it->key().starts_with("msg/"), "message key should use msg/ prefix");
    Check(message_it->status().ok(), message_it->status().ToString());

    message_it.reset();
    meta_it.reset();
    for (auto* handle : handles) {
        rocks_status = db->DestroyColumnFamilyHandle(handle);
        Check(rocks_status.ok(), rocks_status.ToString());
    }
    db.reset();
    std::filesystem::remove_all(root);
}

void TestNestedStoragePathInitialization()
{
    const auto root =
        std::filesystem::temp_directory_path() / "openevent_nested_storage_initialization";
    const auto storage_path = root / "missing" / "parent" / "data";
    std::filesystem::remove_all(root);

    {
        auto storage = openevent::UnifiedStorage::Open(storage_path.string());
        Check(storage.ok(), storage.status().message());
        Check(std::filesystem::is_directory(storage_path / "db"),
              "nested initialization should create the database directory");
        Check(std::filesystem::is_directory(storage_path / "objects"),
              "nested initialization should create the object directory");
    }
    {
        auto reopened = openevent::UnifiedStorage::Open(storage_path.string());
        Check(reopened.ok(), reopened.status().message());
    }

    std::filesystem::remove_all(root);
}

void TestUint64KeyEncodingOrder()
{
    const std::vector<uint64_t> values{9, 10, 255, 256};
    std::vector<std::string> encoded;
    for (uint64_t value : values) {
        encoded.push_back(openevent::EncodeUint64(value));
    }
    std::vector<std::string> sorted = encoded;
    std::sort(sorted.begin(), sorted.end());
    Check(sorted == encoded, "BE64 keys must preserve numeric order across byte boundaries");
}

void TestUnifiedStorageRejectsIncompleteInitialization()
{
    for (int created_column_families = 0; created_column_families <= 2; ++created_column_families) {
        const auto root = std::filesystem::temp_directory_path() /
                          ("openevent_init_stage_" + std::to_string(created_column_families));
        std::filesystem::remove_all(root);
        CreatePartialStorage(root,
                             true,
                             created_column_families >= 1,
                             created_column_families >= 2);

        auto storage = openevent::UnifiedStorage::Open(root.string());
        Check(!storage.ok(), "incomplete initialization must be rejected without online repair");
        std::filesystem::remove_all(root);
    }
}

void TestUnifiedStorageRejectsExtraRootEntries()
{
    const auto root =
        std::filesystem::temp_directory_path() / "openevent_extra_storage_root_entries";
    std::filesystem::remove_all(root);
    {
        auto storage = openevent::UnifiedStorage::Open(root.string());
        Check(storage.ok(), storage.status().message());
    }

    const auto extra_path = root / "unexpected";
    {
        std::ofstream extra_file(extra_path);
        Check(extra_file.good(), "create unexpected storage root file");
    }
    auto storage = openevent::UnifiedStorage::Open(root.string());
    Check(!storage.ok(), "storage root with an extra file must be rejected");

    std::filesystem::remove(extra_path);
    std::error_code ec;
    std::filesystem::create_directory(extra_path, ec);
    Check(!ec, "create unexpected storage root directory: " + ec.message());
    storage = openevent::UnifiedStorage::Open(root.string());
    Check(!storage.ok(), "storage root with an extra directory must be rejected");

    std::filesystem::remove_all(root);
}

void TestUnifiedStorageRejectsInvalidState()
{
    const auto base = std::filesystem::temp_directory_path() / "openevent_invalid_storage";
    std::filesystem::remove_all(base);

    const auto missing_marker = base / "missing-marker";
    CreatePartialStorage(missing_marker, false, false);
    auto storage = openevent::UnifiedStorage::Open(missing_marker.string());
    Check(!storage.ok(), "partial storage without initialization marker must be rejected");

    const auto bad_schema = base / "bad-schema";
    {
        auto initialized = openevent::UnifiedStorage::Open(bad_schema.string());
        Check(initialized.ok(), initialized.status().message());
    }
    PutRawRecord(bad_schema / "db", rocksdb::kDefaultColumnFamilyName, "meta:schema_version",
                 openevent::EncodeUint64(999));
    storage = openevent::UnifiedStorage::Open(bad_schema.string());
    Check(!storage.ok(), "unsupported storage schema must be rejected");

    const auto zero_next_channel_id = base / "zero-next-channel-id";
    {
        auto initialized = openevent::UnifiedStorage::Open(zero_next_channel_id.string());
        Check(initialized.ok(), initialized.status().message());
    }
    PutRawRecord(zero_next_channel_id / "db",
                 rocksdb::kDefaultColumnFamilyName,
                 "meta:next_channel_id",
                 openevent::EncodeUint64(0));
    storage = openevent::UnifiedStorage::Open(zero_next_channel_id.string());
    Check(!storage.ok(), "zero next Channel ID must be rejected");

    const auto conflicting_object_root = base / "conflicting-object-root";
    openevent::WriteObjectResponse written;
    {
        auto core = MakeCore(conflicting_object_root);
        const std::string token = AddToken(*core, 100);
        written = WriteObject(*core, 100, token, "conflict");
    }
    openevent::storage::internal::ObjectRecord preparing;
    preparing.set_object_id(written.object_id());
    preparing.set_object_token(written.object_token());
    preparing.set_creator_principal(100);
    preparing.set_name("object.bin");
    preparing.set_type("application/octet-stream");
    preparing.set_description("test object");
    preparing.set_nbytes(8);
    PutRawRecord(conflicting_object_root / "data" / "db",
                 "objects",
                 std::string("preparing/") + openevent::EncodeUint64(written.object_id()),
                 preparing.SerializeAsString());
    storage = openevent::UnifiedStorage::Open((conflicting_object_root / "data").string());
    Check(!storage.ok() && storage.status().code() == grpc::StatusCode::DATA_LOSS,
          "conflicting PREPARING and COMMITTED object metadata must reject startup");
    Check(std::filesystem::is_regular_file(
              conflicting_object_root / "data" / "objects" / std::to_string(written.object_id())),
          "conflicting metadata recovery must not delete a committed object file");

    std::filesystem::remove_all(base);
}

void TestUnifiedStorageStartupDoesNotScanMessages()
{
    const auto base = std::filesystem::temp_directory_path() / "openevent_no_startup_message_scan";
    std::filesystem::remove_all(base);

    const auto bad_watermark = base / "bad-watermark";
    {
        auto initialized = openevent::UnifiedStorage::Open(bad_watermark.string());
        Check(initialized.ok(), initialized.status().message());
    }
    PutRawRecord(bad_watermark / "db", rocksdb::kDefaultColumnFamilyName, "meta:max_seq",
                 openevent::EncodeUint64(1));
    {
        auto storage = openevent::UnifiedStorage::Open(bad_watermark.string());
        Check(storage.ok(), "startup must not compare max_seq with message records");
        auto snapshot = storage.value()->CreateSnapshot();
        Check(snapshot.ok(), snapshot.status().message());
        auto scan = storage.value()->ScanMessages(
            snapshot.value(),
            1,
            1,
            1,
            [](const openevent::EventMessage&) -> openevent::Result<openevent::MessageScanAction> {
                return openevent::MessageScanAction::kContinue;
            });
        Check(!scan.ok() && scan.status().code() == grpc::StatusCode::DATA_LOSS,
              "runtime message scan must reject a missing record below max_seq");
    }

    const auto bad_message_root = base / "bad-message-root";
    std::string token;
    uint64_t channel_id = 0;
    {
        auto core = MakeCore(bad_message_root);
        token = AddToken(*core, 100);
        channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);
        PublishAuto(*core, 100, token, channel_id, "valid-before-corruption");
    }
    openevent::EventMessage mismatched_message;
    mismatched_message.set_seq(2);
    mismatched_message.set_payload("mismatched-seq");
    PutRawRecord(bad_message_root / "data" / "db",
                 "messages",
                 std::string("msg/") + openevent::EncodeUint64(1),
                 mismatched_message.SerializeAsString());
    {
        auto storage = openevent::UnifiedStorage::Open((bad_message_root / "data").string());
        Check(storage.ok(), "startup must not deserialize historical messages");
    }
    int fatal_calls = 0;
    auto unique_core = MakeCore(bad_message_root,
                                1024,
                                10000,
                                0,
                                [&](const openevent::Status&) { ++fatal_calls; });
    auto core = std::shared_ptr<openevent::OpenEventCore>(std::move(unique_core));
    openevent::EventServiceImpl service(core);
    openevent::FetchRequest request;
    request.set_principal(100);
    request.set_token(token);
    request.set_from_seq(1);
    request.set_limit(1);
    request.add_channels(channel_id);
    openevent::FetchResponse response;
    grpc::Status status = service.Fetch(nullptr, &request, &response);
    Check(!status.ok() && status.error_code() == grpc::StatusCode::DATA_LOSS && fatal_calls == 1,
          "reading invalid stored message data must return DATA_LOSS and trigger shutdown");
    core.reset();

    std::filesystem::remove_all(base);
}

void TestDefaultPayloadLimit()
{
    openevent::ServerConfig config;
    Check(config.max_payload_bytes == 16777216, "default payload limit should be 16 MiB");
}

void TestServerConfigRequiresDataPaths()
{
    openevent::ServerConfig config;
    openevent::Status status = openevent::ValidateServerConfig(config);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "metadata path should be required");

    config.storage_path = "/tmp/openevent-data";
    status = openevent::ValidateServerConfig(config);
    Check(status.ok(), status.message());

    config.admin_listen_addr = config.grpc_listen_addr;
    status = openevent::ValidateServerConfig(config);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "public and admin listen addresses must be different");
}

void TestLoadServerConfigRequiresExistingFile()
{
    const auto path = std::filesystem::temp_directory_path() / "openevent_missing_config.yaml";
    std::filesystem::remove(path);

    auto config = openevent::LoadServerConfig(path.string());
    Check(!config.ok() && config.status().code() == grpc::StatusCode::INVALID_ARGUMENT,
          "missing config file should be rejected");
}

void TestLoadServerConfigReadsRequiredPaths()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_config_load";
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);
    const auto config_path = root / "server.yaml";
    const auto storage_path = root / "data";

    std::ofstream config_file(config_path);
    config_file << "grpc:\n"
                << "  listen_addr: \"127.0.0.1:19527\"\n"
                << "admin:\n"
                << "  listen_addr: \"127.0.0.1:19528\"\n"
                << "storage:\n"
                << "  path: \"" << storage_path.string() << "\"\n"
                << "limits:\n"
                << "  max_payload_bytes: 4096\n";
    config_file.close();

    auto config = openevent::LoadServerConfig(config_path.string());
    Check(config.ok(), config.status().message());
    Check(config.value().storage_path == storage_path.string(), "storage path should come from config");
    Check(config.value().max_payload_bytes == 4096, "payload limit should come from config");

    std::filesystem::remove_all(root);
}

void TestSystemChannelShape()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_system_channel";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);

    std::string token = AddToken(*core, 100);
    openevent::GetChannelRequest request;
    request.set_principal(100);
    request.set_token(token);
    request.set_channel_id(0);
    openevent::GetChannelResponse response;
    openevent::Status status = core->GetChannel(request, &response);
    Check(status.ok(), status.message());
    Check(response.channel().channel_id() == 0, "system channel id");
    Check(response.channel().visibility() == openevent::VISIBILITY_PROTECTED, "system channel visibility");
    Check(!response.channel().has_creator(), "system channel creator must be unset");
    Check(response.channel().members_size() == 0, "system channel members must be empty");
    Check(response.channel().name().empty(), "system channel name should use proto default");

    std::filesystem::remove_all(root);
}

}  // namespace

int main()
{
    TestPublishFetch();
    TestCasAbort();
    TestPrivateAcl();
    TestFetchChannelFilter();
    TestSharedSubscriptionSnapshotRefresh();
    TestRecipientFilter();
    TestRecipientMustBeChannelMember();
    TestPayloadLimit();
    TestGuaranteedNonCommitStatuses();
    TestListTokensPagination();
    TestListMessagesPagination();
    TestFetchScanBudgetAdvancesCursor();
    TestResponseSoftBudgetPagination();
    TestDeleteTokenOrdersBeforePublish();
    TestObjectWriteMetadataReadAndReopen();
    TestMessageObjectReferences();
    TestObjectWriteInjectedFailures();
    TestRpcStorageFailureTriggersFatalShutdown();
    TestObjectWriteCrashRecoveryBoundaries();
    TestConcurrentObjectWritesCanCommitOutOfIdOrder();
    TestObjectWriteReauthenticatesBeforeCommit();
    TestCancelledObjectWriteContinuesAfterPreparing();
    TestPreparingRecoveryAndNoDirectoryScan();
    TestCommittedObjectCorruptionIsFatal();
    TestObjectIoErrorClassificationAndPaths();
    TestUnifiedStorageReopen();
    TestUnifiedStorageLayout();
    TestNestedStoragePathInitialization();
    TestUint64KeyEncodingOrder();
    TestUnifiedStorageRejectsIncompleteInitialization();
    TestUnifiedStorageRejectsExtraRootEntries();
    TestUnifiedStorageRejectsInvalidState();
    TestUnifiedStorageStartupDoesNotScanMessages();
    TestDefaultPayloadLimit();
    TestServerConfigRequiresDataPaths();
    TestLoadServerConfigRequiresExistingFile();
    TestLoadServerConfigReadsRequiredPaths();
    TestSystemChannelShape();
    return 0;
}
