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
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>
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
#include "storage/rocksdb_status.h"
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
                  const std::string& value,
                  bool erase = false)
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
    status = erase ? db->Delete(SyncWriteOptions(), handles[index], key)
                   : db->Put(SyncWriteOptions(), handles[index], key, value);
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
    request.set_principal(principal);
    openevent::AddTokenResponse response;
    openevent::Status status = core.AddToken(request, &response);
    Check(status.ok(), status.message());
    return response.token();
}

uint64_t AllocateUuid(openevent::OpenEventCore& core)
{
    openevent::AllocateUuidsRequest request;
    request.set_count(1);
    openevent::AllocateUuidsResponse response;
    openevent::Status status = core.AllocateUuids(request, &response);
    Check(status.ok() && response.uuids_size() == 1, "failed to allocate message uuid");
    return response.uuids(0);
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
    const uint64_t uuid = AllocateUuid(core);
    openevent::PublishAutoSeqRequest request;
    request.set_principal(principal);
    request.set_token(token);
    request.set_channel_id(channel_id);
    request.set_payload(payload);
    request.set_uuid(uuid);
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
    Check(response.messages_size() == 2, "expected channel initialization and business message");
    Check(response.messages(0).seq() == 1 && response.messages(0).principal() == 0,
          "channel initialization must precede the first business message");
    Check(response.messages(1).payload() == "hello", "expected payload");
    Check(response.messages(1).ts_ms() >= before_publish_ms,
          "expected server timestamp to be captured after request receipt");
    Check(response.messages(1).ts_ms() <= after_publish_ms,
          "expected server timestamp to be captured before publish returned");
    Check(response.next_seq() == 3, "expected next_seq 3");
    Check(response.last_seq() == 2, "expected last_seq 2");
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
    publish.set_seq(3);
    publish.set_payload("bad");
    publish.set_uuid(AllocateUuid(*core));
    openevent::PublishResponse response;
    openevent::Status status = core->Publish(publish, &response);
    Check(!status.ok() && status.code() == grpc::StatusCode::ABORTED, "expected aborted CAS publish");

    std::filesystem::remove_all(root);
}

void TestUuidAllocationAndDeduplication()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_uuid_deduplication";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);
    const std::string token = AddToken(*core, 100);
    const uint64_t channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);

    openevent::AllocateUuidsRequest invalid_allocation;
    openevent::AllocateUuidsResponse allocated;
    openevent::Status status = core->AllocateUuids(invalid_allocation, &allocated);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "zero UUID allocation count must be rejected");
    invalid_allocation.set_count(1025);
    status = core->AllocateUuids(invalid_allocation, &allocated);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "oversized UUID allocation count must be rejected");

    openevent::AllocateUuidsRequest allocation;
    allocation.set_count(3);
    status = core->AllocateUuids(allocation, &allocated);
    Check(status.ok() && allocated.uuids_size() == 3 && allocated.uuids(0) == 2 &&
              allocated.uuids(1) == 3 && allocated.uuids(2) == 4,
          "UUID allocation must return a globally monotonic contiguous batch");

    openevent::GetSeqByUuidRequest get_seq;
    openevent::GetSeqByUuidResponse get_seq_response;
    status = core->GetSeqByUuid(get_seq, &get_seq_response);
    Check(status.ok() && get_seq_response.seq() == 0,
          "UUID zero must resolve to the system initialization message");
    get_seq.set_uuid(allocated.uuids(0));
    status = core->GetSeqByUuid(get_seq, &get_seq_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::NOT_FOUND,
          "UUID lookup must not report an allocated but unused UUID");
    get_seq.set_uuid(99);
    status = core->GetSeqByUuid(get_seq, &get_seq_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::NOT_FOUND,
          "UUID lookup must not report an unallocated UUID");

    openevent::PublishAutoSeqRequest publish;
    publish.set_principal(100);
    publish.set_token(token);
    publish.set_channel_id(channel_id);
    publish.set_payload("first");
    openevent::PublishAutoSeqResponse published;

    status = core->PublishAutoSeq(publish, &published);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "a missing UUID must be rejected");
    publish.set_uuid(99);
    status = core->PublishAutoSeq(publish, &published);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "a UUID not allocated by the server must be rejected");

    publish.set_uuid(allocated.uuids(0));
    status = core->PublishAutoSeq(publish, &published);
    Check(status.ok() && published.seq() == 2, status.message());
    get_seq.set_uuid(allocated.uuids(0));
    status = core->GetSeqByUuid(get_seq, &get_seq_response);
    Check(status.ok() && get_seq_response.seq() == published.seq(),
          "UUID lookup must return the committed message sequence");

    publish.set_payload("different-content-is-not-compared");
    status = core->PublishAutoSeq(publish, &published);
    Check(!status.ok() && status.code() == grpc::StatusCode::ALREADY_EXISTS,
          "a used UUID must reject later writes without comparing content");
    auto max_seq = core->MaxSeq();
    Check(max_seq.ok() && max_seq.value() == 2,
          "a duplicate UUID must not advance the message watermark");

    publish.set_uuid(allocated.uuids(1));
    status = core->PublishAutoSeq(publish, &published);
    Check(status.ok() && published.seq() == 3, status.message());
    core.reset();
    core = MakeCore(root);

    get_seq.set_uuid(allocated.uuids(1));
    status = core->GetSeqByUuid(get_seq, &get_seq_response);
    Check(status.ok() && get_seq_response.seq() == 3,
          "UUID lookup must survive storage reopen");
    publish.set_uuid(allocated.uuids(1));
    status = core->PublishAutoSeq(publish, &published);
    Check(!status.ok() && status.code() == grpc::StatusCode::ALREADY_EXISTS,
          "UUID consumption must survive storage reopen");
    publish.set_uuid(allocated.uuids(2));
    status = core->PublishAutoSeq(publish, &published);
    Check(status.ok() && published.seq() == 4, status.message());

    openevent::AllocateUuidsRequest continued_allocation;
    continued_allocation.set_count(2);
    openevent::AllocateUuidsResponse continued;
    status = core->AllocateUuids(continued_allocation, &continued);
    Check(status.ok() && continued.uuids_size() == 2 && continued.uuids(0) == 5 &&
              continued.uuids(1) == 6,
          "UUID allocator watermark must survive storage reopen");

    publish.set_uuid(continued.uuids(0));
    status = core->PublishAutoSeq(publish, &published);
    Check(status.ok() && published.seq() == 5, status.message());

    openevent::PublishRequest cas_publish;
    cas_publish.set_principal(100);
    cas_publish.set_token(token);
    cas_publish.set_channel_id(channel_id);
    cas_publish.set_seq(6);
    cas_publish.set_payload("manual-cas");
    cas_publish.set_uuid(continued.uuids(1));
    openevent::PublishResponse cas_response;
    status = core->Publish(cas_publish, &cas_response);
    Check(status.ok(), status.message());

    cas_publish.set_payload(std::string(1025, 'x'));
    status = core->Publish(cas_publish, &cas_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::ALREADY_EXISTS,
          "a manual Publish retry must check UUID before stale CAS and payload validation");

    openevent::FetchRequest fetch;
    fetch.set_principal(100);
    fetch.set_token(token);
    fetch.set_from_seq(2);
    fetch.set_limit(10);
    openevent::FetchResponse fetched;
    status = core->Fetch(fetch, &fetched);
    Check(status.ok() && fetched.messages_size() == 5 && fetched.messages(0).uuid() == 2 &&
              fetched.messages(1).uuid() == 3 && fetched.messages(2).uuid() == 4 &&
              fetched.messages(3).uuid() == 5 && fetched.messages(4).uuid() == 6,
          "stored EventMessage values must expose their allocated UUIDs");

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
    Check(response.next_seq() == 3, "next_seq should still advance globally");
    Check(response.last_seq() == 2, "last_seq should report committed tail");

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
    Check(response.messages_size() == 3 && response.messages(0).principal() == 0 &&
              response.messages(1).payload() == "secret" && response.messages(2).principal() == 0,
          "member should see channel initialization, private message, and member addition");

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
    Check(response.messages_size() == 2, "channel filter should return initialization and business message");
    Check(response.messages(0).channel_id() == first_channel_id, "channel filter should keep first channel");
    Check(response.messages(1).payload() == "first-channel", "channel filter should keep first payload");
    Check(response.next_seq() == 5, "channel filter should still advance globally");
    Check(response.last_seq() == 4, "last_seq should report committed tail");

    google::protobuf::RepeatedField<uint64_t> subscription_channels;
    subscription_channels.Add(second_channel_id);
    response.Clear();
    status = core->FetchSubscriptionBatch(
        100, token, 1, 10, false, subscription_channels, &response);
    Check(status.ok(), status.message());
    Check(response.messages_size() == 2,
          "subscription channel filter should return initialization and business message");
    Check(response.messages(0).channel_id() == second_channel_id,
          "subscription channel filter should keep the selected channel");
    Check(response.messages(1).payload() == "second-channel",
          "subscription channel filter should keep the selected payload");
    Check(response.next_seq() == 5,
          "subscription channel filter should advance past filtered messages");
    Check(response.last_seq() == 4,
          "subscription channel filter should report the committed tail");

    fetch.clear_channels();
    response.Clear();
    status = core->Fetch(fetch, &response);
    Check(status.ok(), status.message());
    Check(response.messages_size() == 4, "empty channel filter should return all visible messages");
    Check(response.last_seq() == 4, "empty channel filter last_seq");

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
    Check(status.ok() && max_seq == 1,
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
        100, token, 2, 10, false, channels, &response, &used_version);
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
    request.set_uuid(AllocateUuid(*core));
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
    auto_request.set_uuid(AllocateUuid(*core));
    openevent::PublishAutoSeqResponse auto_response;
    openevent::Status status = core->PublishAutoSeq(auto_request, &auto_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "recipient outside channel members should be rejected");

    openevent::PublishRequest publish;
    publish.set_principal(100);
    publish.set_token(token);
    publish.set_channel_id(channel_id);
    publish.set_seq(2);
    publish.add_recipients(200);
    publish.set_payload("direct");
    publish.set_uuid(AllocateUuid(*core));
    openevent::PublishResponse publish_response;
    status = core->Publish(publish, &publish_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "CAS publish recipient outside channel members should be rejected");

    auto max_seq = core->MaxSeq();
    Check(max_seq.ok(), max_seq.status().message());
    Check(max_seq.value() == 1, "invalid recipient must not advance max_seq");

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
    auto_request.set_uuid(AllocateUuid(*core));
    openevent::PublishAutoSeqResponse auto_response;
    openevent::Status status = core->PublishAutoSeq(auto_request, &auto_response);
    Check(status.ok(), status.message());
    Check(auto_response.seq() == 2, "payload at max limit should publish");

    auto_request.set_payload("12345");
    auto_request.set_uuid(AllocateUuid(*core));
    auto_response.Clear();
    status = core->PublishAutoSeq(auto_request, &auto_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::RESOURCE_EXHAUSTED,
          "oversized PublishAutoSeq payload should return RESOURCE_EXHAUSTED");

    openevent::PublishRequest publish;
    publish.set_principal(100);
    publish.set_token(token);
    publish.set_channel_id(channel_id);
    publish.set_seq(3);
    publish.set_payload("12345");
    publish.set_uuid(AllocateUuid(*core));
    openevent::PublishResponse publish_response;
    status = core->Publish(publish, &publish_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::RESOURCE_EXHAUSTED,
          "oversized Publish payload should return RESOURCE_EXHAUSTED");

    auto max_seq = core->MaxSeq();
    Check(max_seq.ok(), max_seq.status().message());
    Check(max_seq.value() == 2, "oversized payload must not advance max_seq");

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
    request.set_uuid(AllocateUuid(*core));
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
    cas.set_seq(3);
    cas.set_payload("ok");
    cas.set_uuid(AllocateUuid(*core));
    openevent::PublishResponse cas_response;
    status = core->Publish(cas, &cas_response);
    Check(!status.ok() && status.code() == grpc::StatusCode::ABORTED,
          "CAS conflict should guarantee non-commit");

    auto max_seq = core->MaxSeq();
    Check(max_seq.ok() && max_seq.value() == 1,
          "guaranteed non-commit statuses must not create a message or advance max_seq");

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
    uint64_t next_seq = 3;
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
    const auto public_channel = CreateChannel(*core, 100, owner_token, openevent::VISIBILITY_PUBLIC);
    PublishAuto(*core, 100, owner_token, public_channel, "third-visible");

    openevent::FetchRequest request;
    request.set_principal(200);
    request.set_token(outsider_token);
    request.set_from_seq(2);
    request.set_limit(10);
    openevent::FetchResponse response;
    openevent::Status status = core->Fetch(request, &response);
    Check(status.ok(), status.message());
    Check(response.messages_size() == 0, "scan-budget page may be empty when all records are hidden");
    Check(response.next_seq() == 4, "empty scan-budget page must advance past checked records");
    Check(response.last_seq() == 5, "scan-budget page should keep the snapshot tail");

    request.set_from_seq(response.next_seq());
    response.Clear();
    status = core->Fetch(request, &response);
    Check(status.ok(), status.message());
    Check(response.messages_size() == 2 && response.messages(0).principal() == 0 &&
              response.messages(1).payload() == "third-visible",
          "continuing an empty nonterminal page must find the later visible message");
    Check(response.next_seq() == 6, "second scan-budget page should reach the tail");

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
    fetch.set_from_seq(2);
    fetch.set_limit(10);
    openevent::FetchResponse fetch_response;
    openevent::Status status = core->Fetch(fetch, &fetch_response);
    Check(status.ok(), status.message());
    Check(fetch_response.messages_size() == 1 && fetch_response.messages(0).seq() == 2,
          "Fetch should allow one message to exceed the soft response budget");
    Check(fetch_response.next_seq() == 3, "Fetch must not consume a message deferred by byte budget");

    fetch.set_from_seq(fetch_response.next_seq());
    fetch_response.Clear();
    status = core->Fetch(fetch, &fetch_response);
    Check(status.ok(), status.message());
    Check(fetch_response.messages_size() == 1 && fetch_response.messages(0).seq() == 3,
          "Fetch should return the deferred message on the next page");
    Check(fetch_response.next_seq() == 4, "Fetch response-budget pagination should reach the tail");

    openevent::ListMessagesRequest list;
    list.set_from_seq(2);
    list.set_limit(10);
    openevent::ListMessagesResponse list_response;
    status = core->ListMessages(list, &list_response);
    Check(status.ok(), status.message());
    Check(list_response.messages_size() == 1 && list_response.messages(0).seq() == 2,
          "ListMessages should allow one message to exceed the soft response budget");
    Check(list_response.next_seq() == 3,
          "ListMessages must not consume a message deferred by byte budget");

    list.set_from_seq(list_response.next_seq());
    list_response.Clear();
    status = core->ListMessages(list, &list_response);
    Check(status.ok(), status.message());
    Check(list_response.messages_size() == 1 && list_response.messages(0).seq() == 3,
          "ListMessages should return the deferred message on the next page");
    Check(list_response.next_seq() == 4,
          "ListMessages response-budget pagination should reach the tail");

    std::filesystem::remove_all(root);
}

void TestPrincipalTokenCredentials()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_principal_tokens";
    std::filesystem::remove_all(root);
    const std::string shared_token = "shared-token";
    const std::string other_token = "other-token";
    {
        auto storage = openevent::UnifiedStorage::Open((root / "data").string());
        Check(storage.ok(), storage.status().message());
        rocksdb::WriteBatch batch;
        Check(storage.value()->PutToken(&batch, 100, shared_token).ok(), "store first credential");
        Check(storage.value()->PutToken(&batch, 200, shared_token).ok(), "reuse token for another principal");
        Check(storage.value()->PutToken(&batch, 100, other_token).ok(), "store another token for same principal");
        Check(storage.value()->Commit(&batch).ok(), "commit credential set");
        batch.Clear();
        Check(storage.value()->PutToken(&batch, 100, shared_token).ok(), "replay identical credential");
        Check(storage.value()->Commit(&batch).ok(), "identical credential remains one set entry");
    }

    auto core = MakeCore(root);
    auto authenticate = [&](uint64_t principal, const std::string& token) {
        openevent::GetStatusRequest request;
        request.set_principal(principal);
        request.set_token(token);
        openevent::GetStatusResponse response;
        return core->GetStatus(request, &response);
    };
    Check(authenticate(100, shared_token).ok() && authenticate(200, shared_token).ok(),
          "same token must authenticate both persisted principal/token pairs");
    Check(authenticate(100, other_token).ok(), "one principal can retain multiple tokens");
    Check(authenticate(300, shared_token).code() == grpc::StatusCode::UNAUTHENTICATED,
          "a token valid for another principal must not authenticate an absent pair");

    openevent::DeleteTokenRequest remove;
    remove.set_principal(300);
    remove.set_token(shared_token);
    openevent::DeleteTokenResponse response;
    Check(core->DeleteToken(remove, &response).ok(), "deleting an absent pair is idempotent");
    Check(authenticate(100, shared_token).ok() && authenticate(200, shared_token).ok(),
          "deleting absent pair must not delete same token for another principal");
    remove.set_principal(100);
    Check(core->DeleteToken(remove, &response).ok(), "delete one principal/token pair");
    Check(core->DeleteToken(remove, &response).ok(), "repeated pair deletion is idempotent");

    for (int pass = 0; pass < 2; ++pass) {
        Check(authenticate(100, shared_token).code() == grpc::StatusCode::UNAUTHENTICATED,
              "deleted pair must fail authentication before and after restart");
        Check(authenticate(200, shared_token).ok(),
              "deletion must preserve same token belonging to another principal");
        Check(authenticate(100, other_token).ok(),
              "deletion must preserve another token belonging to the same principal");
        if (pass == 0) {
            core.reset();
            core = MakeCore(root);
        }
    }
    core.reset();
    std::filesystem::remove_all(root);
}

void TestTokenManagementRejectsInvalidParameters()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_invalid_tokens";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);
    openevent::AddTokenRequest add;
    openevent::AddTokenResponse added;
    Check(core->AddToken(add, &added).code() == grpc::StatusCode::INVALID_ARGUMENT,
          "AddToken must reject principal zero");
    openevent::DeleteTokenRequest remove;
    remove.set_token("nonempty");
    openevent::DeleteTokenResponse removed;
    Check(core->DeleteToken(remove, &removed).code() == grpc::StatusCode::INVALID_ARGUMENT,
          "DeleteToken must reject principal zero even with a nonempty token");
    remove.set_principal(100);
    remove.clear_token();
    Check(core->DeleteToken(remove, &removed).code() == grpc::StatusCode::INVALID_ARGUMENT,
          "DeleteToken must reject an empty token even with a nonzero principal");
    core.reset();
    std::filesystem::remove_all(root);
}

void TestCorruptCredentialRecordIsFatal()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_corrupt_credential";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);
    const std::string token = AddToken(*core, 100);
    core.reset();
    PutRawRecord(root / "data" / "db", rocksdb::kDefaultColumnFamilyName,
                 std::string("token/") + openevent::EncodeUint64(100) + token, "unexpected-value");
    int fatal_calls = 0;
    core = MakeCore(root, 1024, 10000, 0, [&](const openevent::Status&) { ++fatal_calls; });
    openevent::GetStatusRequest request;
    request.set_principal(100);
    request.set_token(token);
    openevent::GetStatusResponse response;
    const auto status = core->HandleRpcStatus(core->GetStatus(request, &response));
    Check(status.code() == grpc::StatusCode::DATA_LOSS && fatal_calls == 1,
          "credential records with nonempty values must be fatal storage corruption");
    Check(core->GetStatus(request, &response).code() == grpc::StatusCode::UNAVAILABLE,
          "new RPCs must fail after credential storage corruption");
    core.reset();
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
    delete_request.set_principal(100);
    delete_request.set_token(token);
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
    Check(max_seq.ok() && max_seq.value() == 1, "unauthenticated publish must not advance max_seq");

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
    delete_token.set_principal(100);
    delete_token.set_token(token);
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

void TestBatchObjectMetadata()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_batch_object_metadata";
    std::filesystem::remove_all(root);
    auto core = std::shared_ptr<openevent::OpenEventCore>(MakeCore(root));
    const std::string token = AddToken(*core, 100);
    auto first = WriteObject(*core, 100, token, "first");
    auto write = MakeWriteObjectRequest(100, token, "second object");
    write.set_name("second.txt");
    write.set_type("text/plain");
    write.set_description("second description");
    openevent::WriteObjectResponse second;
    Check(core->WriteObject(write, &second).ok(), "write second batch metadata fixture");

    openevent::BatchGetObjectMetadataRequest request;
    std::vector<openevent::GetObjectMetadataResponse> expected;
    for (const auto* object : {&second, &first, &second}) {
        auto* key = request.add_object_keys();
        key->set_object_id(object->object_id());
        key->set_object_token(object->object_token());
        openevent::GetObjectMetadataRequest single;
        single.set_object_id(object->object_id());
        single.set_object_token(object->object_token());
        openevent::GetObjectMetadataResponse metadata;
        Check(core->GetObjectMetadata(single, &metadata).ok(), "read expected single metadata");
        expected.push_back(metadata);
    }
    openevent::BatchGetObjectMetadataResponse response;
    auto check_ordered_response = [&]() {
        Check(response.metadata_size() == 3, "batch metadata must preserve duplicate entries");
        for (int index = 0; index < response.metadata_size(); ++index) {
            Check(response.metadata(index).SerializeAsString() == expected[index].SerializeAsString(),
                  "batch metadata must preserve request order and match single metadata exactly");
        }
    };
    Check(core->BatchGetObjectMetadata(request, &response).ok(), "read ordered metadata batch");
    check_ordered_response();

    auto expect_failure = [&](const openevent::BatchGetObjectMetadataRequest& invalid,
                              grpc::StatusCode code) {
        response.add_metadata()->set_name("stale result");
        const auto status = core->BatchGetObjectMetadata(invalid, &response);
        Check(status.code() == code, "unexpected batch metadata error: " + status.message());
        Check(response.metadata().empty(), "failed batch metadata must expose no partial or stale results");
        return status;
    };
    openevent::BatchGetObjectMetadataRequest empty;
    expect_failure(empty, grpc::StatusCode::INVALID_ARGUMENT);
    auto invalid = request;
    // A repeated object still needs its own capability check.
    invalid.mutable_object_keys(2)->set_object_token("wrong-token");
    const auto wrong_token = expect_failure(invalid, grpc::StatusCode::NOT_FOUND);
    invalid = request;
    invalid.mutable_object_keys(1)->set_object_id(9999);
    const auto missing = expect_failure(invalid, grpc::StatusCode::NOT_FOUND);
    Check(wrong_token.message() == missing.message(),
          "batch metadata must not distinguish a wrong token from a missing object");
    // All key shapes must be checked before the earlier missing object is looked up.
    invalid.mutable_object_keys(2)->set_object_id(0);
    expect_failure(invalid, grpc::StatusCode::INVALID_ARGUMENT);
    invalid = request;
    invalid.mutable_object_keys(0)->set_object_token("wrong-token");
    invalid.mutable_object_keys(2)->clear_object_token();
    expect_failure(invalid, grpc::StatusCode::INVALID_ARGUMENT);

    openevent::BatchGetObjectMetadataRequest boundary;
    *boundary.add_object_keys() = request.object_keys(0);
    Check(core->BatchGetObjectMetadata(boundary, &response).ok() && response.metadata_size() == 1,
          "a one-object metadata batch must be accepted");
    for (int index = 1; index < 1024; ++index) {
        *boundary.add_object_keys() = request.object_keys(0);
    }
    Check(core->BatchGetObjectMetadata(boundary, &response).ok() && response.metadata_size() == 1024 &&
              response.metadata(1023).SerializeAsString() == expected[0].SerializeAsString(),
          "the inclusive 1024-item metadata limit must preserve the final duplicate");
    *boundary.add_object_keys() = request.object_keys(0);
    expect_failure(boundary, grpc::StatusCode::INVALID_ARGUMENT);

    Check(std::filesystem::remove(root / "data" / "objects" / std::to_string(first.object_id())),
          "remove first object data for metadata-only lookup");
    {
        std::ofstream truncated(root / "data" / "objects" / std::to_string(second.object_id()),
                                std::ios::binary | std::ios::trunc);
        truncated << "x";
        Check(truncated.good(), "truncate second object data for metadata-only lookup");
    }
    Check(core->BatchGetObjectMetadata(request, &response).ok(),
          "batch metadata must not open or validate missing or truncated object data");
    check_ordered_response();
    {
        openevent::ObjectStorageServiceImpl service(core);
        grpc::ServerBuilder builder;
        int port = 0;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(&service);
        auto server = builder.BuildAndStart();
        Check(server != nullptr && port > 0, "start batch metadata gRPC server");
        auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                                           grpc::InsecureChannelCredentials());
        auto stub = openevent::ObjectStorageService::NewStub(channel);
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        response.Clear();
        Check(stub->BatchGetObjectMetadata(&context, request, &response).ok(),
              "BatchGetObjectMetadata must be available through the generated gRPC service");
        check_ordered_response();
        server->Shutdown();
        server->Wait();
    }
    core.reset();
    std::filesystem::remove_all(root);
}

void TestBatchObjectMetadataWhilePreparing()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_batch_metadata_preparing";
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
        if (point != openevent::StorageFaultPoint::kAfterTemporaryFileCreate) {
            return openevent::Status::Ok();
        }
        std::unique_lock<std::mutex> lock(state->mutex);
        if (state->active) {
            state->blocked = true;
            state->cv.notify_all();
            state->cv.wait(lock, [&]() { return state->release; });
        }
        return openevent::Status::Ok();
    };
    auto storage = openevent::UnifiedStorage::Open((root / "data").string(), injector);
    Check(storage.ok(), storage.status().message());
    auto* storage_ptr = storage.value().get();
    auto core = std::make_unique<openevent::OpenEventCore>(std::move(storage.value()), 1024);
    const std::string token = AddToken(*core, 100);
    const auto committed = WriteObject(*core, 100, token, "first");
    state->active = true;
    openevent::WriteObjectResponse written;
    openevent::Status write_status;
    std::thread writer([&]() {
        write_status = core->WriteObject(MakeWriteObjectRequest(100, token, "pending"), &written);
    });
    {
        std::unique_lock<std::mutex> lock(state->mutex);
        Check(state->cv.wait_for(lock, std::chrono::seconds(5), [&]() { return state->blocked; }),
              "object write must reach PREPARING before the batch metadata query");
    }
    openevent::BatchGetObjectMetadataRequest request;
    auto* key = request.add_object_keys();
    key->set_object_id(committed.object_id());
    key->set_object_token(committed.object_token());
    {
        auto snapshot = storage_ptr->CreateSnapshot();
        Check(snapshot.ok(), snapshot.status().message());
        auto preparing = storage_ptr->GetPreparingObject(snapshot.value(), committed.object_id() + 1);
        Check(preparing.ok() && preparing.value().has_value(), "blocked write must be PREPARING");
        key = request.add_object_keys();
        key->set_object_id(preparing.value()->object_id);
        key->set_object_token(preparing.value()->object_token);
    }
    openevent::BatchGetObjectMetadataResponse response;
    const auto status = core->BatchGetObjectMetadata(request, &response);
    Check(status.code() == grpc::StatusCode::NOT_FOUND && response.metadata().empty(),
          "a PREPARING object must fail the whole metadata batch even with its correct token");
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->release = true;
    }
    state->cv.notify_all();
    writer.join();
    Check(write_status.ok(), write_status.message());
    Check(core->BatchGetObjectMetadata(request, &response).ok() && response.metadata_size() == 2 &&
              response.metadata(0).nbytes() == 5 && response.metadata(1).nbytes() == 7,
          "a new metadata batch must observe the object after its final commit");
    core.reset();
    std::filesystem::remove_all(root);
}

void TestBatchObjectMetadataCorruptionIsFatal()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_batch_metadata_corruption";
    std::filesystem::remove_all(root);
    openevent::BatchGetObjectMetadataRequest request;
    {
        auto core = MakeCore(root);
        const std::string token = AddToken(*core, 100);
        for (const auto* data : {"good", "corrupt"}) {
            const auto object = WriteObject(*core, 100, token, data);
            auto* key = request.add_object_keys();
            key->set_object_id(object.object_id());
            key->set_object_token(object.object_token());
        }
    }
    PutRawRecord(root / "data" / "db", "objects",
                 std::string("object/") + openevent::EncodeUint64(request.object_keys(1).object_id()),
                 "invalid object metadata");
    int fatal_calls = 0;
    auto unique_core = MakeCore(root, 1024, 10000, 0, [&](const openevent::Status&) { ++fatal_calls; });
    auto core = std::shared_ptr<openevent::OpenEventCore>(std::move(unique_core));
    {
        openevent::ObjectStorageServiceImpl service(core);
        openevent::BatchGetObjectMetadataResponse response;
        auto status = service.BatchGetObjectMetadata(nullptr, &request, &response);
        Check(status.error_code() == grpc::StatusCode::DATA_LOSS && fatal_calls == 1 &&
                  response.metadata().empty(),
              "batch metadata corruption must fail atomically and trigger the shared fatal path");
        request.mutable_object_keys()->RemoveLast();
        status = service.BatchGetObjectMetadata(nullptr, &request, &response);
        Check(status.error_code() == grpc::StatusCode::UNAVAILABLE && fatal_calls == 1 &&
                  response.metadata().empty(),
              "a healthy key must still be rejected after the server enters FAILED");
    }
    core.reset();
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
    publish.set_uuid(AllocateUuid(*core));
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
    publish.set_uuid(AllocateUuid(*core));
    status = core->PublishAutoSeq(publish, &publish_response);
    Check(status.ok(), "a message with exactly 1024 ObjectKeys must commit");
    const uint64_t watermark = publish_response.seq();

    auto* too_many = publish.add_object_keys();
    too_many->set_object_id(first.object_id());
    too_many->set_object_token(first.object_token());
    publish.set_uuid(AllocateUuid(*core));
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
    publish.set_uuid(AllocateUuid(*core));
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
    publish.set_uuid(AllocateUuid(*core));
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
    publish.set_uuid(AllocateUuid(*core));
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
         grpc::StatusCode::UNAVAILABLE, false, 1, true},
        {"preparing-after-commit", openevent::StorageFaultPoint::kAfterCommit, 0,
         grpc::StatusCode::UNAVAILABLE, false, 2, true},
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

        openevent::ObjectStorageServiceImpl object_service(core);
        openevent::BatchGetObjectMetadataRequest metadata_request;
        auto* key = metadata_request.add_object_keys();
        key->set_object_id(1);
        key->set_object_token("unused-after-failure");
        openevent::BatchGetObjectMetadataResponse metadata_response;
        status = object_service.BatchGetObjectMetadata(nullptr, &metadata_request, &metadata_response);
        Check(status.error_code() == grpc::StatusCode::UNAVAILABLE && fatal_calls == 1 &&
                  metadata_response.metadata().empty(),
              "batch metadata RPC must obey the shared FAILED gate");

        core.reset();
        std::filesystem::remove_all(root);
    };

    run_case("data_loss",
             openevent::Status(grpc::StatusCode::DATA_LOSS, "injected stored-data corruption"));
    run_case("rocksdb_unavailable",
             openevent::Status::Fatal(grpc::StatusCode::UNAVAILABLE,
                                      "injected RocksDB availability failure"));
}

void TestFatalStorageDiagnostics()
{
    const std::string secrets = "credential-secret object-token-secret payload-secret\nforged log line";
    const auto corruption = openevent::RocksToStatus(
        rocksdb::Status::Corruption(secrets), "read credential");
    const auto io_error = openevent::RocksToStatus(
        rocksdb::Status::IOError(secrets), "commit storage batch");
    Check(corruption.code() == grpc::StatusCode::DATA_LOSS &&
              io_error.code() == grpc::StatusCode::UNAVAILABLE,
          "safe diagnostics must preserve storage error codes");
    Check(corruption.FatalLogMessage().find("operation=read credential; error=Corruption") !=
              std::string::npos &&
              io_error.FatalLogMessage().find("operation=commit storage batch; error=IO error") !=
              std::string::npos,
          "RocksDB diagnostics must retain the operation and underlying error category");
    for (const auto& status : {corruption, io_error,
                               openevent::Status::Fatal(grpc::StatusCode::UNAVAILABLE, secrets)}) {
        const std::string line = status.FatalLogMessage();
        Check(line.find("secret") == std::string::npos && line.find('\n') == std::string::npos,
              "free-form storage errors must not leak credentials, payload or injected log lines");
    }

    const auto root = std::filesystem::temp_directory_path() / "openevent_fatal_diagnostics";
    std::filesystem::remove_all(root);
    std::atomic<int> fatal_calls{0};
    std::ostringstream log;
    auto core = MakeCore(root, 1024, 10000, 0, [&](const openevent::Status& status) {
        ++fatal_calls;
        log << status.FatalLogMessage() << '\n';
    });
    std::atomic<bool> start{false};
    std::vector<std::thread> threads;
    for (int index = 0; index < 16; ++index) {
        threads.emplace_back([&]() {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            Check(core->HandleRpcStatus(corruption).code() == grpc::StatusCode::DATA_LOSS,
                  "concurrent fatal errors must retain their RPC result");
        });
    }
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) {
        thread.join();
    }
    core->HandleRpcStatus(io_error);
    Check(fatal_calls == 1 && log.str() == corruption.FatalLogMessage() + '\n',
          "only the first fatal error may produce a diagnostic, even under concurrency");
    core.reset();
    std::filesystem::remove_all(root);
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
    delete_request.set_principal(100);
    delete_request.set_token(token);
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

void TestTimedOutPublishDoesNotCommitAfterCoordinatorWait()
{
    const auto root =
        std::filesystem::temp_directory_path() / "openevent_publish_deadline_after_coordinator_wait";
    std::filesystem::remove_all(root);
    struct State {
        std::mutex mutex;
        std::condition_variable cv;
        bool active = false;
        bool first_commit_blocked = false;
        bool release_first = false;
        int commit_attempts = 0;
    };
    auto state = std::make_shared<State>();
    auto injector = [state](openevent::StorageFaultPoint point) {
        if (point != openevent::StorageFaultPoint::kBeforeCommit) {
            return openevent::Status::Ok();
        }
        std::unique_lock<std::mutex> lock(state->mutex);
        if (!state->active) {
            return openevent::Status::Ok();
        }
        ++state->commit_attempts;
        if (state->commit_attempts == 1) {
            state->first_commit_blocked = true;
            state->cv.notify_all();
            state->cv.wait(lock, [&]() { return state->release_first; });
        }
        return openevent::Status::Ok();
    };

    auto unique_core = MakeCore(root, 1024, 10000, 0, {}, injector);
    const std::string token = AddToken(*unique_core, 100);
    const uint64_t channel_id =
        CreateChannel(*unique_core, 100, token, openevent::VISIBILITY_PUBLIC);
    const uint64_t first_uuid = AllocateUuid(*unique_core);
    const uint64_t timed_out_uuid = AllocateUuid(*unique_core);
    auto core = std::shared_ptr<openevent::OpenEventCore>(std::move(unique_core));
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->active = true;
    }

    openevent::PublishAutoSeqRequest first_request;
    first_request.set_principal(100);
    first_request.set_token(token);
    first_request.set_channel_id(channel_id);
    first_request.set_payload("first");
    first_request.set_uuid(first_uuid);
    openevent::PublishAutoSeqResponse first_response;
    openevent::Status first_status;
    std::thread first([&]() { first_status = core->PublishAutoSeq(first_request, &first_response); });
    {
        std::unique_lock<std::mutex> lock(state->mutex);
        state->cv.wait(lock, [&]() { return state->first_commit_blocked; });
    }

    openevent::EventServiceImpl service(core);
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    Check(server != nullptr && port > 0, "start publish deadline test gRPC server");
    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                                       grpc::InsecureChannelCredentials());
    auto stub = openevent::EventService::NewStub(channel);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(100));
    openevent::PublishAutoSeqRequest timed_out_request;
    timed_out_request.set_principal(100);
    timed_out_request.set_token(token);
    timed_out_request.set_channel_id(channel_id);
    timed_out_request.set_payload("timed-out");
    timed_out_request.set_uuid(timed_out_uuid);
    openevent::PublishAutoSeqResponse timed_out_response;
    grpc::Status timed_out_status =
        stub->PublishAutoSeq(&context, timed_out_request, &timed_out_response);
    Check(timed_out_status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED,
          "queued publish must reach its client deadline");

    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->release_first = true;
    }
    state->cv.notify_all();
    first.join();
    Check(first_status.ok() && first_response.seq() == 2,
          "the publish holding the coordinator must commit first");

    server->Shutdown();
    server->Wait();
    auto max_seq = core->MaxSeq();
    Check(max_seq.ok() && max_seq.value() == 2,
          "a publish whose deadline expired while waiting for the coordinator must not commit");
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        Check(state->commit_attempts == 1,
              "the timed-out publish must be rejected before attempting storage commit");
    }

    core.reset();
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
    publish.set_uuid(AllocateUuid(*core));
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
        publish.set_uuid(AllocateUuid(*core));
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
                  key.find("offset:") == std::string::npos && key.find("ch_last:") == std::string::npos,
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
                             created_column_families >= 1,
                             created_column_families >= 2);

        auto storage = openevent::UnifiedStorage::Open(root.string());
        Check(!storage.ok(), "incomplete initialization must be rejected without online repair");
        std::filesystem::remove_all(root);
    }

    const auto root = std::filesystem::temp_directory_path() / "openevent_missing_initial_metadata";
    for (const auto* key : {"meta:schema_version", "meta:max_seq", "meta:next_uuid",
                            "meta:next_channel_id", "meta:next_object_id"}) {
        std::filesystem::remove_all(root);
        {
            auto storage = openevent::UnifiedStorage::Open(root.string());
            Check(storage.ok(), storage.status().message());
        }
        PutRawRecord(root / "db", rocksdb::kDefaultColumnFamilyName, key, "", true);
        auto storage = openevent::UnifiedStorage::Open(root.string());
        Check(!storage.ok() && storage.status().code() == grpc::StatusCode::DATA_LOSS,
              std::string("missing required metadata must reject startup without repair: ") + key);
    }
    std::filesystem::remove_all(root);
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

    const auto bad_schema = base / "bad-schema";
    {
        auto initialized = openevent::UnifiedStorage::Open(bad_schema.string());
        Check(initialized.ok(), initialized.status().message());
    }
    PutRawRecord(bad_schema / "db", rocksdb::kDefaultColumnFamilyName, "meta:schema_version",
                 openevent::EncodeUint64(999));
    auto storage = openevent::UnifiedStorage::Open(bad_schema.string());
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

    const auto zero_next_uuid = base / "zero-next-uuid";
    {
        auto initialized = openevent::UnifiedStorage::Open(zero_next_uuid.string());
        Check(initialized.ok(), initialized.status().message());
    }
    PutRawRecord(zero_next_uuid / "db",
                 rocksdb::kDefaultColumnFamilyName,
                 "meta:next_uuid",
                 openevent::EncodeUint64(0));
    storage = openevent::UnifiedStorage::Open(zero_next_uuid.string());
    Check(!storage.ok(), "zero next UUID must be rejected");

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

void TestServerConfigPayloadLimit()
{
    openevent::ServerConfig config;
    config.storage_path = "/tmp/openevent-data";

    config.max_payload_bytes = openevent::kMaxPayloadBytesLimit;
    openevent::Status status = openevent::ValidateServerConfig(config);
    Check(status.ok(), "60 MiB payload limit should be accepted");

    config.max_payload_bytes = openevent::kMaxPayloadBytesLimit + 1;
    status = openevent::ValidateServerConfig(config);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT &&
              status.message() == "limits.max_payload_bytes must not exceed 62914560 (60 MiB)",
          "payload limit above 60 MiB should be rejected before listening");
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

    {
        std::ofstream oversized_config(config_path);
        oversized_config << "storage:\n"
                         << "  path: \"" << storage_path.string() << "\"\n"
                         << "limits:\n"
                         << "  max_payload_bytes: 62914561\n";
        oversized_config.close();

        auto rejected = openevent::LoadServerConfig(config_path.string());
        Check(!rejected.ok() && rejected.status().code() == grpc::StatusCode::INVALID_ARGUMENT,
              "config above 60 MiB should fail during load");
    }

    std::filesystem::remove_all(root);
}

google::protobuf::Struct CheckChannelSystemMessage(
    const openevent::EventMessage& message, const std::string& kind, uint64_t channel_id)
{
    Check(message.seq() > 0 && message.uuid() > 0 && message.principal() == 0 &&
              message.channel_id() == channel_id && message.recipients().empty() &&
              message.object_keys().empty(),
          "channel system messages use ordinary seq/UUID and reserved sender with no recipients or objects");
    google::protobuf::Struct envelope;
    const auto parsed = google::protobuf::util::JsonStringToMessage(message.payload(), &envelope);
    Check(parsed.ok(), "system payload must be valid JSON: " + parsed.ToString());
    Check(envelope.fields().size() == 2 && envelope.fields().contains("kind") &&
              envelope.fields().at("kind").string_value() == kind &&
              envelope.fields().contains("data") &&
              envelope.fields().at("data").kind_case() == google::protobuf::Value::kStructValue,
          "system envelope must contain only kind and data");
    const auto data = envelope.fields().at("data").struct_value();
    Check(data.fields().contains("schema_version") &&
              data.fields().at("schema_version").number_value() == 1,
          "system schema_version must be numeric one");
    return data;
}

void CheckJsonUint64(const std::string& json, const std::string& field, uint64_t value)
{
    Check(std::regex_search(json, std::regex("\"" + field + "\"\\s*:\\s*" +
                                               std::to_string(value) + "\\s*[,}]")),
          "system JSON must preserve the exact uint64 integer for " + field);
}

void TestChannelSystemMessages()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_channel_system_messages";
    std::filesystem::remove_all(root);
    // System metadata is not constrained by the business Publish payload limit.
    auto core = MakeCore(root, 4);
    const uint64_t owner = UINT64_C(9007199254740993);
    const uint64_t member = std::numeric_limits<uint64_t>::max();
    const auto token = AddToken(*core, owner);
    const auto outsider_token = AddToken(*core, 200);
    const auto before_uuid = AllocateUuid(*core);
    Check(before_uuid == 1, "first client UUID allocation");

    openevent::CreateChannelRequest create;
    create.set_principal(owner);
    create.set_token(token);
    create.set_name("会话\"\\\n\t");
    create.set_protocol("chat.\"v1\\");
    create.set_description(std::string("详情\r\b\f\t\n\"\\") + char(0) + char(1));
    create.set_visibility(openevent::VISIBILITY_PRIVATE);
    create.add_members(member);
    create.add_members(member);
    create.add_members(owner);
    const uint64_t before_ms = NowMs();
    openevent::CreateChannelResponse created;
    Check(core->CreateChannel(create, &created).ok(), "create channel with escaped metadata and uint64 members");
    const uint64_t after_ms = NowMs();
    const uint64_t channel = created.channel().channel_id();

    openevent::FetchRequest fetch;
    fetch.set_principal(owner);
    fetch.set_token(token);
    fetch.set_from_seq(1);
    fetch.set_limit(100);
    openevent::FetchResponse fetched;
    Check(core->Fetch(fetch, &fetched).ok() && fetched.messages_size() == 1,
          "creation writes exactly one event, without separate initial-member events");
    const auto initialized = fetched.messages(0);
    const auto data = CheckChannelSystemMessage(initialized, "system.channel_initialized", channel);
    Check(initialized.seq() == 1 && initialized.uuid() == before_uuid + 1 &&
              initialized.ts_ms() >= before_ms && initialized.ts_ms() <= after_ms,
          "channel initialization has the next global seq/UUID and commit timestamp");
    Check(data.fields().size() == 7 && data.fields().at("name").string_value() == create.name() &&
              data.fields().at("protocol").string_value() == create.protocol() &&
              data.fields().at("description").string_value() == create.description() &&
              data.fields().at("visibility").string_value() == "private" &&
              data.fields().at("members").list_value().values_size() == 2,
          "initialization preserves every Channel attribute, escapes control characters, and deduplicates members");
    CheckJsonUint64(initialized.payload(), "creator", owner);
    for (const auto principal : {owner, member}) {
        Check(std::regex_search(initialized.payload(), std::regex("[\\[,]\\s*" +
                               std::to_string(principal) + "\\s*[,\\]]")),
              "members remain exact JSON integers even above the double-precision range");
    }
    Check(AllocateUuid(*core) == initialized.uuid() + 1,
          "client allocation after a system event must not reuse its UUID");
    PublishAuto(*core, owner, token, channel, "chat");

    openevent::AddMemberRequest add;
    add.set_principal(owner);
    add.set_token(token);
    add.set_channel_id(channel);
    add.set_target_principal(200);
    openevent::AddMemberResponse added;
    Check(core->AddMember(add, &added).ok(), "add member with an event");
    openevent::RemoveMemberRequest remove;
    remove.set_principal(owner);
    remove.set_token(token);
    remove.set_channel_id(channel);
    remove.set_target_principal(member);
    openevent::RemoveMemberResponse removed;
    Check(core->RemoveMember(remove, &removed).ok(), "remove member with an event");
    Check(core->Fetch(fetch, &fetched).ok() && fetched.messages_size() == 4 &&
              fetched.messages(1).principal() == owner && fetched.messages(1).payload() == "chat",
          "channel initialization is first, followed by business and membership events in commit order");
    const auto added_data = CheckChannelSystemMessage(fetched.messages(2), "system.member_added", channel);
    const auto removed_data = CheckChannelSystemMessage(fetched.messages(3), "system.member_removed", channel);
    Check(added_data.fields().size() == 2 && removed_data.fields().size() == 2,
          "member events contain only schema_version and member");
    CheckJsonUint64(fetched.messages(2).payload(), "member", 200);
    CheckJsonUint64(fetched.messages(3).payload(), "member", member);
    Check(fetched.messages(2).uuid() == 5 && fetched.messages(3).uuid() == 6 &&
              AllocateUuid(*core) == 7,
          "client allocations, business messages, and member events share one UUID space");
    for (const auto& message : fetched.messages()) {
        openevent::GetSeqByUuidRequest lookup;
        lookup.set_uuid(message.uuid());
        openevent::GetSeqByUuidResponse found;
        Check(core->GetSeqByUuid(lookup, &found).ok() && found.seq() == message.seq(),
              "UUID lookup includes channel and membership system messages");
    }

    // Public/protected initialization is readable without membership and keeps its visibility name.
    for (const auto visibility : {openevent::VISIBILITY_PUBLIC, openevent::VISIBILITY_PROTECTED}) {
        const auto id = CreateChannel(*core, owner, token, visibility);
        fetch.set_principal(200);
        fetch.set_token(outsider_token);
        fetch.clear_channels();
        fetch.add_channels(id);
        Check(core->Fetch(fetch, &fetched).ok() && fetched.messages_size() == 1,
              "system events in readable public/protected channels do not require membership");
        const auto visible_data = CheckChannelSystemMessage(fetched.messages(0), "system.channel_initialized", id);
        Check(visible_data.fields().at("visibility").string_value() ==
                  (visibility == openevent::VISIBILITY_PUBLIC ? "public" : "protected"),
              "initialization encodes visibility as the documented string");
    }
    core.reset();
    std::filesystem::remove_all(root);
}

void TestChannelSystemMessageAclAndNoOps()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_channel_system_acl";
    std::filesystem::remove_all(root);
    bool count_commits = false;
    int commits = 0;
    auto core = MakeCore(root, 1024, 10000, 0, {}, [&](openevent::StorageFaultPoint point) {
        if (count_commits && point == openevent::StorageFaultPoint::kBeforeCommit) ++commits;
        return openevent::Status::Ok();
    });
    const auto token = AddToken(*core, 100);
    const auto member_token = AddToken(*core, 200);
    const auto channel = CreateChannel(*core, 100, token, openevent::VISIBILITY_PRIVATE);
    google::protobuf::RepeatedField<uint64_t> all_channels;
    openevent::FetchResponse batch;
    Check(core->FetchSubscriptionBatch(200, member_token, 1, 100, false, all_channels, &batch).ok() &&
              batch.messages().empty() && batch.next_seq() == 2,
          "all-channel subscription filters unreadable initialization while advancing its cursor");
    openevent::AddMemberRequest add;
    add.set_principal(100);
    add.set_token(token);
    add.set_channel_id(channel);
    add.set_target_principal(200);
    openevent::AddMemberResponse added;
    Check(core->AddMember(add, &added).ok(), "add subscribing member");
    Check(core->FetchSubscriptionBatch(200, member_token, 2, 100, false, all_channels, &batch).ok() &&
              batch.messages_size() == 1 && batch.messages(0).seq() == 2,
          "all-channel subscription discovers newly readable channels through their member event");
    CheckChannelSystemMessage(batch.messages(0), "system.member_added", channel);
    Check(core->FetchSubscriptionBatch(200, member_token, 1, 100, true, all_channels, &batch).ok() &&
              batch.messages().empty() && batch.next_seq() == 3,
          "recipient-only subscription filters channel and membership system events");

    count_commits = true;
    Check(core->AddMember(add, &added).code() == grpc::StatusCode::ALREADY_EXISTS,
          "duplicate member addition is rejected");
    add.set_target_principal(0);
    Check(core->AddMember(add, &added).code() == grpc::StatusCode::INVALID_ARGUMENT,
          "invalid member addition is rejected");
    openevent::CreateChannelRequest invalid_create;
    invalid_create.set_principal(100);
    invalid_create.set_token(token);
    openevent::CreateChannelResponse invalid_created;
    Check(core->CreateChannel(invalid_create, &invalid_created).code() == grpc::StatusCode::INVALID_ARGUMENT,
          "invalid channel creation is rejected");
    openevent::RemoveMemberRequest remove;
    remove.set_principal(100);
    remove.set_token(token);
    remove.set_channel_id(channel);
    remove.set_target_principal(300);
    openevent::RemoveMemberResponse removed;
    Check(core->RemoveMember(remove, &removed).ok(), "removing an absent member is an idempotent success");
    remove.set_target_principal(100);
    Check(core->RemoveMember(remove, &removed).code() == grpc::StatusCode::PERMISSION_DENIED,
          "removing the creator is rejected");
    Check(commits == 0, "no-op and invalid channel mutations must not attempt a storage commit");
    count_commits = false;
    auto max_seq = core->MaxSeq();
    Check(max_seq.ok() && max_seq.value() == 2 && AllocateUuid(*core) == 3,
          "no-op and rejected channel mutations preserve message and UUID watermarks");

    remove.set_target_principal(200);
    Check(core->RemoveMember(remove, &removed).ok(), "remove subscribed private member");
    Check(core->FetchSubscriptionBatch(200, member_token, 3, 100, false, all_channels, &batch).ok() &&
              batch.messages().empty() && batch.next_seq() == 4,
          "removed private member cannot read its own removal event through an all-channel subscription");
    google::protobuf::RepeatedField<uint64_t> explicit_channels;
    explicit_channels.Add(channel);
    Check(core->FetchSubscriptionBatch(200, member_token, 3, 100, false, explicit_channels, &batch).code() ==
              grpc::StatusCode::PERMISSION_DENIED,
          "system removal does not bypass explicit private-channel ACL checks");
    openevent::ListMessagesRequest list;
    list.set_from_seq(1);
    list.set_limit(100);
    openevent::ListMessagesResponse listed;
    Check(core->ListMessages(list, &listed).ok() && listed.messages_size() == 3,
          "administrative listing includes private initialization and both real member events");
    CheckChannelSystemMessage(listed.messages(2), "system.member_removed", channel);
    openevent::CreateChannelRequest next_channel;
    next_channel.set_principal(100);
    next_channel.set_token(token);
    next_channel.set_name("new-conversation");
    next_channel.set_protocol("chat.v1");
    next_channel.set_visibility(openevent::VISIBILITY_PRIVATE);
    next_channel.add_members(200);
    openevent::CreateChannelResponse next_created;
    Check(core->CreateChannel(next_channel, &next_created).ok() && next_created.channel().channel_id() == 2,
          "rejected creation must not consume a channel ID");
    Check(core->FetchSubscriptionBatch(200, member_token, 4, 100, false, all_channels, &batch).ok() &&
              batch.messages_size() == 1 && batch.messages(0).channel_id() == 2,
          "existing all-channel subscription discovers a newly created private channel with initial membership");
    CheckChannelSystemMessage(batch.messages(0), "system.channel_initialized", 2);
    core.reset();
    std::filesystem::remove_all(root);
}

void TestChannelMemberLimits()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_channel_member_limits";
    std::filesystem::remove_all(root);
    bool count_commits = false;
    int commits = 0;
    auto core = MakeCore(root, 1024, 10000, 0, {}, [&](openevent::StorageFaultPoint point) {
        if (count_commits && point == openevent::StorageFaultPoint::kBeforeCommit) ++commits;
        return openevent::Status::Ok();
    });
    const auto token = AddToken(*core, 100);
    openevent::CreateChannelRequest create;
    create.set_principal(100);
    create.set_token(token);
    create.set_name("bounded-members");
    create.set_visibility(openevent::VISIBILITY_PRIVATE);
    for (uint64_t member = 2000; member < 3024; ++member) create.add_members(member);
    openevent::CreateChannelResponse created;

    count_commits = true;
    Check(core->CreateChannel(create, &created).code() == grpc::StatusCode::RESOURCE_EXHAUSTED,
          "1024 distinct requested members plus an absent creator exceed the total member limit");
    create.clear_members();
    for (int index = 0; index < 1025; ++index) create.add_members(100);
    Check(core->CreateChannel(create, &created).code() == grpc::StatusCode::INVALID_ARGUMENT,
          "the 1024-item request shape limit applies before deduplicating members");
    const auto empty_seq = core->MaxSeq();
    Check(commits == 0 && empty_seq.ok() && empty_seq.value() == 0,
          "rejected creations must not commit metadata or system events");
    count_commits = false;

    create.clear_members();
    for (uint64_t member = 2000; member < 3023; ++member) create.add_members(member);
    Check(core->CreateChannel(create, &created).ok() &&
              created.channel().channel_id() == 1 && created.channel().members_size() == 1024 &&
              std::count(created.channel().members().begin(), created.channel().members().end(), 100) == 1,
          "1023 requested members plus the creator fill a channel without consuming rejected channel IDs");

    create.add_members(100);
    Check(core->CreateChannel(create, &created).ok() &&
              created.channel().channel_id() == 2 && created.channel().members_size() == 1024 &&
              std::count(created.channel().members().begin(), created.channel().members().end(), 100) == 1,
          "1024 requested members including the creator fit without counting the creator twice");

    create.clear_members();
    for (uint64_t member = 2000; member < 3022; ++member) create.add_members(member);
    create.add_members(2000);
    create.add_members(2000);
    Check(core->CreateChannel(create, &created).ok() &&
              created.channel().channel_id() == 3 && created.channel().members_size() == 1023,
          "deduplicating a 1024-item request leaves room for one additional member");
    const auto channel_id = created.channel().channel_id();

    openevent::AddMemberRequest add;
    add.set_principal(100);
    add.set_token(token);
    add.set_channel_id(channel_id);
    add.set_target_principal(4000);
    openevent::AddMemberResponse added;
    Check(core->AddMember(add, &added).ok(), "adding the 1024th total member succeeds");

    openevent::GetChannelRequest get;
    get.set_principal(100);
    get.set_token(token);
    get.set_channel_id(channel_id);
    openevent::GetChannelResponse channel;
    Check(core->GetChannel(get, &channel).ok() && channel.channel().members_size() == 1024,
          "the full channel remains readable with all 1024 members");
    const auto full_channel = channel.channel().SerializeAsString();
    Check(AllocateUuid(*core) == 5, "only three creations and the successful addition consumed system UUIDs");

    count_commits = true;
    add.set_target_principal(4001);
    Check(core->AddMember(add, &added).code() == grpc::StatusCode::RESOURCE_EXHAUSTED,
          "adding the 1025th total member is rejected");
    add.set_target_principal(4000);
    Check(core->AddMember(add, &added).code() == grpc::StatusCode::ALREADY_EXISTS,
          "an existing member takes precedence over the full-channel error");
    add.set_target_principal(100);
    Check(core->AddMember(add, &added).code() == grpc::StatusCode::ALREADY_EXISTS,
          "the creator is also an existing member when the channel is full");
    const auto unchanged_seq = core->MaxSeq();
    Check(commits == 0 && unchanged_seq.ok() && unchanged_seq.value() == 4 &&
              core->GetChannel(get, &channel).ok() && channel.channel().SerializeAsString() == full_channel,
          "rejected additions preserve channel state and do not attempt any event or watermark commit");
    count_commits = false;
    Check(AllocateUuid(*core) == 6, "rejected additions do not consume UUIDs");

    openevent::RemoveMemberRequest remove;
    remove.set_principal(100);
    remove.set_token(token);
    remove.set_channel_id(channel_id);
    remove.set_target_principal(2000);
    openevent::RemoveMemberResponse removed;
    Check(core->RemoveMember(remove, &removed).ok(), "removing a member frees a slot");
    add.set_target_principal(4001);
    Check(core->AddMember(add, &added).ok(), "a previously rejected member can use the freed slot");
    Check(core->GetChannel(get, &channel).ok() && channel.channel().members_size() == 1024 &&
              std::count(channel.channel().members().begin(), channel.channel().members().end(), 2000) == 0 &&
              std::count(channel.channel().members().begin(), channel.channel().members().end(), 4001) == 1,
          "the replacement leaves exactly 1024 distinct members");
    create.clear_members();
    Check(core->CreateChannel(create, &created).ok() && created.channel().channel_id() == 4,
          "rejected member mutations do not advance the channel ID watermark");

    openevent::ListMessagesRequest list;
    list.set_from_seq(1);
    list.set_limit(100);
    openevent::ListMessagesResponse listed;
    Check(core->ListMessages(list, &listed).ok() && listed.messages_size() == 7 && listed.last_seq() == 7,
          "only successful channel mutations write system events and advance max_seq");
    const std::vector<uint64_t> expected_uuids{1, 2, 3, 4, 7, 8, 9};
    for (int index = 0; index < listed.messages_size(); ++index) {
        Check(listed.messages(index).seq() == static_cast<uint64_t>(index + 1) &&
                  listed.messages(index).uuid() == expected_uuids[index],
              "rejected member changes leave no message or UUID allocation gaps");
    }
    CheckChannelSystemMessage(listed.messages(3), "system.member_added", channel_id);
    CheckChannelSystemMessage(listed.messages(4), "system.member_removed", channel_id);
    CheckChannelSystemMessage(listed.messages(5), "system.member_added", channel_id);
    core.reset();
    std::filesystem::remove_all(root);
}

void TestChannelSystemMessageCrashRecovery()
{
    using Point = openevent::StorageFaultPoint;
    for (const auto operation : {std::string("create"), std::string("add"), std::string("remove")}) {
        for (const auto point : {Point::kBeforeCommit, Point::kAfterCommit}) {
            const auto root = std::filesystem::temp_directory_path() /
                ("openevent_channel_crash_" + operation + "_" + std::to_string(static_cast<int>(point)));
            std::filesystem::remove_all(root);
            std::string token;
            uint64_t last_client_uuid = 0;
            {
                auto core = MakeCore(root);
                token = AddToken(*core, 100);
                if (operation != "create") {
                    openevent::CreateChannelRequest create;
                    create.set_principal(100);
                    create.set_token(token);
                    create.set_name("existing");
                    create.set_visibility(openevent::VISIBILITY_PRIVATE);
                    if (operation == "remove") create.add_members(200);
                    openevent::CreateChannelResponse response;
                    Check(core->CreateChannel(create, &response).ok(), "prepare channel crash test");
                }
                last_client_uuid = AllocateUuid(*core);
            }
            const auto child = ::fork();
            Check(child >= 0, "fork channel mutation crash test");
            if (child == 0) {
                auto core = MakeCore(root, 1024, 10000, 0, {}, [point](Point observed) {
                    if (observed == point) ::_exit(73);
                    return openevent::Status::Ok();
                });
                if (operation == "create") {
                    CreateChannel(*core, 100, token, openevent::VISIBILITY_PRIVATE);
                } else if (operation == "add") {
                    openevent::AddMemberRequest request;
                    request.set_principal(100);
                    request.set_token(token);
                    request.set_channel_id(1);
                    request.set_target_principal(200);
                    openevent::AddMemberResponse response;
                    core->AddMember(request, &response);
                } else {
                    openevent::RemoveMemberRequest request;
                    request.set_principal(100);
                    request.set_token(token);
                    request.set_channel_id(1);
                    request.set_target_principal(200);
                    openevent::RemoveMemberResponse response;
                    core->RemoveMember(request, &response);
                }
                ::_exit(74);
            }
            int result = 0;
            Check(::waitpid(child, &result, 0) == child && WIFEXITED(result) && WEXITSTATUS(result) == 73,
                  "channel mutation must crash at the requested commit boundary");
            const bool committed = point == Point::kAfterCommit;
            const uint64_t old_seq = operation == "create" ? 0 : 1;
            std::string persisted_event;
            for (int reopen = 0; reopen < 2; ++reopen) {
                auto storage = openevent::UnifiedStorage::Open((root / "data").string());
                Check(storage.ok(), storage.status().message());
                auto snapshot = storage.value()->CreateSnapshot();
                Check(snapshot.ok(), snapshot.status().message());
                const auto max_seq = storage.value()->GetMaxSeq(snapshot.value());
                const auto next_uuid = storage.value()->GetNextUuid(snapshot.value());
                const auto next_channel = storage.value()->GetNextChannelId(snapshot.value());
                const auto used_uuid = storage.value()->GetUsedUuidSeq(snapshot.value(), last_client_uuid + 1);
                const auto channel = storage.value()->GetChannel(snapshot.value(), 1);
                Check(max_seq.ok() && max_seq.value() == old_seq + committed &&
                          next_uuid.ok() && next_uuid.value() == last_client_uuid + 1 + committed &&
                          next_channel.ok() && next_channel.value() ==
                              (operation == "create" && !committed ? 1 : 2),
                      "channel, message, and UUID allocation watermarks recover as one atomic batch");
                Check(used_uuid.ok() && used_uuid.value().has_value() == committed &&
                          (!committed || used_uuid.value().value() == old_seq + 1),
                      "system message UUID index commits with channel state");
                const bool exists = operation != "create" || committed;
                Check(channel.ok() && channel.value().has_value() == exists,
                      "channel creation must recover together with its initialization event");
                if (exists) {
                    const auto& members = channel.value()->members();
                    const bool has_member = std::find(members.begin(), members.end(), 200) != members.end();
                    const bool expected_member = operation == "add" ? committed :
                                                 operation == "remove" ? !committed : false;
                    Check(has_member == expected_member, "membership recovers on the same side of the commit as its event");
                }
                uint64_t messages = 0;
                auto scan = storage.value()->ScanMessages(snapshot.value(), 0, max_seq.value(), 100,
                    [&](const openevent::EventMessage& message) -> openevent::Result<openevent::MessageScanAction> {
                        ++messages;
                        if (committed && message.seq() == old_seq + 1) {
                            CheckChannelSystemMessage(message, operation == "create" ? "system.channel_initialized" :
                                operation == "add" ? "system.member_added" : "system.member_removed", 1);
                            Check(message.uuid() == last_client_uuid + 1, "recovered event uses its committed UUID");
                            if (reopen == 0) persisted_event = message.SerializeAsString();
                            else Check(message.SerializeAsString() == persisted_event,
                                       "restart must not regenerate the event or its timestamp");
                        }
                        return openevent::MessageScanAction::kContinue;
                    });
                Check(scan.ok() && messages == max_seq.value() + 1,
                      "recovered log has every committed event exactly once and no partial mutation event");
            }
            std::filesystem::remove_all(root);
        }
    }
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
    Check(response.channel().name() == "system", "system channel name");
    Check(response.channel().protocol() == "system.v1", "system channel protocol");

    std::filesystem::remove_all(root);
}

void TestInitializationCommitBoundary()
{
    using Point = openevent::StorageFaultPoint;
    for (const auto point : {Point::kBeforeInitializationCommit, Point::kAfterInitializationCommit}) {
        const auto root = std::filesystem::temp_directory_path() /
                          ("openevent_initialization_crash_" + std::to_string(static_cast<int>(point)));
        std::filesystem::remove_all(root);
        const pid_t child = ::fork();
        Check(child >= 0, "fork initialization crash test");
        if (child == 0) {
            openevent::UnifiedStorage::Open(root.string(), [point](Point observed) {
                if (observed == point) {
                    ::_exit(73);
                }
                return openevent::Status::Ok();
            });
            ::_exit(74);
        }
        int result = 0;
        Check(::waitpid(child, &result, 0) == child && WIFEXITED(result) && WEXITSTATUS(result) == 73,
              "process must crash at the requested initialization boundary");
        {
            auto storage = openevent::UnifiedStorage::Open(root.string(), [](Point observed) {
                Check(observed != Point::kBeforeInitializationCommit &&
                          observed != Point::kAfterInitializationCommit,
                      "reopening an interrupted initialization must not create new initial state");
                return openevent::Status::Ok();
            });
            Check(storage.ok() == (point == Point::kAfterInitializationCommit),
                  "only the synchronous initial-state batch completes initialization");
            if (storage.ok()) {
                auto snapshot = storage.value()->CreateSnapshot();
                Check(snapshot.ok(), snapshot.status().message());
                auto max_seq = storage.value()->GetMaxSeq(snapshot.value());
                auto system_seq = storage.value()->GetUsedUuidSeq(snapshot.value(), 0);
                Check(max_seq.ok() && max_seq.value() == 0 && system_seq.ok() &&
                          system_seq.value().has_value() && system_seq.value().value() == 0,
                      "committed initialization must preserve its message watermark and UUID zero index");
                size_t messages = 0;
                auto scan = storage.value()->ScanMessages(snapshot.value(), 0, 0, 1,
                    [&](const openevent::EventMessage& message) -> openevent::Result<openevent::MessageScanAction> {
                        Check(message.seq() == 0 && message.uuid() == 0,
                              "committed initialization must preserve its system message");
                        ++messages;
                        return openevent::MessageScanAction::kContinue;
                    });
                Check(scan.ok() && messages == 1, "system message must remain readable after the crash");
            }
        }
        std::filesystem::remove_all(root);
    }
}

void TestMessageCommitFailureAndRestart()
{
    using Point = openevent::StorageFaultPoint;
    for (const auto point : {Point::kBeforeCommit, Point::kAfterCommit}) {
        const auto root = std::filesystem::temp_directory_path() /
                          ("openevent_message_commit_failure_" + std::to_string(static_cast<int>(point)));
        std::filesystem::remove_all(root);
        bool active = false;
        int fatal_calls = 0;
        auto core = MakeCore(root, 1024, 10000, 0,
            [&](const openevent::Status&) { ++fatal_calls; },
            [&](Point observed) {
                return active && observed == point
                           ? openevent::Status(grpc::StatusCode::UNAVAILABLE, "injected DB write failure")
                           : openevent::Status::Ok();
            });
        auto token = AddToken(*core, 100);
        auto channel = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);
        auto uuid = AllocateUuid(*core);
        openevent::PublishAutoSeqRequest request;
        request.set_principal(100);
        request.set_token(token);
        request.set_channel_id(channel);
        request.set_uuid(uuid);
        request.set_payload("atomic-message");
        openevent::PublishAutoSeqResponse response;
        active = true;
        auto status = core->PublishAutoSeq(request, &response);
        Check(status.code() == grpc::StatusCode::UNAVAILABLE && fatal_calls == 1,
              "every DB write error must make the core unavailable");
        Check(core->PublishAutoSeq(request, &response).code() == grpc::StatusCode::UNAVAILABLE,
              "no new publish starts after the fatal error");
        core.reset();
        core = MakeCore(root);
        auto max_seq = core->MaxSeq();
        Check(max_seq.ok() && max_seq.value() == (point == Point::kAfterCommit ? 2 : 1),
              "restored state decides whether the failed write persisted");
        status = core->PublishAutoSeq(request, &response);
        Check(point == Point::kAfterCommit ? status.code() == grpc::StatusCode::ALREADY_EXISTS : status.ok(),
              "UUID and message must recover together and support retry with the same UUID");
        openevent::GetSeqByUuidRequest lookup;
        lookup.set_uuid(uuid);
        openevent::GetSeqByUuidResponse found;
        Check(core->GetSeqByUuid(lookup, &found).ok() && found.seq() == 2,
              "the retried logical message must have one committed seq");
        core.reset();
        std::filesystem::remove_all(root);
    }
}

void TestSystemMessagePersistenceAndValidation()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_system_message";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);
    const auto token = AddToken(*core, 100);
    openevent::FetchRequest fetch;
    fetch.set_principal(100);
    fetch.set_token(token);
    fetch.set_limit(1);
    openevent::FetchResponse fetched;
    Check(core->Fetch(fetch, &fetched).ok() && fetched.messages_size() == 1,
          "from seq zero must return the initialization message");
    const auto original = fetched.messages(0);
    Check(original.seq() == 0 && original.principal() == 0 && original.uuid() == 0 &&
              original.channel_id() == 0 && original.recipients().empty() && original.object_keys().empty(),
          "initialization message reserved fields");
    Check(fetched.next_seq() == 1 && fetched.last_seq() == 0, "system page cursor");
    fetch.set_only_my_recipient(true);
    Check(core->Fetch(fetch, &fetched).ok() && fetched.messages().empty() && fetched.next_seq() == 1,
          "recipient filtering must consume the system message without returning it");
    const auto channel = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);
    PublishAuto(*core, 100, token, channel, "business");
    core.reset();
    core = MakeCore(root);
    fetch.set_only_my_recipient(false);
    Check(core->Fetch(fetch, &fetched).ok() && fetched.messages(0).SerializeAsString() == original.SerializeAsString(),
          "restart must preserve the exact initialization event and timestamp");
    openevent::GetStatusRequest request;
    request.set_principal(100);
    request.set_token(token);
    openevent::GetStatusResponse status;
    Check(core->GetStatus(request, &status).ok() && status.max_seq() == 2,
          "status preserves the latest business message sequence after restart");
    core.reset();

    const auto db_path = root / "data" / "db";
    const auto message_key = std::string("msg/") + openevent::EncodeUint64(0);
    const auto uuid_key = std::string("uuid/") + openevent::EncodeUint64(0);
    auto rejected = [&]() {
        auto storage = openevent::UnifiedStorage::Open((root / "data").string());
        Check(!storage.ok() && storage.status().code() == grpc::StatusCode::DATA_LOSS,
              "invalid system state must reject startup without repairing it");
    };
    PutRawRecord(db_path, "messages", message_key, "", true);
    rejected();
    PutRawRecord(db_path, "messages", message_key, "malformed protobuf");
    rejected();
    for (const auto& payload : {std::string("not JSON"), std::string("{}"),
            std::string(R"({"kind":"system.initialization","data":{"schema_version":2}})"),
            std::string(R"({"kind":"system.initialization","data":{"schema_version":"1"}})"),
            std::string(R"({"kind":"system.initialization","data":{}})")}) {
        auto invalid = original;
        invalid.set_payload(payload);
        PutRawRecord(db_path, "messages", message_key, invalid.SerializeAsString());
        rejected();
    }
    auto invalid = original;
    invalid.set_principal(1);
    PutRawRecord(db_path, "messages", message_key, invalid.SerializeAsString());
    rejected();
    PutRawRecord(db_path, "messages", message_key, original.SerializeAsString());
    PutRawRecord(db_path, rocksdb::kDefaultColumnFamilyName, uuid_key, "", true);
    rejected();
    PutRawRecord(db_path, rocksdb::kDefaultColumnFamilyName, uuid_key, openevent::EncodeUint64(1));
    rejected();
    std::filesystem::remove_all(root);
}

}  // namespace

int main()
{
    TestPublishFetch();
    TestCasAbort();
    TestUuidAllocationAndDeduplication();
    TestPrivateAcl();
    TestFetchChannelFilter();
    TestSharedSubscriptionSnapshotRefresh();
    TestRecipientFilter();
    TestRecipientMustBeChannelMember();
    TestPayloadLimit();
    TestGuaranteedNonCommitStatuses();
    TestListMessagesPagination();
    TestFetchScanBudgetAdvancesCursor();
    TestResponseSoftBudgetPagination();
    TestPrincipalTokenCredentials();
    TestTokenManagementRejectsInvalidParameters();
    TestCorruptCredentialRecordIsFatal();
    TestDeleteTokenOrdersBeforePublish();
    TestObjectWriteMetadataReadAndReopen();
    TestBatchObjectMetadata();
    TestBatchObjectMetadataWhilePreparing();
    TestBatchObjectMetadataCorruptionIsFatal();
    TestMessageObjectReferences();
    TestObjectWriteInjectedFailures();
    TestRpcStorageFailureTriggersFatalShutdown();
    TestFatalStorageDiagnostics();
    TestObjectWriteCrashRecoveryBoundaries();
    TestConcurrentObjectWritesCanCommitOutOfIdOrder();
    TestObjectWriteReauthenticatesBeforeCommit();
    TestCancelledObjectWriteContinuesAfterPreparing();
    TestTimedOutPublishDoesNotCommitAfterCoordinatorWait();
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
    TestServerConfigPayloadLimit();
    TestLoadServerConfigRequiresExistingFile();
    TestLoadServerConfigReadsRequiredPaths();
    TestSystemChannelShape();
    TestChannelSystemMessages();
    TestChannelSystemMessageAclAndNoOps();
    TestChannelMemberLimits();
    TestChannelSystemMessageCrashRecovery();
    TestInitializationCommitBoundary();
    TestMessageCommitFailureAndRestart();
    TestSystemMessagePersistenceAndValidation();
    return 0;
}
