#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>
#include <grpcpp/grpcpp.h>
#include <rocksdb/options.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include "object_record.pb.h"
#include "server/server_config.h"
#include "service/grpc_services.h"
#include "service/open_event_core.h"
#include "storage/encoding.h"
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

constexpr auto kTestWait = std::chrono::seconds(10);

void WaitForFlag(std::condition_variable& cv, std::unique_lock<std::mutex>& lock,
                 const bool& flag, const char* failure)
{
    Check(cv.wait_for(lock, kTestWait, [&]() { return flag; }), failure);
}

template <typename T>
T Await(std::future<T>& operation, const char* failure)
{
    Check(operation.wait_for(kTestWait) == std::future_status::ready, failure);
    return operation.get();
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

void EditRawColumnFamily(const std::filesystem::path& path, const std::string& column_family,
                         const auto& edit)
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
    status = edit(*db, handles[index]);
    Check(status.ok(), status.ToString());

    for (auto* handle : handles) {
        status = db->DestroyColumnFamilyHandle(handle);
        Check(status.ok(), status.ToString());
    }
}

void PutRawRecord(const std::filesystem::path& path, const std::string& column_family,
                  const std::string& key, const std::string& value, bool erase = false)
{
    EditRawColumnFamily(path, column_family, [&](rocksdb::DB& db, rocksdb::ColumnFamilyHandle* handle) {
        return erase ? db.Delete(SyncWriteOptions(), handle, key)
                     : db.Put(SyncWriteOptions(), handle, key, value);
    });
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

uint64_t LastMessageSeq(openevent::OpenEventCore& core)
{
    openevent::ListMessagesRequest request;
    request.set_from_seq(std::numeric_limits<uint64_t>::max());
    request.set_limit(1);
    openevent::ListMessagesResponse response;
    const auto status = core.ListMessages(request, &response);
    Check(status.ok(), status.message());
    return response.last_seq();
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

void CheckObjectState(const openevent::UnifiedStorage& storage, const std::filesystem::path& root,
                      bool expected_committed)
{
    auto snapshot = storage.CreateSnapshot();
    Check(snapshot.ok(), snapshot.status().message());
    auto preparing = storage.GetPreparingObject(snapshot.value(), 1);
    auto committed = storage.GetCommittedObject(snapshot.value(), 1);
    auto next_id = storage.GetNextObjectId(snapshot.value());
    Check(preparing.ok() && !preparing.value().has_value(),
          "the object must have no PREPARING metadata");
    Check(committed.ok() && committed.value().has_value() == expected_committed,
          "committed state does not match the injected persistence point");
    Check(next_id.ok() && next_id.value() == 2, "the reserved object ID must not be reused");
    if (expected_committed) {
        auto data = storage.ReadObjectFile(
            committed.value().value(), 0, committed.value().value().nbytes());
        Check(data.ok() && data.value() == "fault-data",
              "committed object must remain intact after an injected failure or crash");
    } else {
        Check(!std::filesystem::exists(root / "data" / "objects" / "1"),
              "an uncommitted object file must have been removed");
    }
}

void CheckRecoveredObjectState(const std::filesystem::path& root, bool expected_committed)
{
    auto storage = openevent::UnifiedStorage::Open((root / "data").string());
    Check(storage.ok(), storage.status().message());
    CheckObjectState(*storage.value(), root, expected_committed);
}

void TestPublishLimitAndFetch()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_publish_fetch";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root, 4);
    const std::string token = AddToken(*core, 100);
    const uint64_t channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);
    openevent::PublishAutoSeqRequest auto_request;
    auto_request.set_principal(100);
    auto_request.set_token(token);
    auto_request.set_channel_id(channel_id);
    auto_request.set_payload("1234");
    auto_request.set_uuid(AllocateUuid(*core));
    openevent::PublishAutoSeqResponse auto_response;
    const uint64_t before_publish_ms = NowMs();
    openevent::Status status = core->PublishAutoSeq(auto_request, &auto_response);
    const uint64_t after_publish_ms = NowMs();
    Check(status.ok(), status.message());
    Check(auto_response.seq() == 2, "payload at max limit should publish");

    auto_request.set_payload("12345");
    status = core->PublishAutoSeq(auto_request, &auto_response);
    Check(status.code() == grpc::StatusCode::ALREADY_EXISTS,
          "PublishAutoSeq must check a consumed UUID before the payload limit");

    openevent::PublishRequest retry;
    retry.set_principal(100);
    retry.set_token(token);
    retry.set_channel_id(channel_id);
    retry.set_seq(2);
    retry.set_uuid(auto_request.uuid());
    retry.set_payload("12345");
    openevent::PublishResponse retry_response;
    status = core->Publish(retry, &retry_response);
    Check(status.code() == grpc::StatusCode::ALREADY_EXISTS,
          "Publish must check a consumed UUID before an oversized payload or stale seq");

    auto_request.set_uuid(AllocateUuid(*core));
    status = core->PublishAutoSeq(auto_request, &auto_response);
    Check(status.code() == grpc::StatusCode::RESOURCE_EXHAUSTED,
          "oversized PublishAutoSeq payload should return RESOURCE_EXHAUSTED");

    openevent::FetchRequest fetch;
    fetch.set_principal(100);
    fetch.set_token(token);
    fetch.set_from_seq(2);
    fetch.set_limit(1);

    openevent::FetchResponse response;
    status = core->Fetch(fetch, &response);
    Check(status.ok(), status.message());
    Check(response.messages_size() == 1 && response.messages(0).seq() == 2 &&
              response.messages(0).ts_ms() >= before_publish_ms &&
              response.messages(0).ts_ms() <= after_publish_ms,
          "business message timestamp must fall within the publish call");

    fetch.set_from_seq(100);
    status = core->Fetch(fetch, &response);
    Check(status.ok() && response.messages().empty() && response.last_seq() == 2 &&
              response.next_seq() == response.last_seq() + 1,
          "Fetch beyond the tail must return an empty page and the cursor after the tail");

    auto_request.set_payload("ok");
    auto_request.set_channel_id(channel_id + 1000);
    status = core->PublishAutoSeq(auto_request, &auto_response);
    Check(status.code() == grpc::StatusCode::NOT_FOUND,
          "missing channel should guarantee non-commit");

    openevent::DeleteTokenRequest revoke;
    revoke.set_principal(100);
    revoke.set_token(token);
    openevent::DeleteTokenResponse revoked;
    Check(core->DeleteToken(revoke, &revoked).ok(), "revoke the current owner credential");
    auto_request.set_channel_id(channel_id);
    status = core->PublishAutoSeq(auto_request, &auto_response);
    Check(status.code() == grpc::StatusCode::UNAUTHENTICATED,
          "publishing with a revoked token must be unauthenticated");
    Check(LastMessageSeq(*core) == 2, "rejected publications must not advance max_seq");

    core.reset();
    std::filesystem::remove_all(root);
}

void TestPublishRecipients()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_publish_recipients";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);
    const auto token = AddToken(*core, 100);
    openevent::CreateChannelRequest create;
    create.set_principal(100);
    create.set_token(token);
    create.set_name("recipients");
    create.set_visibility(openevent::VISIBILITY_PRIVATE);
    create.add_members(200);
    openevent::CreateChannelResponse created;
    Check(core->CreateChannel(create, &created).ok(), "create channel with two members");

    openevent::PublishRequest request;
    request.set_principal(100);
    request.set_token(token);
    request.set_channel_id(created.channel().channel_id());
    request.set_seq(2);
    request.set_uuid(AllocateUuid(*core));
    for (uint64_t recipient : {200, 200, 100, 200, 300}) {
        request.add_recipients(recipient);
    }
    openevent::PublishResponse published;
    Check(core->Publish(request, &published).code() == grpc::StatusCode::INVALID_ARGUMENT,
          "a nonmember after repeated valid recipients must still prevent publication");
    request.mutable_recipients()->RemoveLast();
    Check(core->Publish(request, &published).ok(), "Publish must accept repeated member IDs");

    openevent::FetchRequest fetch;
    fetch.set_principal(100);
    fetch.set_token(token);
    fetch.set_from_seq(0);
    fetch.set_limit(10);
    fetch.set_only_my_recipient(true);
    openevent::FetchResponse response;
    Check(core->Fetch(fetch, &response).ok() && response.messages_size() == 1,
          "recipient filter must return just the addressed message");
    const auto& message = response.messages(0);
    Check(message.recipients_size() == 2 && message.recipients(0) == 200 && message.recipients(1) == 100,
          "Publish must store distinct recipients in first-occurrence order");

    core.reset();
    std::filesystem::remove_all(root);
}

void TestUuidAllocationAndRestart()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_uuid_deduplication";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);
    const std::string token = AddToken(*core, 100);
    const uint64_t channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);

    openevent::AllocateUuidsRequest invalid_allocation;
    openevent::AllocateUuidsResponse allocated;
    openevent::Status status = core->AllocateUuids(invalid_allocation, &allocated);
    Check(status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "zero UUID allocation count must be rejected");
    invalid_allocation.set_count(1025);
    status = core->AllocateUuids(invalid_allocation, &allocated);
    Check(status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "oversized UUID allocation count must be rejected");

    openevent::AllocateUuidsRequest allocation;
    allocation.set_count(3);
    status = core->AllocateUuids(allocation, &allocated);
    Check(status.ok() && allocated.uuids_size() == 3 && allocated.uuids(0) == 2 &&
              allocated.uuids(1) == 3 && allocated.uuids(2) == 4,
          "UUID allocation must return a globally monotonic contiguous batch");

    openevent::GetSeqByUuidRequest get_seq;
    openevent::GetSeqByUuidResponse get_seq_response;
    get_seq.set_uuid(99);
    status = core->GetSeqByUuid(get_seq, &get_seq_response);
    Check(status.code() == grpc::StatusCode::NOT_FOUND,
          "UUID lookup must not report an unallocated UUID");

    openevent::PublishAutoSeqRequest publish;
    publish.set_principal(100);
    publish.set_token(token);
    publish.set_channel_id(channel_id);
    publish.set_payload("first");
    openevent::PublishAutoSeqResponse published;

    status = core->PublishAutoSeq(publish, &published);
    Check(status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "a missing UUID must be rejected");
    publish.set_uuid(99);
    status = core->PublishAutoSeq(publish, &published);
    Check(status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "a UUID not allocated by the server must be rejected");

    core.reset();
    core = MakeCore(root);

    publish.set_uuid(allocated.uuids(2));
    status = core->PublishAutoSeq(publish, &published);
    Check(status.ok() && published.seq() == 2,
          "the last UUID of an unused batch must remain usable after reopen");

    openevent::AllocateUuidsRequest continued_allocation;
    continued_allocation.set_count(2);
    openevent::AllocateUuidsResponse continued;
    status = core->AllocateUuids(continued_allocation, &continued);
    Check(status.ok() && continued.uuids_size() == 2 && continued.uuids(0) == 5 &&
              continued.uuids(1) == 6,
          "UUID allocator watermark must survive storage reopen");

    core.reset();
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

    openevent::FetchRequest missing = fetch;
    missing.set_from_seq(100);
    missing.add_channels(channel_id + 1);
    Check(core->Fetch(missing, &response).code() == grpc::StatusCode::NOT_FOUND,
          "a missing explicit channel must reject Fetch even beyond the tail");

    openevent::AddMemberRequest add;
    add.set_principal(100);
    add.set_token(owner_token);
    add.set_channel_id(channel_id);
    add.set_target_principal(200);
    openevent::AddMemberResponse add_response;
    status = core->AddMember(add, &add_response);
    Check(status.ok(), status.message());

    fetch.add_channels(channel_id);
    status = core->Fetch(fetch, &response);
    Check(status.ok(), status.message());
    Check(response.messages_size() == 3 && response.messages(0).principal() == 0 &&
              response.messages(1).payload() == "secret" && response.messages(2).principal() == 0,
          "member should see channel initialization, private message, and member addition");

    const auto owned = CreateChannel(*core, 200, other_token, openevent::VISIBILITY_PRIVATE);
    const auto visible = CreateChannel(*core, 100, owner_token, openevent::VISIBILITY_PUBLIC);
    const auto hidden = CreateChannel(*core, 100, owner_token, openevent::VISIBILITY_PRIVATE);
    openevent::ListChannelsRequest list;
    list.set_principal(200);
    list.set_token(other_token);
    openevent::ListChannelsResponse listed;
    auto expect_channels = [&](openevent::ChannelFilter filter, std::vector<uint64_t> expected) {
        list.set_filter(filter);
        auto result = core->ListChannels(list, &listed);
        Check(result.ok(), result.message());
        std::vector<uint64_t> ids;
        for (const auto& channel : listed.channels()) {
            ids.push_back(channel.channel_id());
        }
        std::sort(ids.begin(), ids.end());
        std::sort(expected.begin(), expected.end());
        Check(ids == expected, "channel list must contain exactly the matching channels");
    };
    expect_channels(openevent::CHANNEL_FILTER_ALL, {0, channel_id, owned, visible});
    expect_channels(openevent::CHANNEL_FILTER_JOINED, {channel_id, owned});
    expect_channels(openevent::CHANNEL_FILTER_OWNED, {owned});

    core.reset();
    PutRawRecord(root / "data" / "db", "default", "ch/" + openevent::EncodeUint64(hidden), "");
    core = MakeCore(root);
    status = core->ListChannels(list, &listed);
    Check(status.code() == grpc::StatusCode::DATA_LOSS,
          "an invalid record after a matching channel must fail the entire list");

    core.reset();
    std::filesystem::remove_all(root);
}

void TestFetchScanBudgetAdvancesCursor()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_fetch_scan_budget";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root, 1024, 1);

    std::string owner_token = AddToken(*core, 100);
    std::string outsider_token = AddToken(*core, 200);
    CreateChannel(*core, 100, owner_token, openevent::VISIBILITY_PRIVATE);
    const auto public_channel = CreateChannel(*core, 100, owner_token, openevent::VISIBILITY_PUBLIC);

    openevent::FetchRequest request;
    request.set_principal(200);
    request.set_token(outsider_token);
    request.set_from_seq(1);
    request.set_limit(10);
    openevent::FetchResponse response;
    openevent::Status status = core->Fetch(request, &response);
    Check(status.ok(), status.message());
    Check(response.messages_size() == 0, "scan-budget page may be empty when all records are hidden");
    Check(response.next_seq() == 2, "empty scan-budget page must advance past the private channel");
    Check(response.last_seq() == 2, "scan-budget page should keep the snapshot tail");

    request.set_from_seq(response.next_seq());
    status = core->Fetch(request, &response);
    Check(status.ok(), status.message());
    Check(response.messages_size() == 1 && response.messages(0).seq() == 2 &&
              response.messages(0).channel_id() == public_channel,
          "continuing an empty page must return the public channel's initialization message");
    Check(response.next_seq() == 3, "second scan-budget page should advance past the tail");

    core.reset();
    std::filesystem::remove_all(root);
}

void TestMessagePageBoundariesAndCorruption()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_response_budget";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);

    openevent::ListMessagesRequest list;
    openevent::ListMessagesResponse list_response;
    auto status = core->ListMessages(list, &list_response);
    Check(status.code() == grpc::StatusCode::INVALID_ARGUMENT, "zero ListMessages limit should be rejected");
    list.set_limit(1001);
    status = core->ListMessages(list, &list_response);
    Check(status.code() == grpc::StatusCode::INVALID_ARGUMENT, "oversized ListMessages limit should be rejected");
    list.set_from_seq(100);
    list.set_limit(1);
    status = core->ListMessages(list, &list_response);
    Check(status.ok() && list_response.messages().empty() && list_response.last_seq() == 0 &&
              list_response.next_seq() == 1,
          "ListMessages beyond the tail must return an empty page and the cursor after the tail");

    const auto token = AddToken(*core, 100);
    const auto channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);
    list.set_from_seq(1);
    status = core->ListMessages(list, &list_response);
    Check(status.ok() && list_response.messages_size() == 1 && list_response.messages(0).seq() == 1,
          "read the channel initialization message before corrupting its sequence");
    auto mismatched = list_response.messages(0);
    mismatched.set_seq(2);

    openevent::FetchRequest fetch;
    fetch.set_principal(100);
    fetch.set_token(token);
    fetch.set_from_seq(0);
    fetch.set_limit(10);
    openevent::FetchResponse fetch_response;

    // A malformed next record makes any read past a full page observable.
    core.reset();
    const auto db_path = root / "data" / "db";
    PutRawRecord(db_path, "messages", "msg/" + openevent::EncodeUint64(1), mismatched.SerializeAsString());
    core = MakeCore(root, 1024, 10000, 1);
    status = core->Fetch(fetch, &fetch_response);
    Check(status.ok() && fetch_response.messages_size() == 1 && fetch_response.messages(0).seq() == 0 &&
              fetch_response.next_seq() == 1,
          "Fetch must return the oversized first message without reading the next record");

    const size_t first_message_bytes = fetch_response.messages(0).ByteSizeLong();
    core.reset();
    int fatal_calls = 0;
    core = MakeCore(root, 1024, 10000, first_message_bytes, [&](const openevent::Status&) { ++fatal_calls; });
    list.set_from_seq(0);
    list.set_limit(10);
    status = core->ListMessages(list, &list_response);
    Check(status.ok() && list_response.messages_size() == 1 && list_response.messages(0).seq() == 0 &&
              list_response.next_seq() == 1,
          "ListMessages must stop before the next record when the byte budget is exactly full");
    {
        openevent::EventServiceImpl service(std::move(core));
        fetch.set_from_seq(list_response.next_seq());
        fetch.add_channels(channel_id);
        const auto rpc_status = service.Fetch(nullptr, &fetch, &fetch_response);
        Check(rpc_status.error_code() == grpc::StatusCode::DATA_LOSS && fatal_calls == 1,
              "a stored message sequence differing from its key must trigger fatal DATA_LOSS");
    }
    std::filesystem::remove_all(root);
}

void TestPrincipalTokenCredentials()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_principal_tokens";
    std::filesystem::remove_all(root);
    const std::string shared_token = "shared-token";
    const std::string other_token = "other-token";
    auto storage = openevent::UnifiedStorage::Open((root / "data").string());
    Check(storage.ok(), storage.status().message());
    {
        rocksdb::WriteBatch batch;
        Check(storage.value()->PutToken(&batch, 100, shared_token).ok(), "store first credential");
        Check(storage.value()->PutToken(&batch, 200, shared_token).ok(), "reuse token for another principal");
        Check(storage.value()->PutToken(&batch, 100, other_token).ok(), "store another token for same principal");
        Check(storage.value()->Commit(&batch).ok(), "commit credential set");
    }

    auto core = std::make_unique<openevent::OpenEventCore>(std::move(storage.value()), 1024);
    auto authenticate = [&](uint64_t principal, const std::string& token) {
        openevent::GetStatusRequest request;
        request.set_principal(principal);
        request.set_token(token);
        openevent::GetStatusResponse response;
        return core->GetStatus(request, &response);
    };
    Check(authenticate(100, shared_token).ok(), "stored credential must authenticate before deletion");

    openevent::DeleteTokenRequest remove;
    openevent::DeleteTokenResponse response;
    openevent::AddTokenRequest add;
    openevent::AddTokenResponse added;
    Check(core->AddToken(add, &added).code() == grpc::StatusCode::INVALID_ARGUMENT,
          "AddToken must reject principal zero");
    remove.set_token(shared_token);
    Check(core->DeleteToken(remove, &response).code() == grpc::StatusCode::INVALID_ARGUMENT,
          "DeleteToken must reject principal zero even with a nonempty token");
    remove.set_principal(100);
    remove.clear_token();
    Check(core->DeleteToken(remove, &response).code() == grpc::StatusCode::INVALID_ARGUMENT,
          "DeleteToken must reject an empty token even with a nonzero principal");

    remove.set_token(shared_token);
    Check(core->DeleteToken(remove, &response).ok(), "delete one principal/token pair");

    core.reset();
    core = MakeCore(root);
    Check(authenticate(100, shared_token).code() == grpc::StatusCode::UNAUTHENTICATED,
          "deleted pair must fail authentication after restart");
    Check(authenticate(200, shared_token).ok(),
          "deletion must preserve the same token belonging to another principal");
    Check(authenticate(100, other_token).ok(),
          "deletion must preserve another token belonging to the same principal");
    core.reset();
    std::filesystem::remove_all(root);
}

void TestObjectWriteLimitsAndReadAuthorization()
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
        Check(status.code() == expected_code,
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
    invalid = MakeWriteObjectRequest(100, "wrong-token", "data");
    expect_write_status(invalid, grpc::StatusCode::UNAUTHENTICATED);

    const std::string prefix("ab\0cdef", 7);
    auto largest = MakeWriteObjectRequest(100, token, prefix);
    largest.mutable_data()->resize(kMaxObjectBytes, 'm');
    openevent::WriteObjectResponse written;
    auto status = core->WriteObject(largest, &written);
    Check(status.ok(), status.message());

    openevent::DeleteTokenRequest delete_token;
    delete_token.set_principal(100);
    delete_token.set_token(token);
    openevent::DeleteTokenResponse delete_response;
    status = core->DeleteToken(delete_token, &delete_response);
    Check(status.ok(), status.message());
    openevent::ReadObjectRequest read;
    read.set_object_id(written.object_id());
    read.set_object_token(written.object_token());
    read.set_nbytes(prefix.size());
    openevent::ReadObjectResponse read_response;
    status = core->ReadObject(read, &read_response);
    Check(status.ok() && read_response.data() == prefix,
          "ObjectKey authorization must survive deletion of the creator token");

    core.reset();
    std::filesystem::remove_all(root);
}

void CheckBatchObjectMetadataErrors(openevent::OpenEventCore& core,
                                    const openevent::WriteObjectResponse& object)
{
    openevent::ObjectKey key;
    key.set_object_id(object.object_id());
    key.set_object_token(object.object_token());
    auto expect_failure = [&](const openevent::BatchGetObjectMetadataRequest& invalid,
                              grpc::StatusCode code) {
        openevent::BatchGetObjectMetadataResponse response;
        const auto status = core.BatchGetObjectMetadata(invalid, &response);
        Check(status.code() == code, "unexpected batch metadata error: " + status.message());
    };
    openevent::BatchGetObjectMetadataRequest empty;
    expect_failure(empty, grpc::StatusCode::INVALID_ARGUMENT);
    openevent::BatchGetObjectMetadataRequest invalid;
    *invalid.add_object_keys() = key;
    *invalid.add_object_keys() = key;
    // A repeated object still needs its own capability check.
    invalid.mutable_object_keys(1)->set_object_token("wrong-token");
    expect_failure(invalid, grpc::StatusCode::NOT_FOUND);
    invalid.mutable_object_keys(0)->set_object_id(object.object_id() + 1);
    *invalid.mutable_object_keys(1) = key;
    expect_failure(invalid, grpc::StatusCode::NOT_FOUND);
    // All key shapes must be checked before the earlier missing object is looked up.
    invalid.mutable_object_keys(1)->set_object_id(0);
    expect_failure(invalid, grpc::StatusCode::INVALID_ARGUMENT);
    *invalid.mutable_object_keys(1) = key;
    invalid.mutable_object_keys(1)->clear_object_token();
    expect_failure(invalid, grpc::StatusCode::INVALID_ARGUMENT);
}

void TestConcurrentObjectWritesAndMetadataVisibility()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_concurrent_object_writes";
    std::filesystem::remove_all(root);
    std::atomic<bool> first_file{true};
    std::promise<void> paused, resume;
    auto first_paused = paused.get_future();
    auto can_resume = resume.get_future();
    auto injector = [&](openevent::StorageFaultPoint point) {
        if (point == openevent::StorageFaultPoint::kAfterObjectFileCreate && first_file.exchange(false)) {
            paused.set_value();
            Await(can_resume, "first upload was not released after the visibility checks");
        }
        return openevent::Status::Ok();
    };
    auto core = MakeCore(root, 1024, 10000, 0, {}, injector);
    const std::string token = AddToken(*core, 100);
    auto first = std::async(std::launch::async, [&]() { return WriteObject(*core, 100, token, "first"); });
    Await(first_paused, "first upload did not reach file creation");
    auto second = std::async(std::launch::async, [&]() { return WriteObject(*core, 100, token, "second"); });
    const auto committed = Await(second, "second upload did not finish while the first was paused");
    openevent::BatchGetObjectMetadataRequest request;
    auto* key = request.add_object_keys();
    key->set_object_id(committed.object_id());
    key->set_object_token(committed.object_token());
    openevent::BatchGetObjectMetadataResponse response;
    Check(core->BatchGetObjectMetadata(request, &response).ok() && response.metadata_size() == 1 &&
              response.metadata(0).nbytes() == 6,
          "the completed upload must be visible while the first upload is still writing");
    resume.set_value();
    const auto written = Await(first, "first upload did not finish after file I/O resumed");
    Check(written.object_id() < committed.object_id(), "object IDs must follow reservation order");
    key = request.add_object_keys();
    key->set_object_id(written.object_id());
    key->set_object_token(written.object_token());
    Check(core->BatchGetObjectMetadata(request, &response).ok() && response.metadata_size() == 2 &&
              response.metadata(0).nbytes() == 6 && response.metadata(1).nbytes() == 5,
          "a new metadata batch must observe both objects after their final commits");
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
    CheckBatchObjectMetadataErrors(*core, first);
    const auto second = WriteObject(*core, 100, token, "second");

    openevent::PublishAutoSeqRequest publish;
    publish.set_principal(100);
    publish.set_token(token);
    publish.set_channel_id(channel_id);
    publish.set_payload("references");
    publish.set_uuid(AllocateUuid(*core));
    for (int i = 0; i < 1024; ++i) {
        const auto& object = i % 2 == 0 ? first : second;
        auto* key = publish.add_object_keys();
        key->set_object_id(object.object_id());
        key->set_object_token(object.object_token());
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
    Check(keys.size() == 1024, "a message at the reference limit must retain all 1024 keys");
    for (int i = 0; i < keys.size(); ++i) {
        const auto& expected = publish.object_keys(i);
        Check(keys.Get(i).object_id() == expected.object_id() &&
                  keys.Get(i).object_token() == expected.object_token(),
              "message ObjectKeys must retain duplicates and request order");
    }

    const uint64_t watermark = publish_response.seq();

    auto* too_many = publish.add_object_keys();
    too_many->set_object_id(first.object_id());
    too_many->set_object_token(first.object_token());
    publish.set_uuid(AllocateUuid(*core));
    status = core->PublishAutoSeq(publish, &publish_response);
    Check(status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "a message with 1025 ObjectKeys must be rejected");

    publish.clear_object_keys();
    auto* valid_key = publish.add_object_keys();
    valid_key->set_object_id(first.object_id());
    valid_key->set_object_token(first.object_token());
    auto* invalid_key = publish.add_object_keys();
    invalid_key->set_object_id(first.object_id());
    invalid_key->set_object_token("wrong-token");
    status = core->PublishAutoSeq(publish, &publish_response);
    Check(status.code() == grpc::StatusCode::NOT_FOUND,
          "duplicate object IDs must validate every supplied token");

    Check(LastMessageSeq(*core) == watermark,
          "rejected object references must not advance the message watermark");
    openevent::GetSeqByUuidRequest lookup;
    lookup.set_uuid(publish.uuid());
    openevent::GetSeqByUuidResponse mapped;
    Check(core->GetSeqByUuid(lookup, &mapped).code() == grpc::StatusCode::NOT_FOUND,
          "rejected object references must leave the UUID unused");

    core.reset();
    PutRawRecord(root / "data" / "db", "objects",
                 std::string("object/") + openevent::EncodeUint64(second.object_id()),
                 "invalid object metadata");
    int fatal_calls = 0;
    core = MakeCore(root, 1024, 10000, 0, [&](const openevent::Status&) { ++fatal_calls; });
    publish.clear_object_keys();
    auto* denied = publish.add_object_keys();
    denied->set_object_id(first.object_id());
    denied->set_object_token("wrong-token");
    auto* corrupt = publish.add_object_keys();
    corrupt->set_object_id(second.object_id());
    corrupt->set_object_token(second.object_token());
    status = core->PublishAutoSeq(publish, &publish_response);
    Check(status.code() == grpc::StatusCode::DATA_LOSS && fatal_calls == 1,
          "publishing must detect a corrupt referenced record before checking object tokens");
    core.reset();

    openevent::BatchGetObjectMetadataRequest batch;
    *batch.mutable_object_keys() = publish.object_keys();
    fatal_calls = 0;
    {
        openevent::ObjectStorageServiceImpl service(MakeCore(root, 1024, 10000, 0,
            [&](const openevent::Status&) { ++fatal_calls; }));
        openevent::BatchGetObjectMetadataResponse response;
        const auto rpc = service.BatchGetObjectMetadata(nullptr, &batch, &response);
        Check(rpc.error_code() == grpc::StatusCode::DATA_LOSS && fatal_calls == 1,
              "a bad token must not mask corrupt metadata later in the batch");
    }
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
        bool fatal;
    };
    const std::vector<FailureCase> cases{
        {"file-write", openevent::StorageFaultPoint::kAfterObjectFileCreate, 0,
         grpc::StatusCode::RESOURCE_EXHAUSTED, false, false},
        {"committed-before-write", openevent::StorageFaultPoint::kBeforeCommit, 1,
         grpc::StatusCode::UNAVAILABLE, false, true},
        {"committed-after-write", openevent::StorageFaultPoint::kAfterCommit, 1,
         grpc::StatusCode::UNAVAILABLE, true, true},
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
        auto storage = openevent::UnifiedStorage::Open((root / "data").string(), injector);
        Check(storage.ok(), storage.status().message());
        auto* live_storage = storage.value().get();
        auto core = std::make_unique<openevent::OpenEventCore>(
            std::move(storage.value()), 1024, 10000, 0,
            [&](const openevent::Status&) { ++fatal_calls; });
        const std::string token = AddToken(*core, 100);
        state->skip_matches = test_case.skip_matches;
        state->active = true;

        openevent::WriteObjectRequest request = MakeWriteObjectRequest(100, token, "fault-data");
        openevent::WriteObjectResponse response;
        openevent::Status status = core->WriteObject(request, &response);
        Check(status.code() == test_case.code,
              std::string("unexpected injected failure result for ") + test_case.name);
        Check(fatal_calls == (test_case.fatal ? 1 : 0),
              std::string("unexpected fatal shutdown result for ") + test_case.name);
        if (test_case.fatal) {
            core.reset();
            CheckRecoveredObjectState(root, test_case.committed);
        } else {
            CheckObjectState(*live_storage, root, false);
            core.reset();
        }
        std::filesystem::remove_all(root);
    }
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
    auto check_diagnostics = [](const openevent::Status& status, const char* operation,
                                const std::string& category) {
        const auto rpc = status.ToGrpc();
        Check(status.requires_server_exit() && rpc.error_code() == status.code(),
              "fatal storage errors must preserve their RPC status code");
        for (const auto& text : {status.FatalLogMessage(), status.message(), rpc.error_message()}) {
            Check(text.find(operation) != std::string::npos && text.find(category) != std::string::npos,
                  "storage diagnostics must identify the operation and underlying error category");
            Check(text.find("secret") == std::string::npos && text.find('\n') == std::string::npos,
                  "storage diagnostics must discard sensitive text and embedded newlines");
        }
    };
    check_diagnostics(corruption, "read credential", rocksdb::Status::Corruption().ToString());
    check_diagnostics(io_error, "commit storage batch", rocksdb::Status::IOError().ToString());
    const auto error = openevent::UnifiedStorage::MostSevereReadError({io_error, corruption, io_error});
    Check(error.code() == grpc::StatusCode::DATA_LOSS,
          "corruption must take priority over an I/O error");
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
        {"after-file-write", openevent::StorageFaultPoint::kAfterObjectFileWrite, 0, false},
        {"after-committed", openevent::StorageFaultPoint::kAfterCommit, 1, true},
    };
    constexpr int kInjectedCrashExitCode = 73;

    for (const auto& test_case : cases) {
        const auto root = std::filesystem::temp_directory_path() /
                          (std::string("openevent_object_crash_") + test_case.name);
        std::filesystem::remove_all(root);
        const auto nested = root / "missing" / "parent";
        const pid_t child = ::fork();
        Check(child >= 0, "fork object crash test process");
        if (child == 0) {
            ::alarm(kTestWait.count());
            int skip_matches = test_case.skip_matches;
            bool writing_object = false;
            auto injector = [&](openevent::StorageFaultPoint point) {
                if (!writing_object || point != test_case.point) {
                    return openevent::Status::Ok();
                }
                if (skip_matches > 0) {
                    --skip_matches;
                    return openevent::Status::Ok();
                }
                ::_exit(kInjectedCrashExitCode);
            };
            auto core = MakeCore(nested, 1024, 10000, 0, {}, injector);
            const auto token = AddToken(*core, 100);
            openevent::WriteObjectRequest request = MakeWriteObjectRequest(100, token, "fault-data");
            openevent::WriteObjectResponse response;
            writing_object = true;
            core->WriteObject(request, &response);
            ::_exit(74);
        }

        int child_status = 0;
        Check(::waitpid(child, &child_status, 0) == child, "wait for injected object crash");
        Check(WIFEXITED(child_status) && WEXITSTATUS(child_status) == kInjectedCrashExitCode,
              std::string("child did not exit at injected point: ") + test_case.name);
        CheckRecoveredObjectState(nested, test_case.committed);
        std::filesystem::remove_all(root);
    }
}

void TestObjectWriteReauthenticatesBeforeCommit()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_object_final_auth";
    std::filesystem::remove_all(root);
    struct State {
        std::mutex mutex;
        std::condition_variable cv;
        bool blocked = false;
        bool release = false;
    };
    auto state = std::make_shared<State>();
    auto injector = [state](openevent::StorageFaultPoint point) {
        if (point != openevent::StorageFaultPoint::kAfterObjectFileCreate) {
            return openevent::Status::Ok();
        }
        std::unique_lock<std::mutex> lock(state->mutex);
        state->blocked = true;
        state->cv.notify_all();
        WaitForFlag(state->cv, lock, state->release, "token deletion did not finish while upload was paused");
        return openevent::Status::Ok();
    };
    auto storage = openevent::UnifiedStorage::Open((root / "data").string(), injector);
    Check(storage.ok(), storage.status().message());
    auto* live_storage = storage.value().get();
    auto core = std::make_unique<openevent::OpenEventCore>(std::move(storage.value()), 1024);
    const std::string token = AddToken(*core, 100);

    auto writer = std::async(std::launch::async, [&]() {
        auto request = MakeWriteObjectRequest(100, token, "fault-data");
        openevent::WriteObjectResponse response;
        return core->WriteObject(request, &response);
    });
    {
        std::unique_lock<std::mutex> lock(state->mutex);
        WaitForFlag(state->cv, lock, state->blocked, "upload did not reach file I/O before token deletion");
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
    const auto write_status = Await(writer, "upload did not finish after deleting its token");
    Check(write_status.code() == grpc::StatusCode::UNAUTHENTICATED,
          "object write must fail final authentication after token deletion");
    CheckObjectState(*live_storage, root, false);
    core.reset();
    std::filesystem::remove_all(root);
}

void TestCancelledObjectWriteContinuesAfterPreparing()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_object_cancel_after_preparing";
    std::filesystem::remove_all(root);
    std::atomic<bool> pause_after_commit{false};
    std::promise<void> preparing;
    std::promise<void> resume;
    auto prepared = preparing.get_future();
    auto release = resume.get_future();
    auto injector = [&](openevent::StorageFaultPoint point) {
        if (point == openevent::StorageFaultPoint::kAfterCommit && pause_after_commit.exchange(false)) {
            preparing.set_value();
            Await(release, "client did not cancel the PREPARING upload");
        }
        return openevent::Status::Ok();
    };
    auto storage = openevent::UnifiedStorage::Open((root / "data").string(), injector);
    Check(storage.ok(), storage.status().message());
    auto* live_storage = storage.value().get();
    auto core = std::make_shared<openevent::OpenEventCore>(std::move(storage.value()), 1024);
    const std::string token = AddToken(*core, 100);
    pause_after_commit = true;

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
        auto client = std::async(std::launch::async, [&]() {
            auto request = MakeWriteObjectRequest(100, token, "fault-data");
            openevent::WriteObjectResponse response;
            return stub->WriteObject(&context, request, &response);
        });
        Await(prepared, "upload did not commit PREPARING before cancel");
        context.TryCancel();
        const auto rpc_status = Await(client, "cancelled upload RPC did not return");
        Check(rpc_status.error_code() == grpc::StatusCode::CANCELLED,
              "cancelled object RPC must report CANCELLED to the client");
        resume.set_value();
        auto shutdown = std::async(std::launch::async, [&] { server->Shutdown(); });
        Await(shutdown, "cancelled upload handler did not finish");
    }
    CheckObjectState(*live_storage, root, true);
    core.reset();
    std::filesystem::remove_all(root);
}

void TestPublishDeadlineDuringBlockedCommit()
{
    const auto root =
        std::filesystem::temp_directory_path() / "openevent_publish_deadline_during_blocked_commit";
    std::filesystem::remove_all(root);
    struct State {
        std::mutex mutex;
        std::condition_variable cv;
        bool active = false;
        bool first_commit_blocked = false;
        bool release_first = false;
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
        state->active = false;
        state->first_commit_blocked = true;
        state->cv.notify_all();
        WaitForFlag(state->cv, lock, state->release_first, "second publish did not return at its deadline");
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
    auto first = std::async(std::launch::async, [&]() {
        return core->PublishAutoSeq(first_request, &first_response);
    });
    {
        std::unique_lock<std::mutex> lock(state->mutex);
        WaitForFlag(state->cv, lock, state->first_commit_blocked, "first publish did not acquire the coordinator");
    }

    {
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
              "the second publish must reach its deadline while the first commit is blocked");

        {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->release_first = true;
        }
        state->cv.notify_all();
        const auto first_status = Await(first, "first publish did not finish after the timed-out publish");
        Check(first_status.ok() && first_response.seq() == 2,
              "the publish holding the coordinator must commit first");

        server->Shutdown();
    }
    Check(LastMessageSeq(*core) == 2,
          "only the successful publish must advance the message watermark");

    core.reset();
    std::filesystem::remove_all(root);
}

void TestStorageLayoutAndPreparingRecovery()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_object_recovery";
    const auto storage_path = root / "data";
    const auto objects_path = storage_path / "objects";
    std::filesystem::remove_all(root);
    {
        auto storage = openevent::UnifiedStorage::Open(storage_path.string());
        Check(storage.ok(), storage.status().message());
        openevent::storage::internal::ObjectRecord object;
        object.set_object_id(1);
        object.set_object_token(std::string(43, 'A'));
        object.set_creator_principal(100);
        object.set_name("preparing");
        object.set_type("application/octet-stream");
        object.set_nbytes(4);

        rocksdb::WriteBatch batch;
        openevent::Status status = storage.value()->PutPreparingObject(&batch, object);
        Check(status.ok(), status.message());
        status = storage.value()->SetNextObjectId(&batch, 3);
        Check(status.ok(), status.message());
        status = storage.value()->Commit(&batch);
        Check(status.ok(), status.message());
        std::ofstream partial(objects_path / "1", std::ios::binary);
        partial << "da";
        Check(partial.good(), "create partially written ID file for recovery");
    }

    const auto second_key = std::string("preparing/") + openevent::EncodeUint64(2);
    openevent::storage::internal::ObjectRecord incomplete;
    incomplete.set_object_id(2);
    PutRawRecord(storage_path / "db", "objects", second_key, incomplete.SerializeAsString());
    auto rejected = openevent::UnifiedStorage::Open(storage_path.string());
    Check(rejected.status().code() == grpc::StatusCode::DATA_LOSS,
          "startup recovery must validate PREPARING metadata even when the object ID matches");
    Check(std::filesystem::exists(objects_path / "1"),
          "invalid PREPARING metadata must prevent cleanup of earlier objects");
    std::filesystem::remove_all(root);
}

void TestObjectWritePreservesExistingFileOnCollision()
{
    const auto root =
        std::filesystem::temp_directory_path() / "openevent_object_io_error_paths";
    std::filesystem::remove_all(root);
    int fatal_calls = 0;
    {
        auto core = MakeCore(root, 1024, 10000, 0, [&](const openevent::Status&) { ++fatal_calls; });
        const std::string token = AddToken(*core, 100);
        const auto collision_path = root / "data" / "objects" / "1";
        {
            std::ofstream file(collision_path, std::ios::binary);
            file << "collision";
            Check(file.good(), "create colliding ID file");
        }
        openevent::WriteObjectResponse response;
        auto status = core->WriteObject(MakeWriteObjectRequest(100, token, "data"), &response);
        Check(status.code() == grpc::StatusCode::DATA_LOSS && fatal_calls == 1,
              "an existing ID file must trigger fatal storage corruption");
        std::ifstream file(collision_path, std::ios::binary);
        const std::string contents{std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
        Check(contents == "collision", "an upload must leave the existing file intact on collision");
    }

    std::filesystem::remove_all(root);
}

void TestReadObjectRejectsInvalidFiles()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_invalid_object_files";
    std::filesystem::remove_all(root);
    std::filesystem::create_directory(root);
    auto files = openevent::ObjectFileStore::Open(root.string());
    Check(files.ok(), files.status().message());

    Check(::mkfifo((root / "1").c_str(), 0600) == 0, "create object FIFO");
    auto read_fifo = std::async(std::launch::async, [&] { return files.value()->ReadRange(1, 8, 0, 1); });
    Check(Await(read_fifo, "reading a FIFO blocked waiting for a writer").status().code() ==
              grpc::StatusCode::DATA_LOSS,
          "a committed object replaced by a FIFO must report DATA_LOSS");

    const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    Check(fd >= 0, "create Unix socket");
    // Bind a relative name because the absolute build path may exceed sun_path.
    const auto cwd = std::filesystem::current_path();
    std::filesystem::current_path(root);
    sockaddr_un endpoint{AF_UNIX, "2"};
    Check(::bind(fd, reinterpret_cast<const sockaddr*>(&endpoint), sizeof(endpoint)) == 0,
          "bind socket in place of object file");
    std::filesystem::current_path(cwd);
    ::close(fd);
    Check(files.value()->ReadRange(2, 8, 0, 1).status().code() == grpc::StatusCode::DATA_LOSS,
          "a committed object replaced by a socket must report DATA_LOSS");

    {
        std::ofstream truncated(root / "3", std::ios::binary);
        truncated.put('x');
        truncated.close();
        Check(truncated.good(), "create one-byte object file");
    }
    Check(files.value()->ReadRange(3, 8, 0, 1).status().code() == grpc::StatusCode::DATA_LOSS,
          "reading one available byte must detect a file shorter than its committed eight bytes");
    std::filesystem::remove_all(root);
}

void TestUnifiedStorageRejectsIncompleteInitialization()
{
    {
        const auto root = std::filesystem::temp_directory_path() / "openevent_missing_objects_cf";
        std::filesystem::remove_all(root);
        {
            auto storage = openevent::UnifiedStorage::Open(root.string());
            Check(storage.ok(), storage.status().message());
        }
        EditRawColumnFamily(root / "db", "objects", [](rocksdb::DB& db, rocksdb::ColumnFamilyHandle* handle) {
            return db.DropColumnFamily(handle);
        });

        auto storage = openevent::UnifiedStorage::Open(root.string());
        Check(!storage.ok(), "startup must reject a database missing the objects column family");
        std::filesystem::remove_all(root);
    }

    const auto root = std::filesystem::temp_directory_path() / "openevent_invalid_initial_metadata";
    std::filesystem::remove_all(root);
    {
        auto storage = openevent::UnifiedStorage::Open(root.string());
        Check(storage.ok(), storage.status().message());
    }
    const auto db_path = root / "db";
    PutRawRecord(db_path, rocksdb::kDefaultColumnFamilyName, "meta:max_seq", "", true);
    auto missing = openevent::UnifiedStorage::Open(root.string());
    Check(missing.status().code() == grpc::StatusCode::DATA_LOSS,
          "missing required metadata must reject startup without repair");
    std::filesystem::remove_all(root);
}

void TestServerConfigValidation()
{
    openevent::ServerConfig config;
    openevent::Status status = openevent::ValidateServerConfig(config);
    Check(status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "metadata path should be required");

    config.storage_path = "/tmp/openevent-data";
    status = openevent::ValidateServerConfig(config);
    Check(status.ok(), status.message());

    for (const char* endpoint : {"localhost:+0", "dns:///127.0.0.1%3A%30", "127.0.0.1:65536",
                                 "localhost", "127.0.0.1:0,127.0.0.1:9527"}) {
        auto invalid = config;
        invalid.grpc_listen_addr = endpoint;
        status = openevent::ValidateServerConfig(invalid);
        Check(status.code() == grpc::StatusCode::INVALID_ARGUMENT &&
                  status.message().find("grpc.listen_addr") != std::string::npos,
              std::string("invalid listening port must be rejected: ") + endpoint);
    }
    auto payload = config;
    payload.max_payload_bytes = 0;
    status = openevent::ValidateServerConfig(payload);
    Check(status.code() == grpc::StatusCode::INVALID_ARGUMENT &&
              status.message().find("limits.max_payload_bytes") != std::string::npos,
          "zero payload limit must identify the invalid setting");
    payload.max_payload_bytes = 60 * 1024 * 1024 + 1;
    status = openevent::ValidateServerConfig(payload);
    Check(status.code() == grpc::StatusCode::INVALID_ARGUMENT &&
              status.message().find("limits.max_payload_bytes") != std::string::npos,
          "oversized payload limit must identify the invalid setting");

    config.admin_listen_addr = "[::1]:000";
    status = openevent::ValidateServerConfig(config);
    Check(status.code() == grpc::StatusCode::INVALID_ARGUMENT &&
              status.message().find("admin.listen_addr") != std::string::npos,
          "a zero admin port must identify the invalid admin setting");
    config.admin_listen_addr = config.grpc_listen_addr;
    status = openevent::ValidateServerConfig(config);
    Check(status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "public and admin listen addresses must be different");
}

void TestLoadServerConfig()
{
    const auto path = std::filesystem::temp_directory_path() / "openevent_test_config.yaml";
    {
        std::ofstream yaml(path);
        yaml << "storage:\n  path: data\nlimits:\n  max_payload_bytes: -1\n";
        Check(yaml.good(), "write config with a negative payload limit");
    }
    auto config = openevent::LoadServerConfig(path.string());
    Check(config.status().code() == grpc::StatusCode::INVALID_ARGUMENT,
          "a negative payload limit in YAML must be rejected");
    std::filesystem::remove(path);
}

void TestChannelSystemMessages()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_channel_system_messages";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);
    const uint64_t owner = 100;
    const uint64_t member = 300;
    const auto token = AddToken(*core, owner);
    const auto before_uuid = AllocateUuid(*core);
    Check(before_uuid == 1, "first client UUID allocation");

    openevent::CreateChannelRequest create;
    create.set_principal(owner);
    create.set_token(token);
    create.set_name("channel-events");
    create.set_visibility(openevent::VISIBILITY_PRIVATE);
    create.add_members(member);
    openevent::CreateChannelResponse created;
    Check(core->CreateChannel(create, &created).ok(), "create channel for system events");
    const uint64_t channel = created.channel().channel_id();
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

    Check(core->RemoveMember(remove, &removed).ok(), "removing an absent member is an idempotent success");
    remove.set_target_principal(owner);
    Check(core->RemoveMember(remove, &removed).code() == grpc::StatusCode::PERMISSION_DENIED,
          "removing the creator is rejected");
    add.set_target_principal(0);
    Check(core->AddMember(add, &added).code() == grpc::StatusCode::INVALID_ARGUMENT,
          "invalid member addition is rejected");
    create.clear_name();
    Check(core->CreateChannel(create, &created).code() == grpc::StatusCode::INVALID_ARGUMENT,
          "invalid channel creation is rejected");

    openevent::FetchRequest fetch;
    fetch.set_principal(owner);
    fetch.set_token(token);
    fetch.set_from_seq(1);
    fetch.set_limit(100);
    openevent::FetchResponse fetched;
    Check(core->Fetch(fetch, &fetched).ok() && fetched.messages_size() == 4 && fetched.last_seq() == 4,
          "only creation, business publication, and the two membership changes advance the message tail");
    const auto& initialized = fetched.messages(0);
    Check(initialized.seq() == 1 && initialized.uuid() == before_uuid + 1,
          "channel initialization has the next global seq and UUID");
    const auto& business = fetched.messages(1);
    Check(business.principal() == owner && business.payload() == "chat" &&
              business.uuid() == initialized.uuid() + 1,
          "the first business message must follow the initialization event and consume the next UUID");
    Check(fetched.messages(2).uuid() == 4 && fetched.messages(3).uuid() == 5 &&
              AllocateUuid(*core) == 6,
          "client allocations, business messages, and member events share one UUID space");

    core.reset();
    std::filesystem::remove_all(root);
}

void TestChannelMemberLimits()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_channel_member_limits";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);
    const auto token = AddToken(*core, 100);
    openevent::CreateChannelRequest create;
    create.set_principal(100);
    create.set_token(token);
    create.set_name("bounded-members");
    create.set_visibility(openevent::VISIBILITY_PRIVATE);
    for (uint64_t member = 2000; member < 3024; ++member) create.add_members(member);
    openevent::CreateChannelResponse created;

    Check(core->CreateChannel(create, &created).code() == grpc::StatusCode::RESOURCE_EXHAUSTED,
          "1024 distinct requested members plus an absent creator exceed the total member limit");
    create.clear_members();
    for (int index = 0; index < 1025; ++index) create.add_members(100);
    Check(core->CreateChannel(create, &created).code() == grpc::StatusCode::INVALID_ARGUMENT,
          "the 1024-item request shape limit applies before deduplicating members");

    create.clear_members();
    std::vector<uint64_t> expected_members{100};
    for (uint64_t member = 2000; member < 3023; ++member) {
        create.add_members(member);
        expected_members.push_back(member);
    }
    auto sorted_members = [](const openevent::ChannelInfo& channel) {
        std::vector<uint64_t> members(channel.members().begin(), channel.members().end());
        std::sort(members.begin(), members.end());
        return members;
    };
    create.add_members(2000);
    Check(core->CreateChannel(create, &created).ok() &&
              created.channel().channel_id() == 1 && sorted_members(created.channel()) == expected_members,
          "1024 inputs with one duplicate leave room for the creator in the first channel");

    create.mutable_members()->Set(create.members_size() - 1, 100);
    Check(core->CreateChannel(create, &created).ok() &&
              sorted_members(created.channel()) == expected_members,
          "1024 requested members including the creator fit without counting the creator twice");

    const auto channel_id = created.channel().channel_id();
    const auto full_channel = created.channel().SerializeAsString();

    openevent::AddMemberRequest add;
    add.set_principal(100);
    add.set_token(token);
    add.set_channel_id(channel_id);
    openevent::AddMemberResponse added;

    openevent::GetChannelRequest get;
    get.set_principal(100);
    get.set_token(token);
    get.set_channel_id(channel_id);
    openevent::GetChannelResponse channel;

    add.set_target_principal(4001);
    Check(core->AddMember(add, &added).code() == grpc::StatusCode::RESOURCE_EXHAUSTED,
          "adding the 1025th total member is rejected");
    add.set_target_principal(2000);
    Check(core->AddMember(add, &added).code() == grpc::StatusCode::ALREADY_EXISTS,
          "an existing member takes precedence over the full-channel error");
    Check(LastMessageSeq(*core) == 2 &&
              core->GetChannel(get, &channel).ok() && channel.channel().SerializeAsString() == full_channel,
          "rejected additions preserve channel state and the message watermark");
    Check(AllocateUuid(*core) == 3, "only the two creations consumed system UUIDs");

    openevent::RemoveMemberRequest remove;
    remove.set_principal(100);
    remove.set_token(token);
    remove.set_channel_id(channel_id);
    remove.set_target_principal(2000);
    openevent::RemoveMemberResponse removed;
    Check(core->RemoveMember(remove, &removed).ok(), "removing a member frees a slot");
    add.set_target_principal(4001);
    Check(core->AddMember(add, &added).ok(), "a previously rejected member can use the freed slot");
    std::erase(expected_members, remove.target_principal());
    expected_members.push_back(add.target_principal());
    Check(core->GetChannel(get, &channel).ok() && sorted_members(channel.channel()) == expected_members,
          "the replacement must change only the removed and added members");
    core.reset();
    std::filesystem::remove_all(root);
}

void TestChannelCreationCrashRecovery()
{
    using Point = openevent::StorageFaultPoint;
    const auto root = std::filesystem::temp_directory_path() / "openevent_channel_creation_crash";
    std::filesystem::remove_all(root);
    const pid_t child = ::fork();
    Check(child >= 0, "fork channel creation crash test");
    if (child == 0) {
        ::alarm(kTestWait.count());
        bool creating_channel = false;
        auto core = MakeCore(root, 1024, 10000, 0, {}, [&](Point observed) {
            if (creating_channel && observed == Point::kAfterCommit) ::_exit(73);
            return openevent::Status::Ok();
        });
        const auto token = AddToken(*core, 100);
        creating_channel = true;
        CreateChannel(*core, 100, token, openevent::VISIBILITY_PRIVATE);
        ::_exit(74);
    }
    int result = 0;
    Check(::waitpid(child, &result, 0) == child && WIFEXITED(result) && WEXITSTATUS(result) == 73,
          "channel creation must crash just after its commit");
    {
        auto storage = openevent::UnifiedStorage::Open((root / "data").string());
        Check(storage.ok(), storage.status().message());
        auto snapshot = storage.value()->CreateSnapshot();
        Check(snapshot.ok(), snapshot.status().message());
        const auto max_seq = storage.value()->GetMaxSeq(snapshot.value());
        const auto next_uuid = storage.value()->GetNextUuid(snapshot.value());
        const auto used_uuid = storage.value()->GetUsedUuidSeq(snapshot.value(), 1);
        const auto next_channel = storage.value()->GetNextChannelId(snapshot.value());
        const auto channel = storage.value()->GetChannel(snapshot.value(), 1);
        Check(max_seq.ok() && max_seq.value() == 1 && next_uuid.ok() && next_uuid.value() == 2,
              "message and UUID watermarks must recover with the initialization event");
        Check(used_uuid.ok() && used_uuid.value() == 1,
              "the initialization event and its UUID index must recover together");
        Check(next_channel.ok() && next_channel.value() == 2 && channel.ok() && channel.value().has_value(),
              "the channel and its ID watermark must recover with the initialization event");
        openevent::EventMessage event;
        auto scan = storage.value()->ScanMessages(snapshot.value(), 1, 1, 1,
            [&](openevent::EventMessage&& message) -> openevent::Result<openevent::MessageScanAction> {
                event = std::move(message);
                return openevent::MessageScanAction::kContinue;
            });
        Check(scan.ok() && event.seq() == 1 && event.uuid() == 1 && event.channel_id() == 1 &&
                  event.principal() == 0,
              "the recovered system event must belong to the created channel and carry its reserved UUID");
        google::protobuf::Struct payload;
        Check(google::protobuf::util::JsonStringToMessage(event.payload(), &payload).ok() &&
                  payload.fields().contains("kind") &&
                  payload.fields().at("kind").string_value() == "system.channel_initialized",
              "the recovered event must identify the channel initialization");
    }
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
            ::alarm(kTestWait.count());
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
            auto storage = openevent::UnifiedStorage::Open(root.string());
            Check(storage.ok() == (point == Point::kAfterInitializationCommit),
                  "only the synchronous initial-state batch completes initialization");
            if (storage.ok()) {
                auto snapshot = storage.value()->CreateSnapshot();
                Check(snapshot.ok(), snapshot.status().message());
                auto max_seq = storage.value()->GetMaxSeq(snapshot.value());
                Check(max_seq.ok() && max_seq.value() == 0,
                      "committed initialization must preserve its message watermark");
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
        std::shared_ptr<openevent::OpenEventCore> core = MakeCore(root, 1024, 10000, 0,
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
        {
            openevent::EventServiceImpl event_service(core);
            const auto status = event_service.PublishAutoSeq(nullptr, &request, &response);
            Check(status.error_code() == grpc::StatusCode::UNAVAILABLE && fatal_calls == 1,
                  "every DB write error must make the core unavailable");
        }
        if (point == Point::kBeforeCommit) {
            active = false;
            openevent::ChannelServiceImpl channel_service(core);
            openevent::CreateChannelRequest after_failure;
            after_failure.set_principal(100);
            after_failure.set_token(token);
            after_failure.set_name("after-failure");
            after_failure.set_visibility(openevent::VISIBILITY_PUBLIC);
            openevent::CreateChannelResponse channel_response;
            const auto status = channel_service.CreateChannel(nullptr, &after_failure, &channel_response);
            Check(status.error_code() == grpc::StatusCode::UNAVAILABLE && fatal_calls == 1,
                  "subsequent writes must be rejected without triggering another shutdown");
        }
        core.reset();
        core = MakeCore(root);
        openevent::ListMessagesRequest list;
        list.set_from_seq(2);
        list.set_limit(1);
        openevent::ListMessagesResponse restored;
        const auto read = core->ListMessages(list, &restored);
        Check(read.ok(), read.message());
        const bool committed = point == Point::kAfterCommit;
        if (committed) {
            Check(restored.last_seq() == 2 && restored.messages_size() == 1 &&
                      restored.messages(0).seq() == 2 && restored.messages(0).uuid() == uuid &&
                      restored.messages(0).payload() == request.payload(),
                  "the committed message and its watermark must survive restart");
        } else {
            Check(restored.last_seq() == 1 && restored.messages().empty(),
                  "a failure before commit must leave the message absent and the watermark unchanged");
        }
        const auto status = core->PublishAutoSeq(request, &response);
        Check(committed ? status.code() == grpc::StatusCode::ALREADY_EXISTS : status.ok(),
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

void TestSystemMessageValidation()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_system_message";
    std::filesystem::remove_all(root);
    auto core = MakeCore(root);
    openevent::ListMessagesRequest list;
    list.set_limit(1);
    openevent::ListMessagesResponse listed;
    Check(core->ListMessages(list, &listed).ok() && listed.messages_size() == 1,
          "read the original initialization message");
    const auto original = listed.messages(0);
    core.reset();

    const auto db_path = root / "data" / "db";
    const auto message_key = std::string("msg/") + openevent::EncodeUint64(0);
    const auto uuid_key = std::string("uuid/") + openevent::EncodeUint64(0);
    auto rejected = [&]() {
        auto storage = openevent::UnifiedStorage::Open((root / "data").string());
        Check(storage.status().code() == grpc::StatusCode::DATA_LOSS,
              "invalid system state must reject startup without repairing it");
    };
    PutRawRecord(db_path, "messages", message_key, "", true);
    rejected();
    auto invalid = original;
    invalid.set_payload(R"({"kind":"system.initialization","data":{"schema_version":2}})");
    PutRawRecord(db_path, "messages", message_key, invalid.SerializeAsString());
    rejected();
    PutRawRecord(db_path, "messages", message_key, original.SerializeAsString());
    PutRawRecord(db_path, rocksdb::kDefaultColumnFamilyName, uuid_key, openevent::EncodeUint64(1));
    rejected();
    std::filesystem::remove_all(root);
}

}  // namespace

int main()
{
    TestPublishLimitAndFetch();
    TestPublishRecipients();
    TestUuidAllocationAndRestart();
    TestPrivateAcl();
    TestFetchScanBudgetAdvancesCursor();
    TestMessagePageBoundariesAndCorruption();
    TestPrincipalTokenCredentials();
    TestObjectWriteLimitsAndReadAuthorization();
    TestConcurrentObjectWritesAndMetadataVisibility();
    TestMessageObjectReferences();
    TestObjectWriteInjectedFailures();
    TestFatalStorageDiagnostics();
    TestObjectWriteCrashRecoveryBoundaries();
    TestObjectWriteReauthenticatesBeforeCommit();
    TestCancelledObjectWriteContinuesAfterPreparing();
    TestPublishDeadlineDuringBlockedCommit();
    TestStorageLayoutAndPreparingRecovery();
    TestObjectWritePreservesExistingFileOnCollision();
    TestReadObjectRejectsInvalidFiles();
    TestUnifiedStorageRejectsIncompleteInitialization();
    TestServerConfigValidation();
    TestLoadServerConfig();
    TestChannelSystemMessages();
    TestChannelMemberLimits();
    TestChannelCreationCrashRecovery();
    TestInitializationCommitBoundary();
    TestMessageCommitFailureAndRestart();
    TestSystemMessageValidation();
    return 0;
}
