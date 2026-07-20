#include <algorithm>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <rocksdb/options.h>

#include "server/server_config.h"
#include "service/open_event_core.h"
#include "storage/encoding.h"
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
                                                   size_t response_soft_limit_bytes = 0)
{
    auto storage = openevent::UnifiedStorage::Open((root / "data").string());
    Check(storage.ok(), storage.status().message());
    return std::make_unique<openevent::OpenEventCore>(
        std::move(storage.value()), max_payload_bytes, max_scan_records, response_soft_limit_bytes);
}

rocksdb::WriteOptions SyncWriteOptions()
{
    rocksdb::WriteOptions options;
    options.sync = true;
    return options;
}

void CreatePartialStorage(const std::filesystem::path& path, bool write_marker, bool create_messages_cf)
{
    std::error_code ec;
    std::filesystem::create_directories(path.parent_path(), ec);
    Check(!ec, "create partial storage parent: " + ec.message());

    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* raw_db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(options, path.string(), &raw_db);
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

    fetch.clear_channels();
    response.Clear();
    status = core->Fetch(fetch, &response);
    Check(status.ok(), status.message());
    Check(response.messages_size() == 2, "empty channel filter should return all visible messages");
    Check(response.last_seq() == 2, "empty channel filter last_seq");

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
    std::filesystem::remove_all(root);
    {
        auto core = MakeCore(root);
        std::string token = AddToken(*core, 100);
        uint64_t channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);
        PublishAuto(*core, 100, token, channel_id, "layout");
    }

    rocksdb::Options list_options;
    std::vector<std::string> names;
    rocksdb::Status rocks_status = rocksdb::DB::ListColumnFamilies(list_options, storage_path.string(), &names);
    Check(rocks_status.ok(), rocks_status.ToString());
    Check(std::set<std::string>(names.begin(), names.end()) ==
              std::set<std::string>({rocksdb::kDefaultColumnFamilyName, "messages"}),
          "unified storage should contain only default and messages column families");

    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors{
        {rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()},
        {"messages", rocksdb::ColumnFamilyOptions()},
    };
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::DB* raw_db = nullptr;
    rocks_status = rocksdb::DB::Open(
        rocksdb::DBOptions(), storage_path.string(), descriptors, &handles, &raw_db);
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

void TestUnifiedStorageInitializationRecovery()
{
    for (bool create_messages_cf : {false, true}) {
        const auto root = std::filesystem::temp_directory_path() /
                          (create_messages_cf ? "openevent_init_after_cf" : "openevent_init_after_marker");
        std::filesystem::remove_all(root);
        CreatePartialStorage(root, true, create_messages_cf);

        {
            auto storage = openevent::UnifiedStorage::Open(root.string());
            Check(storage.ok(), storage.status().message());
            auto snapshot = storage.value()->CreateSnapshot();
            Check(snapshot.ok(), snapshot.status().message());
            auto max_seq = storage.value()->GetMaxSeq(snapshot.value());
            auto next_channel_id = storage.value()->GetNextChannelId(snapshot.value());
            Check(max_seq.ok() && max_seq.value() == 0,
                  "recovered initialization should restore max_seq=0");
            Check(next_channel_id.ok() && next_channel_id.value() == 1,
                  "recovered initialization should restore next_channel_id=1");
        }

        rocksdb::Options options;
        std::vector<std::string> names;
        rocksdb::Status status = rocksdb::DB::ListColumnFamilies(options, root.string(), &names);
        Check(status.ok(), status.ToString());
        Check(std::set<std::string>(names.begin(), names.end()) ==
                  std::set<std::string>({rocksdb::kDefaultColumnFamilyName, "messages"}),
              "recovered initialization should contain both required column families");
        std::filesystem::remove_all(root);
    }
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
    PutRawRecord(bad_schema, rocksdb::kDefaultColumnFamilyName, "meta:schema_version",
                 openevent::EncodeUint64(2));
    storage = openevent::UnifiedStorage::Open(bad_schema.string());
    Check(!storage.ok(), "unsupported storage schema must be rejected");

    const auto bad_watermark = base / "bad-watermark";
    {
        auto initialized = openevent::UnifiedStorage::Open(bad_watermark.string());
        Check(initialized.ok(), initialized.status().message());
    }
    PutRawRecord(bad_watermark, rocksdb::kDefaultColumnFamilyName, "meta:max_seq",
                 openevent::EncodeUint64(1));
    storage = openevent::UnifiedStorage::Open(bad_watermark.string());
    Check(!storage.ok(), "message watermark without a matching message must be rejected");

    const auto bad_message_root = base / "bad-message-root";
    {
        auto core = MakeCore(bad_message_root);
        std::string token = AddToken(*core, 100);
        uint64_t channel_id = CreateChannel(*core, 100, token, openevent::VISIBILITY_PUBLIC);
        PublishAuto(*core, 100, token, channel_id, "valid-before-corruption");
    }
    const auto bad_message = bad_message_root / "data";
    openevent::EventMessage mismatched_message;
    mismatched_message.set_seq(2);
    mismatched_message.set_payload("mismatched-seq");
    PutRawRecord(bad_message,
                 "messages",
                 std::string("msg/") + openevent::EncodeUint64(1),
                 mismatched_message.SerializeAsString());
    storage = openevent::UnifiedStorage::Open(bad_message.string());
    Check(!storage.ok(), "message key/value seq mismatch must be rejected");

    std::filesystem::remove_all(base);
}

void TestDefaultPayloadLimit()
{
    openevent::ServerConfig config;
    Check(config.max_payload_bytes == 16777216, "default payload limit should be 16 MiB");
    Check(config.shutdown_grace_seconds == 10, "default shutdown grace should be 10 seconds");
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
                << "  max_payload_bytes: 4096\n"
                << "shutdown:\n"
                << "  grace_seconds: 3\n";
    config_file.close();

    auto config = openevent::LoadServerConfig(config_path.string());
    Check(config.ok(), config.status().message());
    Check(config.value().storage_path == storage_path.string(), "storage path should come from config");
    Check(config.value().max_payload_bytes == 4096, "payload limit should come from config");
    Check(config.value().shutdown_grace_seconds == 3, "shutdown grace should come from config");

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
    TestRecipientFilter();
    TestRecipientMustBeChannelMember();
    TestPayloadLimit();
    TestGuaranteedNonCommitStatuses();
    TestListTokensPagination();
    TestListMessagesPagination();
    TestFetchScanBudgetAdvancesCursor();
    TestResponseSoftBudgetPagination();
    TestDeleteTokenOrdersBeforePublish();
    TestUnifiedStorageReopen();
    TestUnifiedStorageLayout();
    TestUint64KeyEncodingOrder();
    TestUnifiedStorageInitializationRecovery();
    TestUnifiedStorageRejectsInvalidState();
    TestDefaultPayloadLimit();
    TestServerConfigRequiresDataPaths();
    TestLoadServerConfigRequiresExistingFile();
    TestLoadServerConfigReadsRequiredPaths();
    TestSystemChannelShape();
    return 0;
}
