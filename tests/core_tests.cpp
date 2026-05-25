#include <cstdlib>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>

#include "server/server_config.h"
#include "service/open_event_core.h"
#include "storage/metadata_store.h"
#include "storage/rocksdb_message_store.h"

namespace {

void Check(bool condition, const std::string& message)
{
    if (!condition) {
        std::cerr << "FAILED: " << message << "\n";
        std::exit(1);
    }
}

std::unique_ptr<openevent::OpenEventCore> MakeCore(const std::filesystem::path& root,
                                                   size_t max_payload_bytes = 1024)
{
    auto metadata = openevent::MetadataStore::Open((root / "meta").string());
    Check(metadata.ok(), metadata.status().message());
    auto messages = openevent::RocksDBMessageStore::Open((root / "messages").string());
    Check(messages.ok(), messages.status().message());
    return std::make_unique<openevent::OpenEventCore>(
        std::move(metadata.value()), std::move(messages.value()), max_payload_bytes);
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
    Check(!response.has_more(), "expected no more data");

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

    config.metadata_path = "/tmp/openevent-meta";
    status = openevent::ValidateServerConfig(config);
    Check(!status.ok() && status.code() == grpc::StatusCode::INVALID_ARGUMENT,
          "message store path should be required");

    config.message_store_path = "/tmp/openevent-messages";
    status = openevent::ValidateServerConfig(config);
    Check(status.ok(), status.message());
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
    const auto metadata_path = root / "meta";
    const auto messages_path = root / "messages";

    std::ofstream config_file(config_path);
    config_file << "grpc:\n"
                << "  listen_addr: \"127.0.0.1:19527\"\n"
                << "admin:\n"
                << "  listen_addr: \"127.0.0.1:19528\"\n"
                << "storage:\n"
                << "  metadata_path: \"" << metadata_path.string() << "\"\n"
                << "store:\n"
                << "  rocksdb:\n"
                << "    path: \"" << messages_path.string() << "\"\n"
                << "limits:\n"
                << "  max_payload_bytes: 4096\n";
    config_file.close();

    auto config = openevent::LoadServerConfig(config_path.string());
    Check(config.ok(), config.status().message());
    Check(config.value().metadata_path == metadata_path.string(), "metadata path should come from config");
    Check(config.value().message_store_path == messages_path.string(), "message store path should come from config");
    Check(config.value().max_payload_bytes == 4096, "payload limit should come from config");

    std::filesystem::remove_all(root);
}

void TestRecoverPending()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_recover";
    std::filesystem::remove_all(root);

    {
        auto metadata = openevent::MetadataStore::Open((root / "meta").string());
        Check(metadata.ok(), metadata.status().message());
        auto messages = openevent::RocksDBMessageStore::Open((root / "messages").string());
        Check(messages.ok(), messages.status().message());

        openevent::Message message;
        message.set_seq(1);
        message.set_channel_id(1);
        message.set_principal(100);
        message.set_payload("pending");
        message.set_ts_ms(NowMs());

        openevent::Status status = metadata.value()->PutPendingMessage(1, message);
        Check(status.ok(), status.message());
    }

    auto core = MakeCore(root);
    openevent::Status status = core->RecoverPending();
    Check(status.ok(), status.message());

    auto max_seq = core->MaxSeq();
    Check(max_seq.ok(), max_seq.status().message());
    Check(max_seq.value() == 1, "pending recovery should commit seq 1");

    std::filesystem::remove_all(root);
}

void TestRecoverPendingWithBadOffset()
{
    const auto root = std::filesystem::temp_directory_path() / "openevent_core_recover_bad_offset";
    std::filesystem::remove_all(root);

    {
        auto metadata = openevent::MetadataStore::Open((root / "meta").string());
        Check(metadata.ok(), metadata.status().message());
        auto messages = openevent::RocksDBMessageStore::Open((root / "messages").string());
        Check(messages.ok(), messages.status().message());

        openevent::Message message;
        message.set_seq(1);
        message.set_channel_id(1);
        message.set_principal(100);
        message.set_payload("pending");
        message.set_ts_ms(NowMs());

        openevent::Status status = metadata.value()->PutPendingMessage(1, message);
        Check(status.ok(), status.message());
        status = metadata.value()->PutPendingOffset(1, 99);
        Check(status.ok(), status.message());
    }

    auto core = MakeCore(root);
    openevent::Status status = core->RecoverPending();
    Check(status.ok(), status.message());

    auto max_seq = core->MaxSeq();
    Check(max_seq.ok(), max_seq.status().message());
    Check(max_seq.value() == 1, "bad pending offset should be recovered by append");

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
    TestRecipientFilter();
    TestRecipientMustBeChannelMember();
    TestPayloadLimit();
    TestDefaultPayloadLimit();
    TestServerConfigRequiresDataPaths();
    TestLoadServerConfigRequiresExistingFile();
    TestLoadServerConfigReadsRequiredPaths();
    TestRecoverPending();
    TestRecoverPendingWithBadOffset();
    TestSystemChannelShape();
    return 0;
}
