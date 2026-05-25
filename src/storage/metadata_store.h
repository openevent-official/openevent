#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <rocksdb/db.h>

#include "common/status.h"
#include "openevent.pb.h"

namespace openevent {

enum class PendingState : uint64_t {
    kPending = 1,
    kCommitted = 2,
};

struct TokenBindingRecord {
    std::string token;
    uint64_t principal = 0;
};

class MetadataStore {
public:
    explicit MetadataStore(std::unique_ptr<rocksdb::DB> db);

    static Result<std::unique_ptr<MetadataStore>> Open(const std::string& path);

    Result<uint64_t> GetMaxSeq() const;
    Result<uint64_t> GetMinSeq() const;
    Result<uint64_t> GetNextChannelId() const;

    Status PutPendingMessage(uint64_t seq, const ::openevent::Message& message);
    Status PutPendingOffset(uint64_t seq, uint64_t offset);
    Status CommitMessage(uint64_t seq, uint64_t offset, uint64_t channel_id, uint64_t timestamp_ms);
    Result<std::vector<uint64_t>> ListPendingSeqs() const;
    Result<std::optional<::openevent::Message>> GetPendingMessage(uint64_t seq) const;
    Result<std::optional<uint64_t>> GetPendingOffset(uint64_t seq) const;

    Result<std::optional<uint64_t>> GetOffsetForSeq(uint64_t seq) const;
    Result<std::optional<uint64_t>> GetSeqForOffset(uint64_t offset) const;

    Result<uint64_t> AllocateChannelId();
    Status PutChannel(const ::openevent::ChannelInfo& channel);
    Result<std::optional<::openevent::ChannelInfo>> GetChannel(uint64_t channel_id) const;
    Result<std::vector<::openevent::ChannelInfo>> ListChannels() const;

    Status PutToken(const std::string& token, uint64_t principal);
    Status DeleteToken(const std::string& token);
    Result<std::optional<uint64_t>> GetPrincipalForToken(const std::string& token) const;
    Result<std::vector<TokenBindingRecord>> ListTokens() const;

private:
    Result<uint64_t> GetUint64(const std::string& key) const;
    Status PutUint64(const std::string& key, uint64_t value);

    std::unique_ptr<rocksdb::DB> db_;
};

}  // namespace openevent
