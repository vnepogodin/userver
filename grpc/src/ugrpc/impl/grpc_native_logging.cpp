#include <ugrpc/impl/grpc_native_logging.hpp>

#include <stdexcept>

#include <absl/log/globals.h>
#include <absl/log/log.h>
#include <absl/log/log_sink.h>
#include <absl/log/log_sink_registry.h>
#include <fmt/format.h>
#include <grpc/support/log.h>

#include <userver/engine/mutex.hpp>
#include <userver/logging/log.hpp>
#include <userver/utils/assert.hpp>
#include <userver/utils/underlying_value.hpp>

USERVER_NAMESPACE_BEGIN

namespace ugrpc::impl {

namespace {

logging::Level ToLogLevel(::absl::LogSeverity severity) noexcept {
    switch (severity) {
        case ::absl::LogSeverity::kInfo:
            return logging::Level::kInfo;
        case ::absl::LogSeverity::kFatal:
            return logging::Level::kCritical;
        case ::absl::LogSeverity::kError:
            [[fallthrough]];
        default: {
            if (ABSL_VLOG_IS_ON(2)) {
                return logging::Level::kDebug;
            }
            return logging::Level::kError;
        }
    }
}

::absl::LogSeverityAtLeast ToAbslLogSeverity(logging::Level level) {
    switch (level) {
        case logging::Level::kDebug:
            absl::SetVLogLevel("*grpc*/*", 2);
            return ::absl::LogSeverityAtLeast::kInfo;
        case logging::Level::kInfo:
            absl::SetVLogLevel("*grpc*/*", -1);
            return ::absl::LogSeverityAtLeast::kInfo;
        case logging::Level::kError:
            absl::SetVLogLevel("*grpc*/*", -1);
            return ::absl::LogSeverityAtLeast::kError;
        default:
            throw std::logic_error(fmt::format(
                "grpcpp log level {} is not allowed. Allowed options: "
                "debug, info, error.",
                logging::ToString(level)
            ));
    }
}

class NativeLogSink : public absl::LogSink {
public:
    NativeLogSink() { absl::AddLogSink(this); }
    ~NativeLogSink() override { absl::RemoveLogSink(this); }

    void Send(const absl::LogEntry& entry) override {
        const auto lvl = ToLogLevel(entry.log_severity());
        if (!logging::ShouldLog(lvl)) return;

        auto& logger = logging::GetDefaultLogger();
        const auto location = utils::impl::SourceLocation::Custom(entry.source_line(), entry.source_filename(), "");
        logging::LogHelper(logger, lvl, location) << entry.text_message();

        // We used to call LogFlush for kError logging level here,
        // but that might lead to a thread switch (there is a coroutine-aware
        // .Wait somewhere down the call chain), which breaks the grpc-core badly:
        // its ExecCtx/ApplicationCallbackExecCtx are attached to a current thread
        // (thread_local that is), and switching threads violates that, obviously.
    }
};

engine::Mutex native_log_level_mutex;
auto native_log_level = logging::Level::kNone;
std::optional<NativeLogSink> native_log_sink = std::nullopt;

}  // namespace

void SetupNativeLogging() { native_log_sink = std::make_optional<NativeLogSink>(); }

void UpdateNativeLogLevel(logging::Level min_log_level_override) {
    std::lock_guard lock(native_log_level_mutex);

    if (utils::UnderlyingValue(min_log_level_override) < utils::UnderlyingValue(native_log_level)) {
        absl::SetMinLogLevel(ToAbslLogSeverity(min_log_level_override));
        native_log_level = min_log_level_override;
    }
}

}  // namespace ugrpc::impl

USERVER_NAMESPACE_END
