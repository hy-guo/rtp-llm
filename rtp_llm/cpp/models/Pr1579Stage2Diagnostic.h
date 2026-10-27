#pragma once
// Isolated PR1579 diagnostic only. Not a production feature or a PR change.
#include <cstdlib>
#include <cstring>
#include <pybind11/embed.h>
#include <torch/csrc/utils/pybind.h>
#include <torch/all.h>

namespace rtp_llm::pr1579_stage2 {

inline bool enabled() {
    const char* value = std::getenv("PR1579_STAGE2_NATIVE_CAPTURE");
    return value != nullptr && std::strcmp(value, "1") == 0;
}

inline void tensor(pybind11::dict& fields, const char* name, const torch::Tensor& value) {
    if (value.defined()) {
        fields[pybind11::str(name)] = value;
    } else {
        fields[pybind11::str(name)] = pybind11::none();
    }
}

template <typename Builder>
inline void capture(const char* event, Builder&& builder) {
    // Disabled strict/reference servers never initialize Python objects here.
    if (!enabled()) {
        return;
    }
    pybind11::gil_scoped_acquire gil;
    pybind11::dict fields;
    builder(fields);
    // The Python observer enforces nonce, per-event three-slot rings and byte
    // limits. Only GPU clones/events occur here; D2H happens after the request.
    // Deliberately do not swallow errors: incomplete capture is diagnostic failure.
    pybind11::module_::import("stage2_observer").attr("capture_native")(event, **fields);
}

}  // namespace rtp_llm::pr1579_stage2
