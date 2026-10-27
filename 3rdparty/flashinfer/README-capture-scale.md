# Capture-safe BMM2 scale encoding

CUDA13 x86 keeps the locked `flashinfer-python==0.6.9` wheel with SHA256
`46d013119ecd039a5844c4847325b3571f99cdd8fd9e00839bf008c67b15811a`.
The package annotation applies `flashinfer-prefill-capture-safe.patch` while
rules_python prepares this wheel. Other packages and platform repositories have
no such annotation. No replacement binary or runtime monkeypatch is used.

The patch retains the original scalar bit layout: FP16/BF16 fill only the lower
16-bit element of a zeroed int32 buffer; other dtypes fill an FP32 buffer and view
it as int32. It preserves the scale value, output dtype/shape/device and upper
16 bits. The modified source includes the original Apache license and an explicit
RTP-LLM modification notice. The matching RECORD update is included.

The frozen rules_python native repacker was exercised offline against the exact
original wheel. Its METADATA and runtime ABI remain `0.6.9`; only
`flashinfer/prefill.py` and its RECORD bytes change. The patched prefill SHA256 is
`f535fbd5d03555d3795740e30e1bdad157fbea409d319acca20046972b96e6be`.
The annotation API is explicitly added by
`patches/rules_python/0004-forward-package-whl-patches.patch`, after the existing
three rules_python patches, using their existing `-p0` convention. The wheel
patch itself uses `patch_strip=1`.

The independent helper regression is
`//rtp_llm/models_py/modules/factory/attention/cuda_impl/test:flashinfer_bmm2_scale_cuda_graph_sm120_test`.
It runs real CUDA capture/replay and compares raw int32 bits after poisoning the
captured outputs. Diagnostic expected-version/hash pins are optional in the
functional test and strict when supplied. A diagnostic run of this patch should
expect distribution/runtime `0.6.9` and the patched source hash above.

Offline repack and CPU bit controls do not establish Bazel repository integration
or CUDA correctness. Fresh RTX execution of the original FMHA tests and this
capture/replay regression remains required.
