"""Real CUDA capture/replay regression for FlashInfer's scalar bit encoding."""

import hashlib
import importlib
import importlib.metadata
import importlib.util
import inspect
import json
import os
import re
import unittest
from pathlib import Path

import torch

EXPECTED_ENV = {
    "distribution_version": "RTP_FLASHINFER_EXPECTED_DISTRIBUTION_VERSION",
    "prefill_sha256": "RTP_FLASHINFER_EXPECTED_PREFILL_SHA256",
    "runtime_version": "RTP_FLASHINFER_EXPECTED_RUNTIME_ABI",
    "torch_version": "RTP_FLASHINFER_EXPECTED_TORCH_VERSION",
}
SCALARS = (1.0, 0.0, -1.0 / 3.0)
POISON = 0x5A5A5A5A


def validate_runtime_evidence(evidence, expected):
    """Check real package compatibility; optional pins belong to diagnostics."""
    for name in ("distribution_version", "runtime_version", "torch_version"):
        if not isinstance(evidence.get(name), str) or not re.fullmatch(
            r"[A-Za-z0-9._+\-]{1,128}", evidence[name]
        ):
            raise AssertionError("invalid runtime version evidence")
    if not re.fullmatch(r"[a-f0-9]{64}", evidence.get("prefill_sha256", "")):
        raise AssertionError("invalid prefill source digest")
    for name in EXPECTED_ENV:
        wanted = expected.get(name)
        if wanted:
            if name == "prefill_sha256" and not re.fullmatch(r"[a-f0-9]{64}", wanted):
                raise AssertionError("invalid expected prefill digest")
            if evidence[name] != wanted:
                raise AssertionError("diagnostic dependency pin mismatch: " + name)
    if evidence.get("cubin_present"):
        if evidence.get("cubin_version") != evidence["runtime_version"]:
            raise AssertionError("FlashInfer cubin ABI version mismatch")
    if evidence.get("jit_cache_present"):
        version = evidence.get("jit_cache_version")
        if (
            not isinstance(version, str)
            or not re.fullmatch(r"[A-Za-z0-9._+\-]{1,128}", version)
            or not version.startswith(evidence["runtime_version"])
        ):
            raise AssertionError("FlashInfer jit-cache ABI version mismatch")


def cpu_reference(scale, dtype):
    """Independent original bit layout, constructed strictly outside capture."""
    if dtype in (torch.float16, torch.bfloat16):
        result = torch.zeros(1, dtype=torch.int32, device="cpu")
        result.view(dtype)[0] = scale
        return result
    return torch.tensor([scale], dtype=torch.float32, device="cpu").view(torch.int32)


class FlashInferBmm2ScaleCudaGraphTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if torch.version.hip is not None or not torch.cuda.is_available():
            raise AssertionError("CUDA GPU is required; this test cannot skip")
        if torch.cuda.device_count() < 1:
            raise AssertionError("one RTX GPU is required")
        cls.device = torch.device("cuda", torch.cuda.current_device())
        model = torch.cuda.get_device_name(cls.device)
        capability = torch.cuda.get_device_capability(cls.device)
        if "RTX PRO 5000" not in model.upper() or tuple(capability) != (12, 0):
            raise AssertionError("the declared RTX PRO 5000 SM120 device is required")
        prefill = importlib.import_module("flashinfer.prefill")
        flashinfer = importlib.import_module("flashinfer")
        helper = prefill._create_scale_bmm2_d_tensor
        source = inspect.getsourcefile(prefill)
        if (
            source is None
            or inspect.getsourcefile(helper) != source
            or helper.__module__ != prefill.__name__
        ):
            raise AssertionError("the actual FlashInfer prefill helper is required")
        evidence = {
            "distribution_version": importlib.metadata.version("flashinfer-python"),
            "runtime_version": flashinfer.__version__,
            "torch_version": str(torch.__version__),
            "torch_cuda": torch.version.cuda,
            "prefill_sha256": hashlib.sha256(Path(source).read_bytes()).hexdigest(),
            "cubin_present": importlib.util.find_spec("flashinfer_cubin") is not None,
            "jit_cache_present": importlib.util.find_spec("flashinfer_jit_cache")
            is not None,
            "physical_model": model,
            "device_capability": list(capability),
        }
        for module, present, field in (
            ("flashinfer_cubin", "cubin_present", "cubin_version"),
            ("flashinfer_jit_cache", "jit_cache_present", "jit_cache_version"),
        ):
            evidence[field] = (
                getattr(importlib.import_module(module), "__version__")
                if evidence[present]
                else None
            )
        expected = {key: os.environ.get(name) for key, name in EXPECTED_ENV.items()}
        validate_runtime_evidence(evidence, expected)
        evidence["diagnostic_pins_provided"] = {
            key: bool(value) for key, value in expected.items()
        }
        print(
            "FLASHINFER_SCALE_REGRESSION_RUNTIME "
            + json.dumps(evidence, sort_keys=True),
            flush=True,
        )
        cls.helper = staticmethod(helper)

    def _capture_replay_bits(self, dtype):
        references = [cpu_reference(scale, dtype) for scale in SCALARS]
        for reference in references:
            self.assertNotEqual(reference.item() & 0xFFFFFFFF, POISON)
        stream = torch.cuda.Stream(device=self.device)
        stream.wait_stream(torch.cuda.current_stream(self.device))
        with torch.cuda.stream(stream):
            for _ in range(3):
                warmup = [self.helper(scale, dtype, self.device) for scale in SCALARS]
        stream.synchronize()
        del warmup
        graph = torch.cuda.CUDAGraph()
        try:
            # No host tensor read, CPU reference construction or explicit
            # synchronization occurs inside this capture body.
            with torch.cuda.graph(graph, stream=stream):
                outputs = [self.helper(scale, dtype, self.device) for scale in SCALARS]
            replay_count = 0
            for _ in range(2):
                with torch.cuda.stream(stream):
                    for output in outputs:
                        output.fill_(POISON)
                    graph.replay()
                replay_count += 1
                stream.synchronize()
                for output, reference in zip(outputs, references):
                    self.assertEqual(output.dtype, torch.int32)
                    self.assertEqual(tuple(output.shape), (1,))
                    self.assertEqual(output.device, self.device)
                    actual = output.detach().cpu()
                    self.assertTrue(
                        torch.equal(actual, reference),
                        "replayed raw int32 bits differ from the original CPU encoding",
                    )
                    if dtype in (torch.float16, torch.bfloat16):
                        self.assertEqual((actual.item() & 0xFFFFFFFF) >> 16, 0)
            self.assertEqual(replay_count, 2)
            print(
                "FLASHINFER_SCALE_REGRESSION_REPLAY "
                + json.dumps(
                    {
                        "dtype": str(dtype),
                        "scalars": len(SCALARS),
                        "graph_replays": replay_count,
                        "raw_int32_checks": replay_count * len(SCALARS),
                        "cpu_reference_bits": [
                            "%08x" % (value.item() & 0xFFFFFFFF) for value in references
                        ],
                        "upper16_zero_checked": dtype
                        in (torch.float16, torch.bfloat16),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
        finally:
            graph.reset()

    def test_fp16_capture_replay_bits(self):
        self._capture_replay_bits(torch.float16)

    def test_bf16_capture_replay_bits(self):
        self._capture_replay_bits(torch.bfloat16)

    def test_fp32_capture_replay_bits(self):
        self._capture_replay_bits(torch.float32)


if __name__ == "__main__":
    unittest.main()
