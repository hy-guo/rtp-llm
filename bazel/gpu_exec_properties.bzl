"""Select the CUDA13 execution pool for legacy SM120 test requests."""

def sm120_exec_properties(properties):
    """Accept a plain dict; preserve every property except the legacy GPU pool."""
    if type(properties) != "dict":
        fail("sm120_exec_properties expects a plain dict")
    if properties.get("gpu") != "RTX_5000_PRO":
        return properties
    # The BUILD prelude cannot inspect a select; preserve its original
    # default reservation in both configurable branches here.
    legacy_properties = dict(properties)
    legacy_properties["gpu_count"] = legacy_properties.get("gpu_count", "1")
    cuda13_properties = dict(legacy_properties)
    cuda13_properties["gpu"] = "RTX_5000_PRO_CU13"
    return select({
        "@//:using_cuda13_x86": cuda13_properties,
        "//conditions:default": legacy_properties,
    })
