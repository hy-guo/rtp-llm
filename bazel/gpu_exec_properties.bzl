"""Select the CUDA13 execution pool for legacy SM120 test requests."""

def sm120_exec_properties(properties):
    """Accept a plain dict; preserve every property except the legacy GPU pool."""
    if type(properties) != "dict":
        fail("sm120_exec_properties expects a plain dict")
    if properties.get("gpu") != "RTX_5000_PRO":
        return properties
    cuda13_properties = dict(properties)
    cuda13_properties["gpu"] = "RTX_5000_PRO_CU13"
    return select({
        "@//:using_cuda13_x86": cuda13_properties,
        "//conditions:default": properties,
    })
