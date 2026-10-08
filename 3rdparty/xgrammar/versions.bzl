# Python and native bindings must use the same tokenizer serialization format.
XGRAMMAR_SOURCE_COMMIT = "60fc70ee4e0842eecc81fdd1941f778b1bd8107f"
XGRAMMAR_PYTHON_SOURCE_URL = "https://github.com/hy-guo/rtp-llm/releases/download/qwen4-exp-deps-20261008/xgrammar-0.2.3%2Brtp.60fc70ee.tar.gz"
XGRAMMAR_PYTHON_SOURCE_SHA256 = "c630a8fa70d1d1f30a31ccbdd4c0a14b3540c45d0a314c49cbb8678df88ae33d"
XGRAMMAR_PYTHON_RUNTIME_URL = "https://github.com/hy-guo/rtp-llm/releases/download/qwen4-exp-deps-20261008/xgrammar-0.2.3%2Brtp.60fc70ee-cp310-cp310-linux_x86_64.whl"
XGRAMMAR_PYTHON_RUNTIME_SHA256 = "031302746481bc38b8bdab3194bb0914c5cdcc1a41d43d2fbad7346ac382a7b9"
XGRAMMAR_PYTHON_REQUIREMENTS = [
    "xgrammar @ " + XGRAMMAR_PYTHON_RUNTIME_URL + "#sha256=" + XGRAMMAR_PYTHON_RUNTIME_SHA256 + ' ; python_version == \"3.10\" and sys_platform == \"linux\" and platform_machine == \"x86_64\"',
    "xgrammar @ " + XGRAMMAR_PYTHON_SOURCE_URL + "#sha256=" + XGRAMMAR_PYTHON_SOURCE_SHA256 + ' ; python_version != \"3.10\" or sys_platform != \"linux\" or platform_machine != \"x86_64\"',
]
