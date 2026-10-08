load(":versions.bzl", "XGRAMMAR_SOURCE_COMMIT")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "new_git_repository")

def xgrammar_deps():
    # feat/anytext-budget: structural-tag max_tokens/max_chars enforcement.
    new_git_repository(
        name = "xgrammar",
        remote = "https://github.com/mlc-ai/xgrammar.git",
        commit = XGRAMMAR_SOURCE_COMMIT,
        init_submodules = False,
        build_file = str(Label("@rtp_llm//3rdparty/xgrammar:xgrammar.BUILD")),
    )
