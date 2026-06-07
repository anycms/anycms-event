
dev:
    cargo run

build:
    cargo build

release:
    cargo build --release


release-patch:
    cargo release patch --no-publish --execute

release-minor:
    cargo release minor --no-publish --execute

release-major:
    cargo release major --no-publish --execute

upgrade:
    cargo +nightly update --breaking -Z unstable-options

# 发布 crate 到 r404 私有仓库 (git.404.net.cn)
publish-crate:
    cargo publish --registry r404

# 构建 ts-sdk 并发布到 npmjs.org (@anycms 公开包)
publish-ts:
    cd ts-sdk && npx tsc
    cd ts-sdk && npm publish
    cd ts-sdk && npm publish --@anycms:registry=https://registry.npmjs.org

# 同时发布 crate 和 ts-sdk
publish-all: publish-crate publish-ts
