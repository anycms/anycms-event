
publish:
    cargo publish --registry crates-io -p anycms-event-derive
    cargo publish --registry crates-io -p anycms-event
    cargo publish --registry crates-io -p anycms-event-sse
    cargo publish --registry crates-io -p anycms-event-redis
    cargo publish --registry crates-io -p anycms-event-actix
    cargo publish --registry crates-io -p anycms-event-axum

release-patch:
    cargo release patch --no-publish --execute

release-minor:
    cargo release minor --no-publish --execute

release-major:
    cargo release major --no-publish --execute
