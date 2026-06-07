
publish:
    cargo publish --registry r404 -p anycms-event-derive
    cargo publish --registry r404 -p anycms-event
    cargo publish --registry r404 -p anycms-event-redis
    cargo publish --registry r404 -p anycms-event-actix
    cargo publish --registry r404 -p anycms-event-axum
    cargo publish --registry r404 -p anycms-event-sse

release-patch:
    cargo release patch --no-publish --execute

release-minor:
    cargo release minor --no-publish --execute

release-major:
    cargo release major --no-publish --execute
