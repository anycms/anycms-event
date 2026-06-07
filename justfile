
publish:
    cargo publish --registry r404 -p anycms-event-derive
    cargo publish --registry r404 -p anycms-event
    cargo publish --registry r404 -p anycms-event-redis

release-patch:
    cargo release patch --no-publish --execute

release-minor:
    cargo release minor --no-publish --execute

release-major:
    cargo release major --no-publish --execute
