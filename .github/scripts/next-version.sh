#!/usr/bin/env bash
# Prints the next release tag (vMAJOR.MINOR.PATCH) for HEAD, from the commit
# messages since the latest release tag (Conventional Commits):
#   - "type!:" or "BREAKING CHANGE" in the body -> major (minor while < v1.0.0)
#   - "feat:" / "feat(scope):"                  -> minor
#   - anything else                              -> patch
# Prints v0.1.0 when there is no release tag yet, and nothing when HEAD is
# already tagged or there is no commit since the latest tag.
# Needs the full history and tags (actions/checkout with fetch-depth: 0).
set -euo pipefail

if git tag --points-at HEAD | grep -qE '^v[0-9]+\.[0-9]+\.[0-9]+$'; then
	exit 0
fi

last=$(git describe --tags --abbrev=0 --match 'v[0-9]*.[0-9]*.[0-9]*' --exclude '*-*' 2>/dev/null || true)
if [[ -z "$last" ]]; then
	echo "v0.1.0"
	exit 0
fi
if [[ -z "$(git rev-list "$last..HEAD")" ]]; then
	exit 0
fi

IFS=. read -r major minor patch <<<"${last#v}"
log=$(git log --format='%s%n%b' "$last..HEAD")
if grep -qE '^[a-z]+(\([^)]*\))?!:|^BREAKING[ -]CHANGE' <<<"$log"; then
	bump=major
elif grep -qE '^feat(\([^)]*\))?:' <<<"$log"; then
	bump=minor
else
	bump=patch
fi
if [[ $bump == major && $major == 0 ]]; then
	bump=minor # before v1.0.0, breaking changes bump the minor version (Go module convention)
fi

case $bump in
major) echo "v$((major + 1)).0.0" ;;
minor) echo "v$major.$((minor + 1)).0" ;;
patch) echo "v$major.$minor.$((patch + 1))" ;;
esac
