#!/usr/bin/env bash
# Tests next-version.sh against throwaway repositories. Run: bash .github/scripts/next-version_test.sh
set -euo pipefail
script="$(cd "$(dirname "$0")" && pwd)/next-version.sh"
fail=0

check() { # name, expected, git commands...
	local name=$1 want=$2
	shift 2
	local dir
	dir=$(mktemp -d)
	(
		cd "$dir"
		git init -q
		git config user.email t@example.com
		git config user.name t
		git config tag.gpgSign false
		git config commit.gpgSign false
		for cmd in "$@"; do eval "$cmd"; done
	)
	local got
	got=$(cd "$dir" && "$script")
	rm -rf "$dir"
	if [[ "$got" != "$want" ]]; then
		echo "FAIL $name: got '$got', want '$want'"
		fail=1
	else
		echo "ok   $name"
	fi
}

c() { echo "git commit -q --allow-empty -m \"$1\""; }

check "first release" "v0.1.0" "$(c 'init')"
check "patch" "v0.1.1" "$(c 'init')" "git tag v0.1.0" "$(c 'fix: x')"
check "minor on feat" "v0.2.0" "$(c 'init')" "git tag v0.1.0" "$(c 'fix: x')" "$(c 'feat(api): y')"
check "breaking before v1 bumps minor" "v0.2.0" "$(c 'init')" "git tag v0.1.3" "$(c 'feat!: z')"
check "breaking after v1 bumps major" "v2.0.0" "$(c 'init')" "git tag v1.4.2" "$(c 'refactor(core)!: z')"
check "breaking footer" "v2.0.0" "$(c 'init')" "git tag v1.4.2" "git commit -q --allow-empty -m 'chore: a' -m 'BREAKING CHANGE: b'"
check "HEAD already tagged" "" "$(c 'init')" "git tag v0.3.0"
check "pre-release tags ignored" "v0.1.1" "$(c 'init')" "git tag v0.1.0" "$(c 'fix: a')" "git tag v0.2.0-rc.1" "$(c 'fix: b')"
check "non-conventional message is a patch" "v1.0.1" "$(c 'init')" "git tag v1.0.0" "$(c 'Add docs')"

exit $fail
