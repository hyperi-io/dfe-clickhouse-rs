# Project:   dfe-clickhouse
# File:      Makefile
# Purpose:   CI targets wrapping hyperi-ci
#
# License:   Apache-2.0
# Copyright: (c) 2026 HYPERI PTY LIMITED

.PHONY: check quality test build check-features

check:
	hyperi-ci check

quality:
	hyperi-ci run quality

test:
	hyperi-ci run test

build:
	hyperi-ci run build

# CONTRIBUTING.md, "Run the checks locally before you push".
check-features:
	cargo check --all-features
	cargo check --no-default-features
