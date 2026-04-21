#!/usr/bin/env bash

set -eo

rm -r data/ch/cache*
rm -r data/de/cache*
rm -r data/at/cache*

rm -f classification.log
rm -f pipeline.log

uv run resolve ch -v
uv run resolve de -v
uv run resolve at -v

uv run classify ch -v
uv run classify de -v
uv run classify at -v