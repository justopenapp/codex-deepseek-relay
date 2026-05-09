# Codex Provider Relay

Node.js relay for Codex CLI `0.129.0` custom providers. Codex now sends custom
provider traffic through the OpenAI Responses API, while DeepSeek and 讯飞星辰
expose OpenAI-compatible Chat Completions endpoints. This relay translates:

- Codex `POST /v1/responses` to provider `POST /chat/completions`
- Provider streaming chat chunks back to Responses API SSE events
- Codex function tools to chat `tools`
- Provider function tool calls back to Responses `function_call` output items

## Run

```bash
cd codex-deepseek-relay
cp .env.example .env
# edit .env and set PROVIDER plus the matching API key
npm start
```

The default relay URL is:

```text
http://127.0.0.1:8787/v1
```

## Codex CLI config

Add this to `~/.codex/config.toml`:

```toml
model_provider = "coding-relay"
model = "deepseek-v4-pro"

[model_providers.coding-relay]
name = "Coding Relay"
base_url = "http://127.0.0.1:8787/v1"
wire_api = "responses"
```

For 讯飞星辰, set `model = "astron-code-latest"` instead. Then start Codex
with the matching model:

```bash
codex -m deepseek-v4-pro
# or
codex -m astron-code-latest
```

## Environment

```text
HOST=127.0.0.1
PORT=8787
PROVIDER=deepseek

DEEPSEEK_API_KEY=sk-your-deepseek-key
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_MODEL=deepseek-v4-pro

XFYUN_API_KEY=your-xfyun-api-key
XFYUN_BASE_URL=https://maas-coding-api.cn-huabei-1.xf-yun.com/v2
XFYUN_MODEL=astron-code-latest

UPSTREAM_STREAM_INCLUDE_USAGE=false
REASONING_OUTPUT=tagged
DEBUG_UPSTREAM=false
```

Set `PROVIDER=deepseek` for DeepSeek, or `PROVIDER=xfyun` for 讯飞星辰. The
provider model value, such as `DEEPSEEK_MODEL` or `XFYUN_MODEL`, overrides
whatever model Codex sends. Remove the provider model value if you want the
Codex `model` value to pass through.

For 讯飞星辰 coding:

```text
PROVIDER=xfyun
XFYUN_BASE_URL=https://maas-coding-api.cn-huabei-1.xf-yun.com/v2
XFYUN_MODEL=astron-code-latest
XFYUN_API_KEY=your-xfyun-api-key
```

Set `UPSTREAM_STREAM_INCLUDE_USAGE=true` only if your upstream supports
OpenAI's streaming usage option.

`REASONING_OUTPUT=tagged` preserves upstream `reasoning_content` in the Codex
output stream as `<think>...</think>`, then converts it back to
`reasoning_content` on the next request. This is required by thinking-mode
providers that expect the reasoning content to be passed back. Set it to `drop`
if you want to hide reasoning content, or `plain` if your upstream does not
require reasoning passback.

Set `DEBUG_UPSTREAM=true` while debugging to print the upstream request target,
status, and response chunks to the relay console. API keys are not printed.

The old `DEEPSEEK_STREAM_INCLUDE_USAGE`, `DEEPSEEK_REASONING_OUTPUT`, and
`DEBUG_DEEPSEEK` environment names are still accepted for existing setups.

## Smoke test

```bash
npm run smoke
```

The smoke test starts a fake upstream and verifies both text streaming and
function-call streaming through the relay.
