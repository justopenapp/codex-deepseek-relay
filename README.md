# Codex DeepSeek Relay

Node.js relay for Codex CLI `0.129.0` custom providers. Codex now sends custom
provider traffic through the OpenAI Responses API, while DeepSeek exposes an
OpenAI-compatible Chat Completions endpoint. This relay translates:

- Codex `POST /v1/responses` to DeepSeek `POST /v1/chat/completions`
- DeepSeek streaming chat chunks back to Responses API SSE events
- Codex function tools to DeepSeek chat `tools`
- DeepSeek function tool calls back to Responses `function_call` output items

## Run

```bash
cd codex-deepseek-relay
cp .env.example .env
# edit .env and set DEEPSEEK_API_KEY
npm start
```

The default relay URL is:

```text
http://127.0.0.1:8787/v1
```

## Codex CLI config

Add this to `~/.codex/config.toml`:

```toml
model_provider = "deepseek-relay"
model = "deepseek-chat"

[model_providers.deepseek-relay]
name = "DeepSeek Relay"
base_url = "http://127.0.0.1:8787/v1"
wire_api = "responses"
env_key = "CODEX_RELAY_API_KEY"
```

Then set the local relay token before starting Codex:

```bash
export CODEX_RELAY_API_KEY=local-relay-token
codex -m deepseek-chat
```

Use the same `CODEX_RELAY_API_KEY` value in the relay `.env` if you want inbound
requests to be checked. Leave it unset in the relay `.env` to accept local
requests without checking Codex's bearer token.

## Environment

```text
HOST=127.0.0.1
PORT=8787
DEEPSEEK_API_KEY=sk-your-deepseek-key
DEEPSEEK_BASE_URL=https://api.deepseek.com/v1
DEEPSEEK_MODEL=deepseek-chat
CODEX_RELAY_API_KEY=local-relay-token
DEEPSEEK_STREAM_INCLUDE_USAGE=false
DEEPSEEK_REASONING_OUTPUT=tagged
DEBUG_DEEPSEEK=false
```

`DEEPSEEK_MODEL` overrides whatever model Codex sends. Remove it if you want the
Codex `model` value to pass through.

Set `DEEPSEEK_STREAM_INCLUDE_USAGE=true` only if your DeepSeek-compatible
upstream supports OpenAI's streaming usage option.

`DEEPSEEK_REASONING_OUTPUT=tagged` preserves DeepSeek `reasoning_content` in the
Codex output stream as `<think>...</think>`, then converts it back to
`reasoning_content` on the next request. Set it to `drop` if you want to hide
reasoning content, or `plain` if your upstream does not require reasoning
passback.

Set `DEBUG_DEEPSEEK=true` while debugging to print the DeepSeek request target,
status, and upstream response chunks to the relay console. API keys are not
printed.

## Smoke test

```bash
npm run smoke
```

The smoke test starts a fake DeepSeek upstream and verifies both text streaming
and function-call streaming through the relay.
