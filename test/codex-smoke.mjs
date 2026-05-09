import { spawn } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import http from "node:http";
import { once } from "node:events";

const upstreamPort = 18768;
const relayPort = 18769;

const upstream = http.createServer((request, response) => {
  if (request.method !== "POST" || request.url !== "/v1/chat/completions") {
    response.writeHead(404);
    response.end();
    return;
  }

  request.resume();
  request.on("end", () => {
    response.writeHead(200, {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-cache",
    });
    writeSse(response, { choices: [{ delta: { content: "hi from relay" } }] });
    writeSse(response, {
      choices: [{ delta: {}, finish_reason: "stop" }],
      usage: { prompt_tokens: 2, completion_tokens: 3, total_tokens: 5 },
    });
    response.write("data: [DONE]\n\n");
    response.end();
  });
});

await listen(upstream, upstreamPort);

const relay = spawn(process.execPath, ["src/server.mjs"], {
  cwd: new URL("..", import.meta.url),
  env: {
    ...process.env,
    HOST: "127.0.0.1",
    PORT: String(relayPort),
    PROVIDER: "deepseek",
    DEEPSEEK_API_KEY: "sk-smoke",
    DEEPSEEK_BASE_URL: `http://127.0.0.1:${upstreamPort}/v1`,
    LOG_LEVEL: "error",
  },
  stdio: ["ignore", "pipe", "pipe"],
});

try {
  await waitForRelay();
  const codexHome = mkdtempSync(join(tmpdir(), "codex-relay-home-"));
  const codex = spawn(
    "codex",
    [
      "exec",
      "--ignore-user-config",
      "-c",
      'model_provider="deepseek-relay"',
      "-c",
      'model_providers.deepseek-relay.name="DeepSeek Relay"',
      "-c",
      `model_providers.deepseek-relay.base_url="http://127.0.0.1:${relayPort}/v1"`,
      "-c",
      'model_providers.deepseek-relay.wire_api="responses"',
      "-m",
      "deepseek-chat",
      "--skip-git-repo-check",
      "--json",
      "say hi",
    ],
    {
      cwd: tmpdir(),
      env: {
        ...process.env,
        CODEX_HOME: codexHome,
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );

  const result = await collectProcess(codex, 30000);
  if (result.code !== 0) {
    throw new Error(`codex exited ${result.code}\nSTDOUT:\n${result.stdout}\nSTDERR:\n${result.stderr}`);
  }

  const messages = result.stdout
    .split(/\r?\n/)
    .filter((line) => line.trim().startsWith("{"))
    .map((line) => JSON.parse(line));

  assert(
    messages.some(
      (event) => event.type === "item.completed" && event.item?.text === "hi from relay",
    ),
    `Codex did not receive relay message. STDOUT:\n${result.stdout}`,
  );

  console.log("codex smoke ok");
} finally {
  relay.kill("SIGTERM");
  upstream.close();
}

async function waitForRelay() {
  const deadline = Date.now() + 3000;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`http://127.0.0.1:${relayPort}/health`);
      if (response.ok) {
        return;
      }
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }
  throw new Error("relay did not start");
}

async function collectProcess(child, timeoutMs) {
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => {
    stdout += chunk.toString("utf8");
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString("utf8");
  });

  const timeout = setTimeout(() => child.kill("SIGTERM"), timeoutMs);
  const [code] = await once(child, "exit");
  clearTimeout(timeout);
  return { code, stdout, stderr };
}

function writeSse(response, payload) {
  response.write(`data: ${JSON.stringify(payload)}\n\n`);
}

async function listen(server, port) {
  server.listen(port, "127.0.0.1");
  await once(server, "listening");
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}
