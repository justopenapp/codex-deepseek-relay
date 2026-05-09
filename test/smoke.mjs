import { spawn } from "node:child_process";
import http from "node:http";
import { once } from "node:events";
import { responsesToChatCompletions } from "../src/server.mjs";

const upstreamPort = 18766;
const relayPort = 18767;
const relayToken = "smoke-relay-token";

const upstream = http.createServer((request, response) => {
  if (request.method !== "POST" || request.url !== "/v1/chat/completions") {
    response.writeHead(404);
    response.end();
    return;
  }

  const chunks = [];
  request.on("data", (chunk) => chunks.push(chunk));
  request.on("end", () => {
    const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    response.writeHead(200, {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-cache",
    });

    if (body.messages.at(-1)?.content.includes("call a tool")) {
      writeSse(response, {
        choices: [
          {
            delta: {
              tool_calls: [
                {
                  index: 0,
                  id: "call_smoke",
                  type: "function",
                  function: { name: "exec_command", arguments: "{\"cmd\"" },
                },
              ],
            },
          },
        ],
      });
      writeSse(response, {
        choices: [
          {
            delta: {
              tool_calls: [
                {
                  index: 0,
                  function: { arguments: ":\"pwd\"}" },
                },
              ],
            },
          },
        ],
      });
    } else {
      writeSse(response, {
        choices: [{ delta: { reasoning_content: "hidden reasoning" } }],
      });
      writeSse(response, { choices: [{ delta: { content: "hi" } }] });
    }

    writeSse(response, {
      choices: [{ delta: {}, finish_reason: "stop" }],
      usage: { prompt_tokens: 2, completion_tokens: 1, total_tokens: 3 },
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
    DEEPSEEK_API_KEY: "sk-smoke",
    DEEPSEEK_BASE_URL: `http://127.0.0.1:${upstreamPort}/v1`,
    CODEX_RELAY_API_KEY: relayToken,
    LOG_LEVEL: "error",
  },
  stdio: ["ignore", "pipe", "pipe"],
});

try {
  await waitForRelay();
  const textEvents = await requestResponses("say hi", []);
  assert(
    textEvents.some((event) => event.type === "response.output_text.delta" && event.delta === "hi"),
    "expected text delta",
  );
  assert(
    textEvents.some(
      (event) =>
        event.type === "response.output_text.delta" && event.delta.includes("<think>"),
    ),
    "expected reasoning_content to be preserved in think tags",
  );

  const converted = responsesToChatCompletions(
    {
      model: "deepseek-chat",
      input: [
        {
          type: "message",
          role: "assistant",
          content: [{ type: "output_text", text: "<think>\nwhy\n</think>\n\nanswer" }],
        },
      ],
    },
    { deepseekModel: "deepseek-chat" },
  );
  assert(
    converted.messages[0].reasoning_content === "why" &&
      converted.messages[0].content === "answer",
    "expected think tags to round-trip into reasoning_content",
  );
  assert(
    textEvents.some((event) => event.type === "response.completed"),
    "expected completed event",
  );

  const toolEvents = await requestResponses("call a tool", [
    {
      type: "function",
      name: "exec_command",
      description: "Run a command.",
      parameters: {
        type: "object",
        properties: { cmd: { type: "string" } },
        required: ["cmd"],
      },
    },
  ]);
  assert(
    toolEvents.some((event) => event.type === "response.function_call_arguments.delta"),
    "expected function call argument delta",
  );
  assert(
    toolEvents.some(
      (event) =>
        event.type === "response.output_item.done" &&
        event.item?.type === "function_call" &&
        event.item?.name === "exec_command",
    ),
    "expected completed function call item",
  );

  console.log("smoke ok");
} finally {
  relay.kill("SIGTERM");
  upstream.close();
}

async function requestResponses(prompt, tools) {
  const response = await fetch(`http://127.0.0.1:${relayPort}/v1/responses`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${relayToken}`,
      "content-type": "application/json",
      accept: "text/event-stream",
    },
    body: JSON.stringify({
      model: "deepseek-chat",
      input: [{ type: "message", role: "user", content: [{ type: "input_text", text: prompt }] }],
      tools,
      stream: true,
    }),
  });

  if (!response.ok) {
    throw new Error(`relay returned ${response.status}: ${await response.text()}`);
  }
  const events = [];
  for await (const payload of decodeSse(response.body)) {
    if (payload === "[DONE]") {
      break;
    }
    events.push(JSON.parse(payload));
  }
  return events;
}

async function* decodeSse(readable) {
  const decoder = new TextDecoder();
  let buffer = "";
  for await (const chunk of readable) {
    buffer += decoder.decode(chunk, { stream: true });
    let splitIndex;
    while ((splitIndex = buffer.indexOf("\n\n")) !== -1) {
      const rawEvent = buffer.slice(0, splitIndex);
      buffer = buffer.slice(splitIndex + 2);
      const dataLines = rawEvent
        .split(/\r?\n/)
        .filter((line) => line.startsWith("data:"))
        .map((line) => line.slice(5).trimStart());
      if (dataLines.length > 0) {
        yield dataLines.join("\n");
      }
    }
  }
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
