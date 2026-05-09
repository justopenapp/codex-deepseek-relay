import http from "node:http";
import { readFileSync, existsSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const projectRoot = resolve(__dirname, "..");

loadDotEnv(resolve(projectRoot, ".env"));
loadDotEnv(resolve(process.cwd(), ".env"));

const config = {
  host: process.env.HOST || "127.0.0.1",
  port: parseInteger(process.env.PORT, 8787),
  deepseekApiKey: process.env.DEEPSEEK_API_KEY || "",
  deepseekBaseUrl: trimTrailingSlash(
    process.env.DEEPSEEK_BASE_URL || "https://api.deepseek.com/v1",
  ),
  deepseekModel: process.env.DEEPSEEK_MODEL || "",
  relayApiKey: process.env.RELAY_API_KEY || process.env.CODEX_RELAY_API_KEY || "",
  maxBodyBytes: parseInteger(process.env.MAX_BODY_BYTES, 20 * 1024 * 1024),
  includeStreamUsage: process.env.DEEPSEEK_STREAM_INCLUDE_USAGE === "true",
  reasoningOutput: normalizeReasoningOutput(process.env.DEEPSEEK_REASONING_OUTPUT),
  debugUpstream: process.env.DEBUG_DEEPSEEK === "true",
  logLevel: process.env.LOG_LEVEL || "info",
};

if (isMainModule()) {
  const server = http.createServer((request, response) => {
    handleRequest(request, response).catch((error) => {
      log("error", "Unhandled request error", { error: error.stack || String(error) });
      if (!response.headersSent) {
        sendJson(response, 500, {
          error: {
            message: "Relay internal error",
            type: "relay_error",
          },
        });
        return;
      }
      response.end();
    });
  });

  server.listen(config.port, config.host, () => {
    log("info", "Codex DeepSeek relay listening", {
      url: `http://${config.host}:${config.port}`,
      upstream: config.deepseekBaseUrl,
      model: config.deepseekModel || "<request model>",
    });
  });
}

async function handleRequest(request, response) {
  const url = new URL(request.url || "/", `http://${request.headers.host || "localhost"}`);

  if (request.method === "GET" && url.pathname === "/health") {
    sendJson(response, 200, {
      ok: true,
      provider: "deepseek",
      model: config.deepseekModel || null,
    });
    return;
  }

  if (request.method === "GET" && matchesEndpoint(url.pathname, "models")) {
    sendJson(response, 200, {
      object: "list",
      data: [
        modelObject(config.deepseekModel || "deepseek-chat"),
        modelObject("deepseek-chat"),
        modelObject("deepseek-reasoner"),
      ],
    });
    return;
  }

  if (request.method !== "POST" || !matchesEndpoint(url.pathname, "responses")) {
    sendJson(response, 404, {
      error: {
        message: "Use POST /v1/responses for Codex CLI requests.",
        type: "not_found",
      },
    });
    return;
  }

  if (config.relayApiKey && !isAuthorized(request, config.relayApiKey)) {
    sendJson(response, 401, {
      error: {
        message: "Invalid relay bearer token.",
        type: "unauthorized",
      },
    });
    return;
  }

  if (!config.deepseekApiKey) {
    sendJson(response, 500, {
      error: {
        message: "DEEPSEEK_API_KEY is not configured for the relay.",
        type: "configuration_error",
      },
    });
    return;
  }

  let responsesRequest;
  try {
    responsesRequest = await readJsonBody(request, config.maxBodyBytes);
  } catch (error) {
    sendJson(response, error.statusCode || 400, {
      error: {
        message: error.message,
        type: "invalid_request_error",
      },
    });
    return;
  }
  const chatRequest = responsesToChatCompletions(responsesRequest, config);

  log("debug", "Forwarding request", {
    model: chatRequest.model,
    messages: chatRequest.messages.length,
    tools: chatRequest.tools?.length || 0,
    stream: chatRequest.stream,
  });

  if (responsesRequest.stream === false) {
    await handleNonStreamingResponse(response, responsesRequest, chatRequest);
    return;
  }

  await handleStreamingResponse(request, response, responsesRequest, chatRequest);
}

async function handleNonStreamingResponse(response, responsesRequest, chatRequest) {
  const url = upstreamUrl("/chat/completions");
  const body = { ...chatRequest, stream: false };
  debugDeepSeekRequest("non-stream", url, body);

  const upstreamResponse = await fetch(url, {
    method: "POST",
    headers: upstreamHeaders(false),
    body: JSON.stringify(body),
  });

  debugDeepSeekResponseHeaders("non-stream", upstreamResponse);

  if (!upstreamResponse.ok) {
    await proxyUpstreamError(response, upstreamResponse);
    return;
  }

  const chatResponse = await upstreamResponse.json();
  debugDeepSeekResponse("non-stream", chatResponse);
  const converted = chatCompletionToResponse(chatResponse, responsesRequest);
  sendJson(response, 200, converted);
}

async function handleStreamingResponse(
  request,
  response,
  responsesRequest,
  chatRequest,
) {
  const abortController = new AbortController();
  request.on("close", () => abortController.abort());

  const upstreamBody = {
    ...chatRequest,
    stream: true,
  };
  if (config.includeStreamUsage) {
    upstreamBody.stream_options = { include_usage: true };
  }

  const url = upstreamUrl("/chat/completions");
  debugDeepSeekRequest("stream", url, upstreamBody);

  const upstreamResponse = await fetch(url, {
    method: "POST",
    headers: upstreamHeaders(true),
    body: JSON.stringify(upstreamBody),
    signal: abortController.signal,
  });

  debugDeepSeekResponseHeaders("stream", upstreamResponse);

  if (!upstreamResponse.ok) {
    await proxyUpstreamError(response, upstreamResponse);
    return;
  }

  response.writeHead(200, {
    "content-type": "text/event-stream; charset=utf-8",
    "cache-control": "no-cache, no-transform",
    connection: "keep-alive",
    "x-accel-buffering": "no",
  });

  const encoder = createResponsesStreamEncoder(response, responsesRequest);
  encoder.created();

  try {
    for await (const payload of decodeSse(upstreamResponse.body)) {
      if (payload === "[DONE]") {
        break;
      }
      const chunk = safeJsonParse(payload);
      if (!chunk) {
        continue;
      }
      debugDeepSeekResponse("stream-chunk", chunk);
      encoder.consumeChatChunk(chunk);
    }
    encoder.completed();
  } catch (error) {
    if (abortController.signal.aborted) {
      return;
    }
    encoder.failed(error);
  } finally {
    response.end();
  }
}

export function responsesToChatCompletions(requestBody, relayConfig = config) {
  const messages = [];
  const unavailableTools = [];

  const instructions = normalizeInstructions(requestBody.instructions);
  if (instructions) {
    messages.push({ role: "system", content: instructions });
  }

  const inputItems = normalizeArray(requestBody.input);
  for (const item of inputItems) {
    appendInputItem(messages, item);
  }

  const tools = [];
  for (const tool of normalizeArray(requestBody.tools)) {
    if (tool?.type === "function" && tool.name) {
      tools.push({
        type: "function",
        function: {
          name: tool.name,
          description: tool.description || "",
          parameters: tool.parameters || { type: "object", properties: {} },
          ...(tool.strict === true ? { strict: true } : {}),
        },
      });
    } else if (tool?.type) {
      unavailableTools.push(tool.type);
    }
  }

  if (unavailableTools.length > 0) {
    messages.push({
      role: "system",
      content:
        `The following non-function Responses API tools are not available through this DeepSeek relay: ` +
        `${[...new Set(unavailableTools)].join(", ")}. Use only the provided function tools.`,
    });
  }

  const chatRequest = {
    model: relayConfig.deepseekModel || requestBody.model || "deepseek-chat",
    messages,
    stream: requestBody.stream !== false,
  };

  copyIfPresent(requestBody, chatRequest, "temperature");
  copyIfPresent(requestBody, chatRequest, "top_p");
  copyIfPresent(requestBody, chatRequest, "frequency_penalty");
  copyIfPresent(requestBody, chatRequest, "presence_penalty");
  copyIfPresent(requestBody, chatRequest, "stop");
  copyIfPresent(requestBody, chatRequest, "seed");
  if (requestBody.max_output_tokens != null) {
    chatRequest.max_tokens = requestBody.max_output_tokens;
  }
  if (tools.length > 0) {
    chatRequest.tools = tools;
    chatRequest.tool_choice = mapToolChoice(requestBody.tool_choice);
  }
  if (typeof requestBody.parallel_tool_calls === "boolean") {
    chatRequest.parallel_tool_calls = requestBody.parallel_tool_calls;
  }

  return chatRequest;
}

function appendInputItem(messages, item) {
  if (typeof item === "string") {
    messages.push({ role: "user", content: item });
    return;
  }

  if (!item || typeof item !== "object") {
    return;
  }

  if (item.type === "message") {
    messages.push(responseMessageToChatMessage(item));
    return;
  }

  if (item.type === "function_call") {
    messages.push({
      role: "assistant",
      content: null,
      tool_calls: [
        {
          id: item.call_id || item.id || `call_${randomId()}`,
          type: "function",
          function: {
            name: item.name || "unknown_tool",
            arguments: item.arguments || "",
          },
        },
      ],
    });
    return;
  }

  if (item.type === "function_call_output") {
    messages.push({
      role: "tool",
      tool_call_id: item.call_id || item.id,
      content: outputToText(item.output),
    });
    return;
  }

  if (item.type === "reasoning") {
    return;
  }

  messages.push({
    role: "user",
    content: itemToText(item),
  });
}

function chatCompletionToResponse(chatResponse, requestBody) {
  const choice = chatResponse.choices?.[0] || {};
  const message = choice.message || {};
  const output = messageToResponseOutput(message);
  const now = Math.floor(Date.now() / 1000);

  return {
    id: `resp_${randomId()}`,
    object: "response",
    created_at: chatResponse.created || now,
    status: "completed",
    model: chatResponse.model || requestBody.model,
    output,
    usage: mapUsage(chatResponse.usage),
  };
}

function messageToResponseOutput(message) {
  const output = [];
  if (typeof message.content === "string" && message.content.length > 0) {
    output.push({
      id: `msg_${randomId()}`,
      type: "message",
      status: "completed",
      role: "assistant",
      content: [
        {
          type: "output_text",
          text: message.content,
          annotations: [],
        },
      ],
    });
  }

  for (const toolCall of normalizeArray(message.tool_calls)) {
    output.push({
      id: toolCall.id || `fc_${randomId()}`,
      type: "function_call",
      status: "completed",
      call_id: toolCall.id || `call_${randomId()}`,
      name: toolCall.function?.name || "unknown_tool",
      arguments: toolCall.function?.arguments || "",
    });
  }

  if (output.length === 0) {
    output.push({
      id: `msg_${randomId()}`,
      type: "message",
      status: "completed",
      role: "assistant",
      content: [{ type: "output_text", text: "", annotations: [] }],
    });
  }

  return output;
}

function createResponsesStreamEncoder(response, requestBody) {
  const responseId = `resp_${randomId()}`;
  const createdAt = Math.floor(Date.now() / 1000);
  const output = [];
  const toolCalls = new Map();
  let messageItem = null;
  let messageOutputIndex = null;
  let messageText = "";
  let reasoningTagOpen = false;
  let finalUsage = null;
  let completed = false;
  let nextOutputIndex = 0;

  const writeEvent = (event) => {
    response.write(`event: ${event.type}\n`);
    response.write(`data: ${JSON.stringify(event)}\n\n`);
  };

  const responseBase = (status) => ({
    id: responseId,
    object: "response",
    created_at: createdAt,
    status,
    model: requestBody.model,
    output: status === "completed" ? output.filter(Boolean) : [],
  });

  const ensureMessage = () => {
    if (messageItem) {
      return messageItem;
    }

    messageItem = {
      id: `msg_${randomId()}`,
      type: "message",
      status: "in_progress",
      role: "assistant",
      content: [],
    };
    messageOutputIndex = nextOutputIndex;
    nextOutputIndex += 1;

    writeEvent({
      type: "response.output_item.added",
      output_index: messageOutputIndex,
      item: messageItem,
    });
    writeEvent({
      type: "response.content_part.added",
      item_id: messageItem.id,
      output_index: messageOutputIndex,
      content_index: 0,
      part: { type: "output_text", text: "", annotations: [] },
    });
    return messageItem;
  };

  const emitOutputDelta = (delta) => {
    if (!delta) {
      return;
    }
    const item = ensureMessage();
    messageText += delta;
    writeEvent({
      type: "response.output_text.delta",
      item_id: item.id,
      output_index: messageOutputIndex,
      content_index: 0,
      delta,
    });
  };

  const emitReasoningDelta = (reasoningContent) => {
    if (typeof reasoningContent !== "string" || config.reasoningOutput === "drop") {
      return;
    }
    if (config.reasoningOutput === "plain") {
      emitOutputDelta(reasoningContent);
      return;
    }
    const prefix = reasoningTagOpen ? "" : "<think>\n";
    reasoningTagOpen = true;
    emitOutputDelta(`${prefix}${reasoningContent}`);
  };

  const closeReasoningTag = () => {
    if (!reasoningTagOpen || config.reasoningOutput !== "tagged") {
      return;
    }
    reasoningTagOpen = false;
    emitOutputDelta("\n</think>\n\n");
  };

  const completeMessage = () => {
    if (!messageItem || messageItem.status === "completed") {
      return;
    }
    closeReasoningTag();
    const completedItem = {
      ...messageItem,
      status: "completed",
      content: [
        {
          type: "output_text",
          text: messageText,
          annotations: [],
        },
      ],
    };
    writeEvent({
      type: "response.output_text.done",
      item_id: completedItem.id,
      output_index: messageOutputIndex,
      content_index: 0,
      text: messageText,
    });
    writeEvent({
      type: "response.content_part.done",
      item_id: completedItem.id,
      output_index: messageOutputIndex,
      content_index: 0,
      part: completedItem.content[0],
    });
    writeEvent({
      type: "response.output_item.done",
      output_index: messageOutputIndex,
      item: completedItem,
    });
    messageItem = completedItem;
    output[messageOutputIndex] = completedItem;
  };

  const ensureToolCall = (toolCall, fallbackIndex) => {
    const key = String(toolCall.index ?? fallbackIndex ?? toolCalls.size);
    let state = toolCalls.get(key);
    if (!state) {
      const callId = toolCall.id || `call_${randomId()}`;
      state = {
        outputIndex: null,
        item: {
          id: callId,
          type: "function_call",
          status: "in_progress",
          call_id: callId,
          name: toolCall.function?.name || "",
          arguments: "",
        },
        pendingArguments: "",
        emitted: false,
      };
      toolCalls.set(key, state);
    }

    if (toolCall.id && !state.item.call_id) {
      state.item.call_id = toolCall.id;
      state.item.id = toolCall.id;
    }
    if (toolCall.function?.name) {
      state.item.name = toolCall.function.name;
    }
    const argumentDelta = toolCall.function?.arguments || "";
    if (argumentDelta) {
      state.pendingArguments += argumentDelta;
    }

    if (!state.emitted && state.item.name) {
      state.outputIndex = nextOutputIndex;
      nextOutputIndex += 1;
      writeEvent({
        type: "response.output_item.added",
        output_index: state.outputIndex,
        item: state.item,
      });
      state.emitted = true;
    }

    if (state.emitted && state.pendingArguments) {
      writeEvent({
        type: "response.function_call_arguments.delta",
        item_id: state.item.id,
        output_index: state.outputIndex,
        delta: state.pendingArguments,
      });
      state.item.arguments += state.pendingArguments;
      state.pendingArguments = "";
    }

    return state;
  };

  const completeToolCalls = () => {
    for (const state of toolCalls.values()) {
      if (!state.emitted) {
        state.item.name ||= "unknown_tool";
        state.outputIndex = nextOutputIndex;
        nextOutputIndex += 1;
        writeEvent({
          type: "response.output_item.added",
          output_index: state.outputIndex,
          item: state.item,
        });
        state.emitted = true;
      }
      if (state.pendingArguments) {
        writeEvent({
          type: "response.function_call_arguments.delta",
          item_id: state.item.id,
          output_index: state.outputIndex,
          delta: state.pendingArguments,
        });
        state.item.arguments += state.pendingArguments;
        state.pendingArguments = "";
      }
      if (state.item.status === "completed") {
        continue;
      }
      const completedItem = { ...state.item, status: "completed" };
      writeEvent({
        type: "response.function_call_arguments.done",
        item_id: completedItem.id,
        output_index: state.outputIndex,
        arguments: completedItem.arguments,
      });
      writeEvent({
        type: "response.output_item.done",
        output_index: state.outputIndex,
        item: completedItem,
      });
      state.item = completedItem;
      output[state.outputIndex] = completedItem;
    }
  };

  return {
    created() {
      writeEvent({
        type: "response.created",
        response: responseBase("in_progress"),
      });
    },

    consumeChatChunk(chunk) {
      if (chunk.usage) {
        finalUsage = mapUsage(chunk.usage);
      }

      for (const choice of normalizeArray(chunk.choices)) {
        const delta = choice.delta || {};
        emitReasoningDelta(delta.reasoning_content);

        if (typeof delta.content === "string" && delta.content) {
          closeReasoningTag();
          emitOutputDelta(delta.content);
        }

        for (const toolCall of normalizeArray(delta.tool_calls)) {
          ensureToolCall(toolCall);
        }
      }
    },

    completed() {
      if (completed) {
        return;
      }
      completeMessage();
      completeToolCalls();
      const responsePayload = {
        ...responseBase("completed"),
        usage: finalUsage || {
          input_tokens: 0,
          output_tokens: 0,
          total_tokens: 0,
        },
      };
      writeEvent({
        type: "response.completed",
        response: responsePayload,
      });
      response.write("data: [DONE]\n\n");
      completed = true;
    },

    failed(error) {
      const errorPayload = {
        message: error.message || "Upstream stream failed",
        type: "upstream_stream_error",
      };
      writeEvent({
        type: "error",
        error: errorPayload,
      });
      writeEvent({
        type: "response.failed",
        response: {
          ...responseBase("failed"),
          error: errorPayload,
        },
      });
    },
  };
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
      const data = parseSseData(rawEvent);
      if (data != null) {
        yield data;
      }
    }
  }

  buffer += decoder.decode();
  const data = parseSseData(buffer);
  if (data != null) {
    yield data;
  }
}

function parseSseData(rawEvent) {
  const dataLines = rawEvent
    .split(/\r?\n/)
    .filter((line) => line.startsWith("data:"))
    .map((line) => line.slice(5).trimStart());
  if (dataLines.length === 0) {
    return null;
  }
  return dataLines.join("\n");
}

function normalizeInstructions(instructions) {
  if (typeof instructions === "string") {
    return instructions;
  }
  if (Array.isArray(instructions)) {
    return instructions.map(itemToText).filter(Boolean).join("\n\n");
  }
  return "";
}

function contentToText(content) {
  if (typeof content === "string") {
    return content;
  }
  if (!Array.isArray(content)) {
    return itemToText(content);
  }
  return content.map(itemToText).filter(Boolean).join("\n\n");
}

function responseMessageToChatMessage(item) {
  const role = mapRole(item.role);
  const text = contentToText(item.content);
  if (role !== "assistant") {
    return { role, content: text };
  }

  const split = splitReasoningContent(text);
  if (!split.reasoningContent) {
    return { role, content: text };
  }

  return {
    role,
    content: split.content,
    reasoning_content: split.reasoningContent,
  };
}

function splitReasoningContent(text) {
  const reasoningParts = [];
  const content = text
    .replace(/<think>\s*([\s\S]*?)\s*<\/think>\s*/gi, (_match, reasoning) => {
      if (reasoning.trim()) {
        reasoningParts.push(reasoning.trim());
      }
      return "";
    })
    .trimStart();

  return {
    content,
    reasoningContent: reasoningParts.join("\n\n"),
  };
}

function outputToText(output) {
  if (typeof output === "string") {
    return output;
  }
  return contentToText(output);
}

function itemToText(item) {
  if (item == null) {
    return "";
  }
  if (typeof item === "string") {
    return item;
  }
  if (typeof item !== "object") {
    return String(item);
  }
  if (typeof item.text === "string") {
    return item.text;
  }
  if (typeof item.output_text === "string") {
    return item.output_text;
  }
  if (typeof item.output === "string") {
    return item.output;
  }
  if (item.type === "input_image" || item.type === "image") {
    return `[image: ${item.image_url || item.file_id || "omitted"}]`;
  }
  if (item.type === "input_file") {
    return `[file: ${item.filename || item.file_id || item.file_url || "omitted"}]`;
  }
  if (Array.isArray(item.content)) {
    return contentToText(item.content);
  }
  return JSON.stringify(item);
}

function mapRole(role) {
  if (role === "assistant" || role === "user" || role === "system") {
    return role;
  }
  if (role === "developer") {
    return "system";
  }
  if (role === "tool") {
    return "tool";
  }
  return "user";
}

function mapToolChoice(toolChoice) {
  if (!toolChoice || toolChoice === "auto") {
    return "auto";
  }
  if (toolChoice === "none" || toolChoice === "required") {
    return toolChoice;
  }
  if (toolChoice.type === "function" && toolChoice.name) {
    return {
      type: "function",
      function: { name: toolChoice.name },
    };
  }
  return "auto";
}

function mapUsage(usage = {}) {
  const inputTokens = usage.input_tokens ?? usage.prompt_tokens ?? 0;
  const outputTokens = usage.output_tokens ?? usage.completion_tokens ?? 0;
  return {
    input_tokens: inputTokens,
    output_tokens: outputTokens,
    total_tokens: usage.total_tokens ?? inputTokens + outputTokens,
  };
}

async function readJsonBody(request, maxBytes) {
  let total = 0;
  const chunks = [];
  for await (const chunk of request) {
    total += chunk.length;
    if (total > maxBytes) {
      const error = new Error(`Request body exceeds ${maxBytes} bytes`);
      error.statusCode = 413;
      throw error;
    }
    chunks.push(chunk);
  }
  const rawBody = Buffer.concat(chunks).toString("utf8");
  try {
    return rawBody ? JSON.parse(rawBody) : {};
  } catch (error) {
    error.statusCode = 400;
    error.message = "Request body is not valid JSON.";
    throw error;
  }
}

async function proxyUpstreamError(response, upstreamResponse) {
  const body = await upstreamResponse.text();
  const parsed = safeJsonParse(body);
  debugDeepSeekResponse("error", parsed || body);
  sendJson(response, upstreamResponse.status, {
    error: parsed?.error || {
      message: body || upstreamResponse.statusText,
      type: "upstream_error",
    },
  });
}

function upstreamUrl(pathname) {
  return `${config.deepseekBaseUrl}${pathname}`;
}

function upstreamHeaders(stream) {
  return {
    authorization: `Bearer ${config.deepseekApiKey}`,
    "content-type": "application/json",
    accept: stream ? "text/event-stream" : "application/json",
  };
}

function debugDeepSeekRequest(kind, url, body) {
  if (!config.debugUpstream) {
    return;
  }
  log("info", `DeepSeek request ${kind}`, {
    url,
    model: body.model,
    stream: body.stream,
    messages: body.messages?.length || 0,
    tools: body.tools?.length || 0,
  });
}

function debugDeepSeekResponseHeaders(kind, upstreamResponse) {
  if (!config.debugUpstream) {
    return;
  }
  log("info", `DeepSeek response headers ${kind}`, {
    status: upstreamResponse.status,
    statusText: upstreamResponse.statusText,
    contentType: upstreamResponse.headers.get("content-type"),
  });
}

function debugDeepSeekResponse(kind, payload) {
  if (!config.debugUpstream) {
    return;
  }
  log("info", `DeepSeek response ${kind}`, truncateForLog(payload, 12000));
}

function truncateForLog(payload, maxLength) {
  const text = typeof payload === "string" ? payload : JSON.stringify(payload);
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, maxLength)}...<truncated ${text.length - maxLength} chars>`;
}

function sendJson(response, statusCode, payload) {
  response.writeHead(statusCode, { "content-type": "application/json; charset=utf-8" });
  response.end(JSON.stringify(payload, null, 2));
}

function matchesEndpoint(pathname, endpoint) {
  return pathname === `/${endpoint}` || pathname === `/v1/${endpoint}`;
}

function isAuthorized(request, expectedToken) {
  const header = request.headers.authorization || "";
  const token = header.startsWith("Bearer ") ? header.slice("Bearer ".length) : "";
  return token === expectedToken;
}

function modelObject(id) {
  return {
    id,
    object: "model",
    created: 0,
    owned_by: "deepseek",
  };
}

function copyIfPresent(source, target, key) {
  if (source[key] != null) {
    target[key] = source[key];
  }
}

function normalizeArray(value) {
  return Array.isArray(value) ? value : [];
}

function safeJsonParse(value) {
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function trimTrailingSlash(value) {
  return value.replace(/\/+$/, "");
}

function parseInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function normalizeReasoningOutput(value) {
  if (value === "drop" || value === "plain") {
    return value;
  }
  return "tagged";
}

function isMainModule() {
  return process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url);
}

function randomId() {
  return Math.random().toString(36).slice(2, 12);
}

function log(level, message, details = undefined) {
  const order = ["debug", "info", "warn", "error"];
  if (order.indexOf(level) < order.indexOf(config.logLevel)) {
    return;
  }
  const payload = details ? ` ${JSON.stringify(details)}` : "";
  process.stderr.write(`[${new Date().toISOString()}] ${level.toUpperCase()} ${message}${payload}\n`);
}

function loadDotEnv(path) {
  if (!existsSync(path)) {
    return;
  }
  const lines = readFileSync(path, "utf8").split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }
    const separator = trimmed.indexOf("=");
    if (separator === -1) {
      continue;
    }
    const key = trimmed.slice(0, separator).trim();
    const rawValue = trimmed.slice(separator + 1).trim();
    if (!key || process.env[key] != null) {
      continue;
    }
    process.env[key] = unquoteEnvValue(rawValue);
  }
}

function unquoteEnvValue(value) {
  if (
    (value.startsWith('"') && value.endsWith('"')) ||
    (value.startsWith("'") && value.endsWith("'"))
  ) {
    return value.slice(1, -1);
  }
  return value;
}
