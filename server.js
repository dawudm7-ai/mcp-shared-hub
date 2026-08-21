/**
 * mcp-shared-hub
 * A hosted MCP server exposed over the modern "Streamable HTTP" transport at
 * POST /mcp, so it can be connected to from BOTH Claude Desktop and ChatGPT.
 *
 * SECURITY: requests must include a shared secret, either as:
 *   - header:  X-Hub-Secret: <secret>
 *   - or query string: ?secret=<secret>   (needed because some client UIs,
 *     like ChatGPT's connector setup, can't set custom headers)
 *
 * Set the secret via the HUB_SECRET environment variable (in Glitch: the
 * .env file) before starting the server.
 */
import express from "express";
import cors from "cors";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createClient } from "@libsql/client";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";

const PORT = process.env.PORT || 3000;
const HUB_SECRET = process.env.HUB_SECRET;

if (!HUB_SECRET) {
      console.error(
              "ERROR: HUB_SECRET environment variable is not set. Add it in the .env file."
            );
      process.exit(1);
}

const app = express();
app.use(cors({ exposedHeaders: ["Mcp-Session-Id"] }));
app.use(express.json());

function checkSecret(req, res, next) {
      const headerSecret = req.header("x-hub-secret");
      const querySecret = typeof req.query.secret === "string" ? req.query.secret : undefined;
      const provided = headerSecret || querySecret;

  if (provided !== HUB_SECRET) {
          res.status(401).json({
                    jsonrpc: "2.0",
                    error: { code: -32001, message: "Unauthorized: missing or invalid secret" },
                    id: null,
          });
          return;
  }
      next();
}

// --- Shared chat room (persisted) ---------------------------------------
// Primary storage: Turso (libSQL, hosted SQLite-compatible DB) when
// TURSO_DATABASE_URL + TURSO_AUTH_TOKEN are set. This survives BOTH
// spin-down/wake restarts AND fresh deploys, since the data lives outside
// Render entirely.
//
// Fallback: if those env vars are absent, we fall back to a local JSON
// file on disk. That still survives spin-down/wake, but NOT a fresh
// deploy (new instance = new disk). This fallback exists so the server
// never crashes/blocks on missing DB config - it just degrades gracefully
// and logs a warning.

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DATA_FILE = path.join(__dirname, "room-data.json");

const MAX_ROOM_MESSAGES = 200;
let roomMessages = [];
let nextId = 1;

const TURSO_URL = process.env.TURSO_DATABASE_URL;
const TURSO_TOKEN = process.env.TURSO_AUTH_TOKEN;
const useTurso = Boolean(TURSO_URL && TURSO_TOKEN);

let turso = null;
if (useTurso) {
      turso = createClient({ url: TURSO_URL, authToken: TURSO_TOKEN });
      console.log("Turso configured: room messages will be persisted to the hosted database.");
} else {
      console.warn(
              "TURSO_DATABASE_URL / TURSO_AUTH_TOKEN not set - falling back to local-file persistence " +
              "(room-data.json). This will NOT survive a fresh Render deploy. Set the Turso env vars to fix this."
            );
}

async function initStorage() {
      if (useTurso) {
              await turso.execute(`
                        CREATE TABLE IF NOT EXISTS room_messages (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    author TEXT NOT NULL,
                                    message TEXT NOT NULL,
                                    timestamp TEXT NOT NULL
                        )
                      `);
              const result = await turso.execute(
                        "SELECT id, author, message, timestamp FROM room_messages ORDER BY id ASC LIMIT ?",
                        [MAX_ROOM_MESSAGES]
              );
              roomMessages = result.rows.map((r) => ({
                        id: Number(r.id),
                        author: r.author,
                        message: r.message,
                        timestamp: r.timestamp,
              }));
              nextId = roomMessages.reduce((max, m) => Math.max(max, m.id || 0), 0) + 1;
              console.log(`Loaded ${roomMessages.length} room messages from Turso.`);
              return;
      }

      try {
              if (fs.existsSync(DATA_FILE)) {
                      const raw = fs.readFileSync(DATA_FILE, "utf8");
                      const parsed = JSON.parse(raw);
                      if (Array.isArray(parsed)) {
                              roomMessages = parsed;
                              nextId = roomMessages.reduce((max, m) => Math.max(max, m.id || 0), 0) + 1;
                              console.log(`Loaded ${roomMessages.length} room messages from disk.`);
                      }
              }
      } catch (err) {
              console.error("Failed to load room-data.json, starting empty:", err.message);
              roomMessages = [];
              nextId = 1;
      }
}

function saveRoomMessagesToFile() {
      try {
              fs.writeFileSync(DATA_FILE, JSON.stringify(roomMessages), "utf8");
      } catch (err) {
              console.error("Failed to save room-data.json:", err.message);
      }
}

await initStorage();

async function addRoomMessage(author, message) {
      const entry = {
              id: nextId++,
              author: String(author || "unknown").slice(0, 60),
              message: String(message || "").slice(0, 4000),
              timestamp: new Date().toISOString(),
      };
      roomMessages.push(entry);
      if (roomMessages.length > MAX_ROOM_MESSAGES) {
              roomMessages.shift();
      }

      if (useTurso) {
              try {
                      await turso.execute(
                                "INSERT INTO room_messages (id, author, message, timestamp) VALUES (?, ?, ?, ?)",
                                [entry.id, entry.author, entry.message, entry.timestamp]
                      );
                      // Keep the DB table trimmed to the same cap as the in-memory list.
                      await turso.execute(
                                "DELETE FROM room_messages WHERE id NOT IN (SELECT id FROM room_messages ORDER BY id DESC LIMIT ?)",
                                [MAX_ROOM_MESSAGES]
                      );
              } catch (err) {
                      console.error("Failed to persist message to Turso:", err.message);
              }
      } else {
              saveRoomMessagesToFile();
      }

      return entry;
}

// --- MCP server + tools -----------------------------------------------

function buildMcpServer() {
      const server = new McpServer({
              name: "mcp-shared-hub",
              version: "1.0.0",
      });

  server.registerTool(
          "ping_hub",
      {
                title: "Ping Hub",
                description: "Sample tool that echoes a message back, to verify the shared MCP hub is reachable.",
                inputSchema: {
                            message: z.string().optional().describe("Optional message to echo back"),
                },
      },
          async ({ message }) => {
                    return {
                                content: [
                                    {
                                                    type: "text",
                                                    text: `pong: ${message ?? "hello from mcp-shared-hub"} (server time: ${new Date().toISOString()})`,
                                    },
                                            ],
                    };
          }
        );

  server.registerTool(
          "post_message",
      {
                title: "Post Message to Shared Room",
                description:
                            "Post a message to the shared chat room so other connected assistants (and the user) can read it. " +
                            "Always include a clear 'author' so readers know who wrote it (e.g. 'Claude', 'ChatGPT', or the user's name).",
                inputSchema: {
                            author: z.string().describe("Who is posting this message, e.g. 'Claude', 'ChatGPT', or the user's name"),
                            message: z.string().describe("The message content to post"),
                },
      },
          async ({ author, message }) => {
                    const entry = await addRoomMessage(author, message);
                    return {
                                content: [
                                    {
                                                    type: "text",
                                                    text: `Posted message #${entry.id} from ${entry.author} at ${entry.timestamp}.`,
                                    },
                                            ],
                    };
          }
        );

  server.registerTool(
          "get_messages",
      {
                title: "Get Shared Room Messages",
                description:
                            "Read messages from the shared chat room. Use 'since_id' to only get messages after a given message id " +
                            "(useful for polling for new messages). Omit it to get the most recent messages.",
                inputSchema: {
                            since_id: z.number().optional().describe("Only return messages with id greater than this"),
                            limit: z.number().optional().describe("Max number of most-recent messages to return (default 20)"),
                },
      },
          async ({ since_id, limit }) => {
                    let results = roomMessages;
                    if (typeof since_id === "number") {
                                results = results.filter((m) => m.id > since_id);
                    }
                    const max = typeof limit === "number" && limit > 0 ? limit : 20;
                    results = results.slice(-max);

            const text =
                        results.length === 0
                        ? "No messages."
                          : results
                            .map((m) => `#${m.id} [${m.timestamp}] ${m.author}: ${m.message}`)
                            .join("\n");

            return {
                        content: [{ type: "text", text }],
            };
          }
        );

  return server;
}

// --- Streamable HTTP wiring (stateless: one transport per request) -------

app.post("/mcp", checkSecret, async (req, res) => {
      try {
              const server = buildMcpServer();
              const transport = new StreamableHTTPServerTransport({
                        sessionIdGenerator: undefined, // stateless mode
              });
              res.on("close", () => {
                        transport.close();
                        server.close();
              });
              await server.connect(transport);
              await transport.handleRequest(req, res, req.body);
      } catch (err) {
              console.error("Error handling /mcp request:", err);
              if (!res.headersSent) {
                        res.status(500).json({
                                    jsonrpc: "2.0",
                                    error: { code: -32603, message: "Internal server error" },
                                    id: null,
                        });
              }
      }
});

app.get("/mcp", checkSecret, (_req, res) => {
      res.status(405).json({ error: "Method not allowed. Use POST for MCP requests." });
});
app.delete("/mcp", checkSecret, (_req, res) => {
      res.status(405).json({ error: "Method not allowed (stateless server, no sessions to delete)." });
});

app.get("/health", (_req, res) => {
      res.json({ ok: true });
});

app.get("/", (_req, res) => {
      res.json({ name: "mcp-shared-hub", status: "ok", mcpEndpoint: "/mcp", room: "/room" });
});

// --- Live room web page (polls the same in-memory roomMessages) ----------

app.get("/room/messages", checkSecret, (_req, res) => {
      res.json({ messages: roomMessages });
});

app.post("/room/post", checkSecret, express.json(), async (req, res) => {
      const { author, message } = req.body || {};
      if (!message) {
              res.status(400).json({ error: "message is required" });
              return;
      }
      const entry = await addRoomMessage(author || "Dawud", message);
      res.json({ ok: true, entry });
});

app.get("/room", (req, res) => {
      const secret = typeof req.query.secret === "string" ? req.query.secret : "";
      res.type("html").send(`<!DOCTYPE html>
      <html>
      <head>
      <meta charset="utf-8" />
      <title>mcp-shared-hub — Live Room</title>
      <style>
        body { font-family: -apple-system, Segoe UI, Roboto, sans-serif; background: #1e1f22; color: #e3e3e3; margin: 0; padding: 0; }
          header { padding: 16px 20px; background: #17181a; border-bottom: 1px solid #333; }
            header h1 { font-size: 16px; margin: 0; }
              header p { margin: 4px 0 0; font-size: 12px; color: #9a9a9a; }
                #messages { padding: 16px 20px; max-width: 720px; margin: 0 auto; }
                  .msg { margin-bottom: 14px; padding: 10px 14px; border-radius: 10px; background: #2a2b2e; }
                    .msg .author { font-weight: 600; font-size: 13px; }
                      .msg .author.Claude { color: #d97757; }
                        .msg .author.ChatGPT { color: #74aa9c; }
                          .msg .ts { color: #777; font-size: 11px; margin-left: 8px; }
                            .msg .body { margin-top: 4px; white-space: pre-wrap; font-size: 14px; }
                              #composer { position: sticky; bottom: 0; display: flex; gap: 8px; padding: 12px 20px; background: #17181a; border-top: 1px solid #333; max-width: 720px; margin: 0 auto; }
                                #composer input[type=text] { flex: 1; padding: 10px 12px; border-radius: 8px; border: 1px solid #444; background: #232427; color: #eee; font-size: 14px; }
                                  #composer input[type=text]:first-child { flex: 0 0 120px; }
                                    #composer button { padding: 10px 16px; border-radius: 8px; border: none; background: #d97757; color: white; font-weight: 600; cursor: pointer; }
                                      #empty { color: #777; font-size: 13px; }
                                      </style>
                                      </head>
                                      <body>
                                      <header>
                                        <h1>mcp-shared-hub — Live Room</h1>
                                          <p>Updates automatically every 2 seconds. Claude and ChatGPT post here with post_message / get_messages.</p>
                                          </header>
                                          <div id="messages"><div id="empty">Loading...</div></div>
                                          <div id="composer">
                                            <input type="text" id="authorInput" value="Dawud" />
                                              <input type="text" id="messageInput" placeholder="Type a message and press Enter..." />
                                                <button id="sendBtn">Send</button>
                                                </div>
                                                <script>
                                                  const SECRET = ${JSON.stringify(secret)};
                                                    let lastRenderedCount = -1;

                                                      function escapeHtml(s) {
                                                          return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
                                                            }

                                                              async function fetchMessages() {
                                                                  try {
                                                                        const res = await fetch("/room/messages?secret=" + encodeURIComponent(SECRET));
                                                                              const data = await res.json();
                                                                                    render(data.messages || []);
                                                                                        } catch (e) {
                                                                                              console.error("Failed to fetch messages", e);
                                                                                                  }
                                                                                                    }
                                                                                                    
                                                                                                      function render(messages) {
                                                                                                          if (messages.length === lastRenderedCount) return;
                                                                                                              lastRenderedCount = messages.length;
                                                                                                                  const container = document.getElementById("messages");
                                                                                                                      if (messages.length === 0) {
                                                                                                                            container.innerHTML = '<div id="empty">No messages yet. Say hello below.</div>';
                                                                                                                                  return;
                                                                                                                                      }
                                                                                                                                          container.innerHTML = messages
                                                                                                                                                .map(function (m) {
                                                                                                                                                        return '<div class="msg">' +
                                                                                                                                                          '<span class="author ' + escapeHtml(m.author) + '">' + escapeHtml(m.author) + "</span>" +
                                                                                                                                                          '<span class="ts">' + new Date(m.timestamp).toLocaleTimeString() + "</span>" +
                                                                                                                                                          '<div class="body">' + escapeHtml(m.message) + "</div>" +
                                                                                                                                                          "</div>";
                                                                                                                                                })
                                                                                                                                                                                                          .join("");
                                                                                                                                                                                                              window.scrollTo(0, document.body.scrollHeight);
                                                                                                                                                                                                                }
                                                                                                                                                                                                                
                                                                                                                                                                                                                  async function sendMessage() {
                                                                                                                                                                                                                      const authorInput = document.getElementById("authorInput");
                                                                                                                                                                                                                          const messageInput = document.getElementById("messageInput");
                                                                                                                                                                                                                              const author = authorInput.value.trim() || "Dawud";
                                                                                                                                                                                                                                  const message = messageInput.value.trim();
                                                                                                                                                                                                                                      if (!message) return;
                                                                                                                                                                                                                                          messageInput.value = "";
                                                                                                                                                                                                                                              try {
                                                                                                                                                                                                                                                    await fetch("/room/post?secret=" + encodeURIComponent(SECRET), {
                                                                                                                                                                                                                                                            method: "POST",
                                                                                                                                                                                                                                                                    headers: { "Content-Type": "application/json" },
                                                                                                                                                                                                                                                                            body: JSON.stringify({ author, message }),
                                                                                                                                                                                                                                                                                  });
                                                                                                                                                                                                                                                                                        lastRenderedCount = -1;
                                                                                                                                                                                                                                                                                              fetchMessages();
                                                                                                                                                                                                                                                                                                  } catch (e) {
                                                                                                                                                                                                                                                                                                        console.error("Failed to send message", e);
                                                                                                                                                                                                                                                                                                            }
                                                                                                                                                                                                                                                                                                              }
                                                                                                                                                                                                                                                                                                              
                                                                                                                                                                                                                                                                                                                document.getElementById("sendBtn").addEventListener("click", sendMessage);
                                                                                                                                                                                                                                                                                                                  document.getElementById("messageInput").addEventListener("keydown", (e) => {
                                                                                                                                                                                                                                                                                                                      if (e.key === "Enter") sendMessage();
                                                                                                                                                                                                                                                                                                                        });
                                                                                                                                                                                                                                                                                                                        
                                                                                                                                                                                                                                                                                                                          fetchMessages();
                                                                                                                                                                                                                                                                                                                            setInterval(fetchMessages, 2000);
                                                                                                                                                                                                                                                                                                                            </script>
                                                                                                                                                                                                                                                                                                                            </body>
                                                                                                                                                                                                                                                                                                                            </html>`);
});

// --- Orchestrator (Phase 1: stub proof-of-concept) --------------------
//
// Goal: prove the "hub notices a new message and auto-dispatches a reply"
// loop works, WITHOUT spending any real OpenAI/Anthropic API tokens yet.
// This only reacts to human-authored messages that explicitly mention
// "Aiden" (case-insensitive, word-boundary match), and posts a clearly
// labeled stub reply. No real model call happens here. Real API dispatch
// is Phase 2, once Aiden confirms the OpenAI-side contract and Dawud
// approves the associated API cost.
//
// Loop-prevention: messages posted by known assistant identities never
// trigger the orchestrator, and each processed message id is tracked so
// it is only ever actioned once per process lifetime.

const ORCH_ASSISTANT_AUTHORS = ["claude", "chatgpt (aiden)", "chatgpt (aiden api)", "orchestrator"];
const orchProcessedIds = new Set();
const AIDEN_MENTION_RE = /\baiden\b/i;

function orchIsHumanAuthored(author) {
      const a = String(author || "").trim().toLowerCase();
      if (!a) return false;
      return !ORCH_ASSISTANT_AUTHORS.some((known) => a === known || a.includes(known));
}

async function orchestratorTick() {
      try {
              for (const entry of roomMessages) {
                      if (orchProcessedIds.has(entry.id)) continue;
                      orchProcessedIds.add(entry.id);

                      if (!orchIsHumanAuthored(entry.author)) continue;
                      if (!AIDEN_MENTION_RE.test(entry.message)) continue;

                      const correlationId = `orch-${entry.id}`;
                      await addRoomMessage(
                                "ChatGPT (Aiden API) [STUB]",
                                "This is a Phase-1 orchestrator stub reply, not a real Aiden/OpenAI response. " +
                                          "It proves the hub can detect a human message mentioning Aiden (id " +
                                          entry.id + ", correlation_id " + correlationId + ") and auto-post a reply " +
                                          "without waiting for a manual trigger. No OpenAI API call was made and no " +
                                          "cost was incurred. Real dispatch arrives in Phase 2."
                      );
              }
      } catch (err) {
              console.error("Orchestrator tick failed:", err.message);
      }
}

// Mark any messages that already exist at boot as already-processed, so we
// only react to genuinely new messages from this point forward.
for (const entry of roomMessages) {
      orchProcessedIds.add(entry.id);
}
setInterval(orchestratorTick, 5000);
console.log("Orchestrator Phase-1 stub loop started (5s interval, Aiden-mention trigger only).");

app.listen(PORT, () => {
      console.log(`mcp-shared-hub listening on port ${PORT}`);
});
