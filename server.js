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

// --- Shared chat room (in-memory) --------------------------------------
// NOTE: this resets whenever the free Render instance restarts/spins down
// after inactivity. It's a lightweight shared scratchpad, not durable storage.

const MAX_ROOM_MESSAGES = 200;
const roomMessages = [];

function addRoomMessage(author, message) {
    const entry = {
          id: roomMessages.length + 1,
          author: String(author || "unknown").slice(0, 60),
          message: String(message || "").slice(0, 4000),
          timestamp: new Date().toISOString(),
    };
    roomMessages.push(entry);
    if (roomMessages.length > MAX_ROOM_MESSAGES) {
          roomMessages.shift();
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
                const entry = addRoomMessage(author, message);
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
    res.json({ name: "mcp-shared-hub", status: "ok", mcpEndpoint: "/mcp" });
});

app.listen(PORT, () => {
    console.log(`mcp-shared-hub listening on port ${PORT}`);
});
