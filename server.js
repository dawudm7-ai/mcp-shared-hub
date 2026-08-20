/**
 * mcp-shared-hub
 * A local MCP server exposed over the modern "Streamable HTTP" transport at
 * POST /mcp, so it can be connected to from BOTH Claude Desktop and ChatGPT.
 *
 * SECURITY: requests must include a shared secret, either as:
 *   - header:  X-Hub-Secret: <secret>
 *   - or query string: ?secret=<secret>   (needed because some client UIs,
 *     like ChatGPT's connector setup, can't set custom headers)
 *
 * Set the secret via the HUB_SECRET environment variable before starting the
 * server. If HUB_SECRET is not set, the server refuses to start.
 */
import express, { Request, Response, NextFunction } from "express";
import cors from "cors";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";

const PORT = Number(process.env.PORT || 3000);
const HUB_SECRET = process.env.HUB_SECRET;

if (!HUB_SECRET) {
  console.error(
    "ERROR: HUB_SECRET environment variable is not set.\n" +
      "Set it before starting the server, e.g.:\n" +
      "  (Windows PowerShell)  $env:HUB_SECRET=\"your-long-random-secret\"; npx tsx server.ts\n" +
      "  (Windows cmd)          set HUB_SECRET=your-long-random-secret && npx tsx server.ts\n" +
      "  (bash/zsh)             HUB_SECRET=your-long-random-secret npx tsx server.ts"
  );
  process.exit(1);
}

const app = express();
app.use(cors({ exposedHeaders: ["Mcp-Session-Id"] }));
app.use(express.json());

function checkSecret(req: Request, res: Response, next: NextFunction) {
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

  return server;
}

// --- Streamable HTTP wiring (stateless: one transport per request) -------

app.post("/mcp", checkSecret, async (req: Request, res: Response) => {
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

// Stateless mode doesn't support server-initiated streams or session
// termination, but some clients probe GET/DELETE on the same URL — reply
// clearly instead of a generic 404.
app.get("/mcp", checkSecret, (_req: Request, res: Response) => {
  res.status(405).json({ error: "Method not allowed. Use POST for MCP requests." });
});
app.delete("/mcp", checkSecret, (_req: Request, res: Response) => {
  res.status(405).json({ error: "Method not allowed (stateless server, no sessions to delete)." });
});

app.get("/health", (_req: Request, res: Response) => {
  res.json({ ok: true });
});

app.listen(PORT, () => {
  console.log(`mcp-shared-hub listening on http://localhost:${PORT}`);
  console.log(`  MCP endpoint:      http://localhost:${PORT}/mcp?secret=...`);
  console.log(`  Health check:      http://localhost:${PORT}/health`);
});
