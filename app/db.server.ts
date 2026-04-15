import { PrismaClient } from "@prisma/client";

declare global {
  // eslint-disable-next-line no-var
  var prismaGlobal: PrismaClient | undefined;
  // eslint-disable-next-line no-var
  var prismaPragmasApplied: boolean | undefined;
}

// Serialize BigInt so Remix loaders can JSON-encode Prisma rows with BigInt columns
// (Session.userId is BigInt?). Without this, loaders crash on any row that carries one.
if (!(BigInt.prototype as any).toJSON) {
  (BigInt.prototype as any).toJSON = function () {
    return this.toString();
  };
}

// SQLite connection pool: WAL mode supports concurrent reads, but Prisma
// defaults to 1 connection. Bumping this lets the loaders run their parallel
// queries truly in parallel instead of serializing on a single connection.
function buildPrismaClient() {
  const baseUrl = process.env.DATABASE_URL || "file:./prisma/dev.sqlite";
  const sep = baseUrl.includes("?") ? "&" : "?";
  const url = baseUrl.includes("connection_limit=")
    ? baseUrl
    : `${baseUrl}${sep}connection_limit=8&socket_timeout=10`;
  return new PrismaClient({
    log: process.env.NODE_ENV === "development" ? ["warn", "error"] : ["error"],
    datasources: { db: { url } },
  });
}

const prisma = global.prismaGlobal ?? buildPrismaClient();

if (process.env.NODE_ENV !== "production") {
  global.prismaGlobal = prisma;
}

/**
 * SQLite PRAGMA tuning — applied once per process.
 *
 * Defaults are hilariously conservative for an analytics workload. These
 * switch us to WAL (readers don't block writers), bump the page cache from
 * 2MB to 64MB, enable 256MB of mmap reads, keep temp tables in RAM, and
 * relax fsync from FULL to NORMAL (fsync on checkpoints, not every write).
 *
 * Expected effect: 2–5x read speedup on the hot loaders, concurrent read +
 * hourly sync no longer blocks each other.
 */
async function applyPragmas() {
  if (global.prismaPragmasApplied) return;
  global.prismaPragmasApplied = true;
  // Use $queryRawUnsafe for ALL pragmas: some of them (journal_mode,
  // busy_timeout) return a row, which $executeRawUnsafe refuses. $queryRawUnsafe
  // handles both returning and non-returning statements safely.
  const pragmas = [
    "PRAGMA journal_mode = WAL",
    "PRAGMA synchronous = NORMAL",
    "PRAGMA cache_size = -131072",     // 128 MB page cache (4 GB VM has plenty of headroom)
    "PRAGMA mmap_size = 536870912",    // 512 MB memory-mapped reads
    "PRAGMA temp_store = MEMORY",
    "PRAGMA busy_timeout = 5000",
  ];
  for (const p of pragmas) {
    try {
      const result: any = await prisma.$queryRawUnsafe(p);
      console.log(`[db] ${p} → ${JSON.stringify(result)}`);
    } catch (err) {
      console.error(`[db] Failed: ${p}:`, (err as Error).message);
    }
  }
}
applyPragmas();

export default prisma;
