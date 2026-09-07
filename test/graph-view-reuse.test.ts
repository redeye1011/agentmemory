import { describe, it, expect, beforeEach, vi } from "vitest";

vi.mock("../src/logger.js", () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

import { GraphRetrieval } from "../src/functions/graph-retrieval.js";
import { onGraphWrite, invalidateGraphCache } from "../src/state/graph-cache.js";
import type { GraphNode, GraphEdge } from "../src/types.js";

function makeNode(
  id: string,
  name: string,
  obsIds: string[] = ["obs_1"],
): GraphNode {
  return {
    id,
    type: "concept",
    name,
    properties: {},
    sourceObservationIds: obsIds,
    createdAt: "2026-02-01T10:00:00Z",
  };
}

function makeEdge(id: string, source: string, target: string): GraphEdge {
  return {
    id,
    type: "related_to",
    sourceNodeId: source,
    targetNodeId: target,
    weight: 0.8,
    sourceObservationIds: ["obs_1"],
    createdAt: "2026-02-01T10:00:00Z",
  };
}

/** Counting KV so the test can assert how often the graph is enumerated. */
function countingKV(nodes: GraphNode[], edges: GraphEdge[]) {
  const store = new Map<string, Map<string, unknown>>();
  store.set("mem:graph:nodes", new Map(nodes.map((n) => [n.id, n])));
  store.set("mem:graph:edges", new Map(edges.map((e) => [e.id, e])));
  const listCalls: string[] = [];
  return {
    listCalls,
    get: async <T>(scope: string, key: string): Promise<T | null> =>
      (store.get(scope)?.get(key) as T) ?? null,
    set: async <T>(scope: string, key: string, data: T): Promise<T> => {
      if (!store.has(scope)) store.set(scope, new Map());
      store.get(scope)!.set(key, data);
      return data;
    },
    delete: async (scope: string, key: string): Promise<void> => {
      store.get(scope)?.delete(key);
    },
    list: async <T>(scope: string): Promise<T[]> => {
      listCalls.push(scope);
      const entries = store.get(scope);
      return entries ? (Array.from(entries.values()) as T[]) : [];
    },
  };
}

describe("graph view reuse (#1300)", () => {
  let kv: ReturnType<typeof countingKV>;
  let retrieval: GraphRetrieval;

  beforeEach(() => {
    kv = countingKV(
      [
        makeNode("gn_1", "docker", ["obs_1"]),
        makeNode("gn_2", "compose", ["obs_2"]),
        makeNode("gn_3", "registry", ["obs_3"]),
      ],
      [makeEdge("ge_1", "gn_1", "gn_2"), makeEdge("ge_2", "gn_2", "gn_3")],
    );
    retrieval = new GraphRetrieval(kv as never);
    invalidateGraphCache(kv as never);
  });

  // The regression: searchByEntities and expandFromChunks each ran
  // kv.list on both graph scopes, so a single hybrid query paid four
  // full enumerations and every query after it paid four more.
  it("enumerates the graph once across many queries", async () => {
    await retrieval.searchByEntities(["docker"], 2, 10);
    await retrieval.searchByEntities(["compose"], 2, 10);
    await retrieval.expandFromChunks(["obs_1"], 1, 5);
    await retrieval.searchByEntities(["registry"], 2, 10);

    // One nodes list + one edges list, for all four calls.
    expect(kv.listCalls).toEqual(["mem:graph:nodes", "mem:graph:edges"]);
  });

  it("still returns results for each query", async () => {
    const docker = await retrieval.searchByEntities(["docker"], 2, 10);
    const registry = await retrieval.searchByEntities(["registry"], 2, 10);
    expect(docker.length).toBeGreaterThan(0);
    expect(registry.length).toBeGreaterThan(0);
  });

  // Writes go through StateKV, which patches the view in place. A node
  // added after the view was built must be reachable without paying for
  // another enumeration.
  it("sees nodes written after the view was built, without re-enumerating", async () => {
    await retrieval.searchByEntities(["docker"], 2, 10);
    const before = kv.listCalls.length;

    const fresh = makeNode("gn_4", "kubernetes", ["obs_9"]);
    await kv.set("mem:graph:nodes", fresh.id, fresh);
    onGraphWrite(kv as never, "mem:graph:nodes", fresh.id, fresh);

    const hits = await retrieval.searchByEntities(["kubernetes"], 2, 10);
    expect(hits.some((r) => r.obsId === "obs_9")).toBe(true);
    expect(kv.listCalls.length).toBe(before);
  });

  it("drops a node from the view when it is marked stale", async () => {
    await retrieval.searchByEntities(["docker"], 2, 10);

    const stale = { ...makeNode("gn_1", "docker", ["obs_1"]), stale: true };
    onGraphWrite(kv as never, "mem:graph:nodes", stale.id, stale);

    const hits = await retrieval.searchByEntities(["docker"], 2, 10);
    expect(hits.some((r) => r.obsId === "obs_1")).toBe(false);
  });

  it("rebuilds after an explicit invalidation", async () => {
    await retrieval.searchByEntities(["docker"], 2, 10);
    const before = kv.listCalls.length;
    invalidateGraphCache(kv as never);
    await retrieval.searchByEntities(["docker"], 2, 10);
    expect(kv.listCalls.length).toBe(before + 2);
  });
});
