// Statechart view for GcBarrier under statechart/ThinPushRace.cfg: one
// thin push racing one barrier-mode GC chain. Two regions, the push and
// the GC, labelled in the words of someone who runs the server.

type V = { [name: string]: any };

const has = (xs: unknown, x: unknown) => Array.isArray(xs) && xs.includes(x);

const pushLabel = (v: V): string => {
  switch (v.push.st) {
    case "idle":
      return "No push yet";
    case "begun":
      return "Push open";
    case "looked":
      return "Bases found live, being read";
    case "uploaded":
      return "Pack uploaded, not live yet";
    case "committed":
      return v.push.cas ? "Ref moved to the new commit" : "Pack live, ref update lost";
    case "rejected":
      return "Refused, GC ran during the push";
    case "failed":
      return "Failed, a base was gone";
    default:
      return "Push timed out";
  }
};

const queued = (v: V) =>
  (Array.isArray(v.pending) && v.pending.length > 0) ||
  Object.values(v.fr as { [k: string]: number }).some((g) => g === 1);

const gcLabel = (v: V): string => {
  switch (v.phase) {
    case "walking":
      return queued(v) ? "Marking, push work queued" : "Marking";
    case "build":
      return "Copying marked objects";
    case "sweep":
      return queued(v) ? "Sweep marking late arrivals" : "Sweep ready to decide";
    default:
      if (v.chains === 0) return "No GC yet";
      return v.packs.P0 === "dead" ? "Old pack retired" : "Old pack kept";
  }
};

export const state = (v: V) => ({ Push: pushLabel(v), GC: gcLabel(v) });

export const step = (action: string, from: V, to: V): string | null => {
  switch (action) {
    case "PushBegin":
      return "the client opens a push";
    case "PushLookup":
      return to.push.st === "looked" ? "the server finds the bases live" : "a base is gone";
    case "PushUpload":
      return "the pack finishes uploading";
    case "PushCommit":
      if (to.push.st === "rejected") return "the commit is refused";
      return to.push.cas ? "the commit moves the ref" : "the commit loses the ref update";
    case "GcMarkInit":
      return "GC starts marking";
    case "GcPop":
    case "GcCommit":
      return "GC marks an object";
    case "GcPage":
      return "GC queues the pushed pack's objects";
    case "GcHandoff":
      return "marking ends";
    case "GcBeginBuild":
      return to.phase === "idle" ? "GC finds nothing to copy" : "GC starts the copy";
    case "GcBuildFinish":
      return "the copy goes live";
    case "GcSwap":
    case "GcFallback":
      return to.packs.P0 === "dead" ? "GC retires the old pack" : "GC keeps the old pack";
    case "JanitorReap":
    case "JanitorDelete":
      return "the janitor cleans up";
    case "Done":
      return "nothing left to do";
    default:
      return action;
  }
};

export const group = (v: V): string => {
  switch (v.phase) {
    case "walking":
      return "Marking";
    case "build":
      return "Copying";
    case "sweep":
      return "Sweeping";
    default:
      return v.chains === 0 ? "Before GC" : "After GC";
  }
};

export const colors = {
  "Before GC": "tan",
  Marking: "blue",
  Copying: "orange",
  Sweeping: "purple",
  "After GC": "teal",
};

export const title = "A thin push racing a barrier-mode GC chain";
