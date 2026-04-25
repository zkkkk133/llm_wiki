import { useWikiStore } from "@/stores/wiki-store"
import { enqueueIngest } from "./ingest-queue"
import { listDirectory } from "@/commands/fs"
import { normalizePath } from "@/lib/path-utils"

const POLL_INTERVAL = 3000 // Check every 3 seconds
let intervalId: ReturnType<typeof setInterval> | null = null

interface PendingAsk {
  id: string
  projectPath: string
  question: string
}

/**
 * Start polling the clip server for new web clips.
 * When a clip is detected, triggers auto-ingest and refreshes the file tree.
 */
export function startClipWatcher() {
  if (intervalId) return // Already running

  intervalId = setInterval(async () => {
    try {
      const store = useWikiStore.getState()
      const project = store.project

      const res = await fetch("http://127.0.0.1:19827/clips/pending", { method: "GET" })
      const data = await res.json()

      if (data.ok && data.clips && data.clips.length > 0) {
        for (const clip of data.clips) {
          const clipProjectPath: string = clip.projectPath
          const clipFilePath: string = clip.filePath

          // Refresh file tree if clip is for current project
          if (project && clipProjectPath === project.path) {
            try {
              const tree = await listDirectory(project.path)
              store.setFileTree(tree)
            } catch {
              // ignore
            }

            // Enqueue (not auto-ingest directly) so the task lands in the
            // persisted queue, shows up in the activity panel, and survives
            // a UI refresh. Same path used by file imports from sources-view.
            // Pass the project's stable UUID — the queue looks up the
            // current filesystem path from the registry at run time.
            const llmConfig = store.llmConfig
            const hasLlm =
              !!llmConfig.apiKey ||
              llmConfig.provider === "ollama" ||
              llmConfig.provider === "custom"
            if (hasLlm) {
              enqueueIngest(project.id, clipFilePath).catch((err) => {
                console.error("Failed to enqueue web clip:", err)
              })
            }
          }
        }
      }

      const asksRes = await fetch("http://127.0.0.1:19827/asks/pending", { method: "GET" })
      const asksData = await asksRes.json()
      if (asksData.ok && asksData.asks && asksData.asks.length > 0) {
        for (const ask of asksData.asks as PendingAsk[]) {
          answerPendingAsk(ask).catch((err) => {
            console.error("Failed to answer API question:", err)
          })
        }
      }
    } catch {
      // Server not running or network error — silently ignore
    }
  }, POLL_INTERVAL)
}

export function stopClipWatcher() {
  if (intervalId) {
    clearInterval(intervalId)
    intervalId = null
  }
}

async function answerPendingAsk(ask: PendingAsk): Promise<void> {
  const store = useWikiStore.getState()
  const project = store.project
  if (!project) {
    await postAskAnswer(ask.id, {
      ok: false,
      error: "No project is open in LLM Wiki",
    })
    return
  }

  if (normalizePath(ask.projectPath) !== normalizePath(project.path)) {
    await postAskAnswer(ask.id, {
      ok: false,
      error: `Requested project is not open: ${ask.projectPath}`,
    })
    return
  }

  try {
    const { answerWikiQuestion } = await import("@/lib/wiki-qa")
    const result = await answerWikiQuestion({
      project,
      question: ask.question,
      llmConfig: store.llmConfig,
      dataVersion: store.dataVersion,
    })
    await postAskAnswer(ask.id, {
      ok: true,
      answer: result.answer,
      references: result.references,
    })
  } catch (err) {
    await postAskAnswer(ask.id, {
      ok: false,
      error: err instanceof Error ? err.message : String(err),
    })
  }
}

async function postAskAnswer(
  id: string,
  payload:
    | { ok: true; answer: string; references: Array<{ title: string; path: string }> }
    | { ok: false; error: string },
): Promise<void> {
  await fetch("http://127.0.0.1:19827/asks/answer", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ id, ...payload }),
  })
}
