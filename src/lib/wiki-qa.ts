import type { LlmConfig } from "@/stores/wiki-store"
import type { WikiProject } from "@/types/wiki"
import { readFile } from "@/commands/fs"
import { streamChat, type ChatMessage as LLMMessage } from "@/lib/llm-client"
import { searchWiki } from "@/lib/search"
import { buildRetrievalGraph, getRelatedNodes } from "@/lib/graph-relevance"
import { computeContextBudget } from "@/lib/context-budget"
import { getOutputLanguage, buildLanguageReminder } from "@/lib/output-language"
import { isGreeting } from "@/lib/greeting-detector"
import { normalizePath, getFileName, getRelativePath } from "@/lib/path-utils"

export interface WikiQaReference {
  title: string
  path: string
}

export interface WikiQaResult {
  answer: string
  references: WikiQaReference[]
}

interface WikiQaOptions {
  project: WikiProject
  question: string
  llmConfig: LlmConfig
  dataVersion?: number
  signal?: AbortSignal
}

export async function answerWikiQuestion({
  project,
  question,
  llmConfig,
  dataVersion = 0,
  signal,
}: WikiQaOptions): Promise<WikiQaResult> {
  const hasLlm =
    !!llmConfig.apiKey ||
    llmConfig.provider === "ollama" ||
    llmConfig.provider === "custom"
  if (!hasLlm || !llmConfig.model) {
    throw new Error("LLM provider is not configured")
  }

  const { messages, references } = await buildWikiQuestionMessages(
    project,
    question,
    llmConfig,
    dataVersion,
  )

  let answer = ""
  let streamError: Error | null = null
  await streamChat(
    llmConfig,
    messages,
    {
      onToken: (token) => {
        answer += token
      },
      onDone: () => {},
      onError: (err) => {
        streamError = err
      },
    },
    signal,
  )

  if (streamError) throw streamError
  return { answer, references }
}

async function buildWikiQuestionMessages(
  project: WikiProject,
  question: string,
  llmConfig: LlmConfig,
  dataVersion: number,
): Promise<{ messages: LLMMessage[]; references: WikiQaReference[] }> {
  const systemMessages: LLMMessage[] = []
  let references: WikiQaReference[] = []
  let langReminder: string | undefined

  if (isGreeting(question)) {
    const outLang = getOutputLanguage(question)
    systemMessages.push({
      role: "system",
      content: [
        `You are a wiki assistant for the project "${project.name}".`,
        "The user sent a casual greeting. Reply briefly and naturally, in one or two sentences.",
        "Do not invent wiki content or pretend to have retrieved pages.",
        "",
        `Respond in ${outLang}.`,
      ].join("\n"),
    })
  } else {
    const pp = normalizePath(project.path)
    const {
      indexBudget: INDEX_BUDGET,
      pageBudget: PAGE_BUDGET,
      maxPageSize: MAX_PAGE_SIZE,
    } = computeContextBudget(llmConfig.maxContextSize)

    const [rawIndex, purpose] = await Promise.all([
      readFile(`${pp}/wiki/index.md`).catch(() => ""),
      readFile(`${pp}/purpose.md`).catch(() => ""),
    ])

    const searchResults = await searchWiki(pp, question)
    const topSearchResults = searchResults.slice(0, 10)

    let index = rawIndex
    if (rawIndex.length > INDEX_BUDGET) {
      const { tokenizeQuery } = await import("@/lib/search")
      const tokens = tokenizeQuery(question)
      const lines = rawIndex.split("\n")
      const keptLines: string[] = []
      let keptSize = 0

      for (const line of lines) {
        const isHeader = line.startsWith("##")
        const lower = line.toLowerCase()
        const isRelevant = tokens.some((t) => lower.includes(t))
        if (isHeader || isRelevant) {
          if (keptSize + line.length + 1 <= INDEX_BUDGET) {
            keptLines.push(line)
            keptSize += line.length + 1
          }
        }
      }

      index = keptLines.join("\n")
      if (index.length < rawIndex.length) {
        index += "\n\n[...index trimmed to relevant entries...]"
      }
    }

    const graph = await buildRetrievalGraph(pp, dataVersion)
    const expandedIds = new Set<string>()
    const searchHitPaths = new Set(topSearchResults.map((r) => r.path))
    const graphExpansions: { title: string; path: string; relevance: number }[] = []

    for (const result of topSearchResults) {
      const fileName = getFileName(result.path)
      const nodeId = fileName.replace(/\.md$/, "")
      const related = getRelatedNodes(nodeId, graph, 3)
      for (const { node, relevance } of related) {
        if (relevance < 2.0) continue
        if (searchHitPaths.has(node.path)) continue
        if (expandedIds.has(node.id)) continue
        expandedIds.add(node.id)
        graphExpansions.push({ title: node.title, path: node.path, relevance })
      }
    }
    graphExpansions.sort((a, b) => b.relevance - a.relevance)

    let usedChars = 0
    type PageEntry = { title: string; path: string; content: string }
    const relevantPages: PageEntry[] = []

    const tryAddPage = async (title: string, filePath: string): Promise<boolean> => {
      if (usedChars >= PAGE_BUDGET) return false
      try {
        const raw = await readFile(filePath)
        const relativePath = getRelativePath(filePath, pp)
        const truncated =
          raw.length > MAX_PAGE_SIZE
            ? raw.slice(0, MAX_PAGE_SIZE) + "\n\n[...truncated...]"
            : raw
        if (usedChars + truncated.length > PAGE_BUDGET) return false
        usedChars += truncated.length
        relevantPages.push({ title, path: relativePath, content: truncated })
        return true
      } catch {
        return false
      }
    }

    for (const r of topSearchResults.filter((r) => r.titleMatch)) {
      await tryAddPage(r.title, r.path)
    }
    for (const r of topSearchResults.filter((r) => !r.titleMatch)) {
      await tryAddPage(r.title, r.path)
    }
    for (const exp of graphExpansions) {
      await tryAddPage(exp.title, exp.path)
    }
    if (relevantPages.length === 0) {
      await tryAddPage("Overview", `${pp}/wiki/overview.md`)
    }

    const pagesContext =
      relevantPages.length > 0
        ? relevantPages
            .map((p, i) => `### [${i + 1}] ${p.title}\nPath: ${p.path}\n\n${p.content}`)
            .join("\n\n---\n\n")
        : "(No wiki pages found)"

    const pageList = relevantPages
      .map((p, i) => `[${i + 1}] ${p.title} (${p.path})`)
      .join("\n")
    const outLang = getOutputLanguage(question)

    systemMessages.push({
      role: "system",
      content: [
        "You are a knowledgeable wiki assistant. Answer questions based on the wiki content provided below.",
        "",
        "## Rules",
        "- Answer based only on the numbered wiki pages provided below.",
        "- If the provided pages do not contain enough information, say so honestly.",
        "- Use [[wikilink]] syntax to reference wiki pages.",
        "- When citing information, use the page number in brackets, e.g. [1], [2].",
        "- At the very end of your response, add a hidden comment listing which page numbers you used:",
        "  <!-- cited: 1, 3, 5 -->",
        "",
        "Use markdown formatting for clarity.",
        "",
        purpose ? `## Wiki Purpose\n${purpose}` : "",
        index ? `## Wiki Index\n${index}` : "",
        relevantPages.length > 0 ? `## Page List\n${pageList}` : "",
        `## Wiki Pages\n\n${pagesContext}`,
        "",
        "---",
        "",
        `## Mandatory output language: ${outLang}`,
        "",
        `You must write your entire response in ${outLang}.`,
        `The wiki content above may be in a different language, but write in ${outLang} only.`,
      ]
        .filter(Boolean)
        .join("\n"),
    })

    langReminder = buildLanguageReminder(question)
    references = relevantPages.map((p) => ({ title: p.title, path: p.path }))
  }

  const finalQuestion = langReminder
    ? `[${langReminder}]\n\n${question}`
    : question

  return {
    messages: [...systemMessages, { role: "user", content: finalQuestion }],
    references,
  }
}
