// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 Vorssaint

import Foundation

typealias AgentUsageProductionLogReader = AgentLogReader

/// Runs the service's production read method, parser, cursor and store. The
/// reader wrapper only observes when a complete line is handed to the service.
enum AgentUsageReadTests {
    enum AgentLogReader {
        static var beforeLine: (() -> Void)?
        static func copilotHistoryLine(_ buffer: Data, range: Range<Int>) -> Bool {
            AgentUsageProductionLogReader.copilotHistoryLine(buffer, range: range)
        }
        static func readCopilotHistory(_ cursor: AgentLogCursor, shouldContinue: () -> Bool,
                                       line: (Data) -> Void) {
            AgentUsageProductionLogReader.readCopilotHistory(cursor, shouldContinue: shouldContinue) {
                beforeLine?()
                line($0)
            }
        }
        static func readAppended(_ cursor: AgentLogCursor, shouldContinue: () -> Bool,
                                 including: ((Data, Range<Int>) -> Bool)? = nil,
                                 line: (Data) -> Void) {
            AgentUsageProductionLogReader.readAppended(cursor, shouldContinue: shouldContinue,
                                                        including: including) {
                beforeLine?()
                line($0)
            }
        }
    }

    final class Cancellation {
        var isCancelled = false
    }

    class Fixture {
        var readerCancellation: Cancellation? = Cancellation()
        var cursors: [String: AgentLogCursor] = [:]
        let store = AgentUsageStore()
        var events: [AgentUsageEvent] = []
        func report(_ event: AgentUsageEvent) { events.append(event) }
    }

    static func run(_ suite: TestSuite) {
        let folder = FileManager.default.temporaryDirectory.appending(path: "vorss-streaming-\(UUID().uuidString)")
        defer {
            AgentLogReader.beforeLine = nil
            try? FileManager.default.removeItem(at: folder)
        }
        do { try FileManager.default.createDirectory(at: folder, withIntermediateDirectories: true) }
        catch { suite.expect(false, "the streaming fixture creates its folder: \(error)"); return }
        let now = Date()
        let timestamp = now.timeIntervalSince1970
        let cases: [(AgentProvider, [String])] = [
            (.claude, [
                #"{"type":"user","timestamp":\#(timestamp),"sessionId":"s","message":{"content":"work"}}"#,
                #"{"type":"assistant","timestamp":\#(timestamp),"sessionId":"s","requestId":"r","message":{"id":"m","model":"claude-opus-5-5","stop_reason":"tool_use","usage":{"input_tokens":10,"output_tokens":2}}}"#,
                #"{"type":"assistant","timestamp":\#(timestamp),"sessionId":"s","requestId":"r","message":{"id":"m","model":"claude-opus-5-5","stop_reason":"tool_use","usage":{"input_tokens":10,"output_tokens":5}}}"#,
                #"{"type":"assistant","timestamp":\#(timestamp),"sessionId":"s","requestId":"r2","message":{"id":"m2","model":"claude-opus-5-5","stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":7}}}"#
            ]),
            (.codex, [
                #"{"type":"session_meta","timestamp":\#(timestamp),"payload":{"id":"s","cwd":"/tmp/example"}}"#,
                #"{"type":"turn_context","timestamp":\#(timestamp),"payload":{"model":"gpt-5.2-codex"}}"#,
                #"{"type":"event_msg","timestamp":\#(timestamp),"payload":{"type":"task_started"}}"#,
                #"{"type":"token_usage_record","timestamp":\#(timestamp),"payload":{"response_id":"r","usage":{"input_tokens":10,"output_tokens":2}}}"#,
                #"{"type":"token_usage_record","timestamp":\#(timestamp),"payload":{"response_id":"r","usage":{"input_tokens":10,"output_tokens":5}}}"#,
                #"{"type":"event_msg","timestamp":\#(timestamp),"payload":{"type":"token_count","rate_limits":{"plan_type":"pro","primary":{"used_percent":42,"window_minutes":300}}}}"#,
                #"{"type":"event_msg","timestamp":\#(timestamp),"payload":{"type":"task_complete","duration_ms":20000}}"#
            ]),
            (.copilot, [
                #"{"id":"start","timestamp":\#(timestamp),"type":"session.start","data":{"sessionId":"s","selectedModel":"gpt-6-sol","context":{"cwd":"/tmp/example"}}}"#,
                #"{"id":"turn","timestamp":\#(timestamp),"type":"user.message","data":{"turnId":"0","content":"private"}}"#,
                #"{"id":"message","timestamp":\#(timestamp),"type":"assistant.message","data":{"model":"gpt-6-sol","content":"private"}}"#,
                #"{"id":"end","timestamp":\#(timestamp),"type":"session.usage_checkpoint","data":{"totalPremiumRequests":1}}"#,
                #"{"id":"usage","timestamp":\#(timestamp),"type":"session.shutdown","data":{"modelMetrics":{"gpt-6-sol":{"requests":{"count":1},"tokenDetails":{"input":{"tokenCount":10},"cache_read":{"tokenCount":20},"cache_write":{"tokenCount":0},"output":{"tokenCount":5}},"usage":{"reasoningTokens":2}}}}}"#,
                #"{"id":"checkpoint","timestamp":\#(timestamp),"type":"session.usage_checkpoint","data":{}}"#
            ])
        ]
        for (provider, lines) in cases {
            // A canonical filename, not a Codex side-thread filename.
            let file = folder.appending(path: "\(provider.rawValue).jsonl")
            do { try Data((lines.joined(separator: "\n") + "\n").utf8).write(to: file) }
            catch { suite.expect(false, "the streaming fixture writes its log: \(error)"); continue }

            let cursor = AgentLogCursor(path: file.path, provider: provider)
            var entries: [AgentLogEntry] = []
            let consume: (Data) -> Void = { line in
                switch provider {
                case .claude: entries += AgentLogParser.parseClaude(line, state: &cursor.state, now: now)
                case .codex: entries += AgentLogParser.parseCodex(line, state: &cursor.state, now: now)
                case .copilot: entries += AgentLogParser.parseCopilot(line, state: &cursor.state, now: now)
                }
            }
            if provider == .copilot {
                AgentUsageProductionLogReader.readCopilotHistory(cursor, line: consume)
            } else {
                AgentUsageProductionLogReader.readAppended(cursor, line: consume)
            }
            let reference = AgentUsageStore()
            reference.reportsTransitions = true
            let expectedEvents = reference.apply(entries, file: file.path, provider: provider,
                                                 tracksTurns: cursor.tracksTurns, parent: cursor.parent,
                                                 modified: cursor.modified, now: now)
            let host = Host()
            host.store.reportsTransitions = true
            var counts: [Int] = []
            AgentLogReader.beforeLine = { counts.append(host.store.records.count) }
            suite.expect(host.read(file.path, provider: provider), "a \(provider.rawValue) log reports parsed entries")
            AgentLogReader.beforeLine = nil
            suite.expect(counts.contains(where: { $0 > 0 }),
                         "\(provider.rawValue) records are applied before the rest of the log is read")
            suite.expect(host.store.records == reference.records && host.store.turns == reference.turns
                            && host.store.waiting == reference.waiting && host.store.limits == reference.limits
                            && host.store.codexPlan == reference.codexPlan && host.events == expectedEvents,
                         "streaming \(provider.rawValue) preserves duplicate merging, usage, turns, limits, plans and event order")
            suite.expect((provider == .copilot || !expectedEvents.isEmpty)
                            && host.cursors[file.path]?.state == cursor.state,
                         "\(provider.rawValue) retains the same parser context without replaying historical finishes")
            suite.expect(!host.read(file.path, provider: provider) && host.events == expectedEvents,
                         "an unchanged \(provider.rawValue) file neither changes the store nor replays events")
            host.readerCancellation?.isCancelled = true
            suite.expect(!host.read(file.path, provider: provider), "a cancelled reading consumes no more entries")
        }

        let openFile = folder.appending(path: "copilot-open.jsonl")
        let openLines = [
            #"{"id":"start","timestamp":"2026-09-27T15:00:00.000Z","type":"session.start","data":{"sessionId":"open","selectedModel":"gpt-6-sol","context":{"cwd":"/tmp/open-project"}}}"#,
            #"{"id":"turn","timestamp":"2026-09-27T15:01:00.000Z","type":"user.message","data":{"content":"still working"}}"#,
            #"{"id":"reply","timestamp":"2026-09-27T15:01:30.000Z","type":"assistant.message","data":{"model":"gpt-6-sol","content":"in progress"}}"#
        ]
        try? Data((openLines.joined(separator: "\n") + "\n").utf8).write(to: openFile)
        let openHost = Host()
        suite.expect(openHost.read(openFile.path, provider: .copilot)
                        && openHost.store.turns[openFile.path]?.project == "open-project"
                        && openHost.store.turns[openFile.path]?.model == "gpt-6-sol"
                        && openHost.cursors[openFile.path]?.state.turnOpen == true,
                     "an existing Copilot log restores the currently open turn on startup")
        if let handle = try? FileHandle(forWritingTo: openFile) {
            _ = try? handle.seekToEnd()
            try? handle.write(contentsOf: Data((#"{"id":"end","timestamp":"2026-09-27T15:02:00.000Z","type":"session.usage_checkpoint","data":{"totalPremiumRequests":0}}"# + "\n").utf8))
            try? handle.close()
        }
        openHost.store.reportsTransitions = true
        suite.expect(openHost.read(openFile.path, provider: .copilot)
                        && openHost.cursors[openFile.path]?.state.turnOpen == false
                        && openHost.store.turns[openFile.path] == nil,
                     "the restored Copilot turn finishes when its next checkpoint arrives")
    }
}
