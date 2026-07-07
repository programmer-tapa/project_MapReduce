// Package streaming manages Hadoop-style subprocess execution for
// language-agnostic Map and Reduce operations (Phase 1.1 of roadmap).
//
// Instead of Go plugins (.so files), the worker spawns an external process
// (Python, Node.js, Bash, etc.) and communicates via stdin/stdout pipes:
//
//	Map:    input lines → stdin → subprocess → stdout → tab-separated key\tvalue\n
//	Reduce: key\tvalue lines → stdin → subprocess → stdout → key\tresult\n
//
// This replaces the fragile, platform-specific Go plugin system with a
// cross-platform, polyglot-friendly mechanism.
package streaming
