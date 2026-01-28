# Agent Guidelines

- Whenever making code changes, ALWAYS increment the version and annotate the CHANGELOG. However, wait until the last second to read in the VERSION file in case other agents are working in the folder. This prevents conflicting version increment operations.

- Auto-commit and push code after every code change, but ONLY after you increment VERSION and annotate CHANGELOG. In the notes, mention what coding agent you are and what model you are using. If you are Claude Code, you would say Claude:Opus 4.5 (if you are using the Opus 4.5 model). If you are Codex, you would say: Codex:gpt-5.1-codex-max-high (if high is the reasoning level).

- Stay out of the _studies, _proposals, _rcodegen, _bugs_open, _bugs_fixed directories. Do not go into them or read from them unless specifically told to do so.

- When you fix a bug, write short details on that bug and store it in _bugs_fixed. Depending on the severity or complexity, decide if you think you should be very brief - or less brief. Give your bug file a good name but always prepend the date. For example: 2026-12-31-failed-to-check-values-bug.md is a perfect name. Always lowercase. Always include the date in the filename.
