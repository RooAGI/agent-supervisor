# Project planning template pack

This pack contains the pinned AgentFlow snapshots used by the Roo Desktop chat orchestration board:

- BoardTemplate `project@1` with required project goal and success criteria fields, plus the structured `task@1` post template.
- FlowTemplate `project-planning@1` with Planning, Execution, and Review stages. It references the exact `project@1` BoardTemplate hash.

The Flowboard MCP server bundles this pack. Call `flowboard_template_list` to discover it and `flowboard_template_get` with `template_id: "project_planning"` to retrieve both snapshots. Pass the returned `board_template` to `board_open` and the returned `flow_template` to `flow_create`. Task posts use `post_template_id: "task"` and `post_template_version: 1`.

Retrieving the pack does not create records. New boards and FlowRuns pin the selected version and its content hash. Publish changed definitions as a new version instead of editing snapshots already in use.
