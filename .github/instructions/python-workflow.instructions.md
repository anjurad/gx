---
name: Python Workflow Guardrails
description: "Use when working on Python code, tests, scripts, or dependencies in this repo. Enforces .venv activation in terminal sessions, uv package management, and consulting the Python skill before Python tasks."
applyTo: "**/*.py"
---
# Python Workflow Guardrails

- These rules are hard requirements for Python work in this repository.
- Always consult `/workspaces/great-expectations/.github/skills/python/SKILL.md` before starting Python coding, reviews, tests, or dependency changes.
- Use `uv` as the Python package manager for this repository.
- Before running any Python command in the terminal, ensure `.venv` exists and activate it:
  - `if [ ! -d ".venv" ]; then uv venv; fi`
  - `source .venv/bin/activate`
- Prefer `uv` commands for dependency and execution workflows:
  - `uv add <package>`
  - `uv add --dev <package>`
  - `uv run <script-or-module>`
  - `uv sync`
- Do not install Python packages globally for this project.
- If a hard rule cannot be followed due to an external blocker, stop and ask the user before proceeding.
