# Reference-Code Workflow with XERJ

This project uses [XERJ](https://xerj.org/) as a local AI-native search engine to retrieve how peer open-source projects solved similar problems **before writing new code**.

## Prerequisites

- XERJ binary installed at `~/bin/xerj` (or on PATH)
- XERJ node running: `xerj --embed-mode lexical --data-dir .xerj-data`
- API key: `export XERJ_API_KEY="$(cat .xerj-data/admin.key)"`

## Indexed Corpora

| Prefix | Source | Description |
|--------|--------|-------------|
| `imageflow` | `.` | This project's source code and docs |
| `ref-controller-runtime` | `.refs/controller-runtime` | kubernetes-sigs/controller-runtime — the core operator framework |
| `ref-sample-controller` | `.refs/sample-controller` | kubernetes/sample-controller — official K8s operator example |
| `ref-kubebuilder` | `.refs/kubebuilder` | kubernetes-sigs/kubebuilder — operator scaffolding tool |

## How to Search Before Coding

### 1. Find how controller-runtime handles a pattern

```bash
xerj search --prefix ref-controller-runtime "reconcile loop error handling retry"
```

### 2. Look up a symbol definition

```bash
xerj def --prefix ref-controller-runtime Reconciler
```

### 3. Search across all reference repos

```bash
xerj search --prefix ref-* "status update conditions"
```

### 4. Search this project's own code

```bash
xerj search --prefix imageflow "import controller mapping TTL"
```

### 5. Filter by format (code vs docs)

```bash
# Code only
xerj search --prefix ref-controller-runtime "webhook validation" --filter ax_format=code

# Docs only
xerj search --prefix ref-controller-runtime "webhook validation" --filter ax_format=txt-prose
```

## Agent Workflow

When an agent is about to implement a feature:

1. **Identify the problem category** (e.g., "controller reconcile error handling", "CRD status conditions", "webhook admission control")
2. **Search reference repos first**: `xerj search --prefix ref-* "<problem description>"`
3. **Read the top results** — these show how established projects solved the same problem
4. **Adapt the pattern** to ImageFlow's conventions (see AGENTS.md for layout rules)
5. **Search own codebase** to avoid duplication: `xerj search --prefix imageflow "<feature name>"`

## Maintenance

### Re-index after pulling updates

```bash
xerj autoindex . --prefix imageflow --state-dir .xerj-state --yes
xerj autoindex .refs/controller-runtime --prefix ref-controller-runtime --state-dir .xerj-state/ref-cr --yes
```

### Add a new reference repo

```bash
git clone --depth 1 <repo-url> .refs/<name>
xerj autoindex .refs/<name> --prefix ref-<name> --code-analyzer code --state-dir .xerj-state/ref-<name> --yes
```

### Refresh reference repos periodically

```bash
cd .refs/controller-runtime && git pull && cd ../..
xerj autoindex .refs/controller-runtime --prefix ref-controller-runtime --state-dir .xerj-state/ref-cr --fresh --yes
```

## Node Management

Start the XERJ node (lexical mode, no external API key needed):

```bash
nohup ~/bin/xerj --embed-mode lexical --data-dir .xerj-data > .xerj-data/xerj.log 2>&1 &
```

For neural embeddings (better semantic search, requires HuggingFace model download):

```bash
nohup ~/bin/xerj --embed-mode neural --data-dir .xerj-data > .xerj-data/xerj.log 2>&1 &
```

Console: http://localhost:9200/_xerj-console/
