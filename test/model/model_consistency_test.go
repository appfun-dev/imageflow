package model

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var repoRoot = filepath.Clean(filepath.Join("..", ".."))

type ruleFile struct {
	Version            int                 `json:"version"`
	Status             string              `json:"status"`
	Rules              []architectureRule  `json:"rules"`
	ExistingViolations []existingViolation `json:"existingViolations"`
}

type existingViolation struct {
	RuleID    string `json:"ruleId"`
	Owner     string `json:"owner"`
	ExitIssue string `json:"exitIssue"`
}

type architectureRule struct {
	ID        string `json:"id"`
	Category  string `json:"category"`
	Statement string `json:"statement"`
	Scope     struct {
		ModelElements []string `json:"modelElements"`
		CodePaths     []string `json:"codePaths"`
	} `json:"scope"`
	Because      string `json:"because"`
	Severity     string `json:"severity"`
	Verification string `json:"verification"`
}

func readRepoFile(t *testing.T, name string) string {
	t.Helper()
	content, err := os.ReadFile(filepath.Join(repoRoot, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(content)
}

func readRules(t *testing.T) ruleFile {
	t.Helper()
	var rules ruleFile
	if err := json.Unmarshal([]byte(readRepoFile(t, "docs/model/architecture-rules.json")), &rules); err != nil {
		t.Fatalf("parse architecture rules: %v", err)
	}
	return rules
}

func mdslDocuments(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(repoRoot, "docs/model/contracts/*.mdsl"))
	if err != nil {
		t.Fatalf("find MDSL contracts: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("no MDSL contracts found")
	}
	documents := make([]string, 0, len(paths))
	for _, path := range paths {
		relative, err := filepath.Rel(repoRoot, path)
		if err != nil {
			t.Fatalf("make MDSL path repository-relative: %v", err)
		}
		documents = append(documents, relative)
	}
	return documents
}

func readMDSL(t *testing.T) string {
	t.Helper()
	var combined strings.Builder
	for _, document := range mdslDocuments(t) {
		combined.WriteString(readRepoFile(t, document))
		combined.WriteString("\n")
	}
	return combined.String()
}

func sortedValues(set map[string]bool) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

func assertSameSet(t *testing.T, label string, got, want map[string]bool) {
	t.Helper()
	gotValues := sortedValues(got)
	wantValues := sortedValues(want)
	if !reflect.DeepEqual(gotValues, wantValues) {
		t.Errorf("%s mismatch\n got: %v\nwant: %v", label, gotValues, wantValues)
	}
}

func matches(text string, expression *regexp.Regexp) []string {
	allMatches := expression.FindAllStringSubmatch(text, -1)
	values := make([]string, 0, len(allMatches))
	for _, match := range allMatches {
		values = append(values, match[1])
	}
	return values
}

func set(values []string) map[string]bool {
	result := make(map[string]bool, len(values))
	for _, value := range values {
		result[value] = true
	}
	return result
}

func rejectDuplicates(t *testing.T, label string, values []string) {
	t.Helper()
	seen := map[string]bool{}
	for _, value := range values {
		if seen[value] {
			t.Errorf("duplicate %s %q", label, value)
		}
		seen[value] = true
	}
}

func TestRequiredArtifactsAndMarkdownLinks(t *testing.T) {
	required := []string{
		"docs/agents/bizdevops-modeling.md",
		"docs/model/README.md",
		"docs/model/context-map.cml",
		"docs/model/contracts/imageflow-api.mdsl",
		"docs/model/diagrams/context-map.mmd",
		"docs/model/architecture-rules.json",
		"docs/model/review-template.md",
	}
	for _, name := range required {
		info, err := os.Stat(filepath.Join(repoRoot, name))
		if err != nil {
			t.Errorf("required model artifact %s: %v", name, err)
		} else if !info.Mode().IsRegular() {
			t.Errorf("required model artifact %s is not a regular file", name)
		}
	}

	documents := append([]string{"AGENTS.md"}, required[:2]...)
	documents = append(documents, "docs/model/review-template.md")
	linkPattern := regexp.MustCompile(`\[[^]]+\]\(([^)]+)\)`)
	for _, document := range documents {
		for _, rawTarget := range linkPattern.FindAllStringSubmatch(readRepoFile(t, document), -1) {
			target := strings.TrimSpace(rawTarget[1])
			isExternal := strings.Contains(target, "://") || strings.HasPrefix(target, "mailto:")
			if target == "" || strings.HasPrefix(target, "#") || isExternal {
				continue
			}
			// Repository links do not use optional Markdown titles or escaped spaces.
			target = strings.Trim(target, "<>")
			target = strings.Split(target, "#")[0]
			resolved := target
			if !filepath.IsAbs(resolved) {
				resolved = filepath.Join(repoRoot, filepath.Dir(document), resolved)
			}
			if _, err := os.Stat(filepath.Clean(resolved)); err != nil {
				t.Errorf("%s links to missing %q: %v", document, target, err)
			}
		}
	}
}

func TestModelStatusSynchronization(t *testing.T) {
	status := readRules(t).Status
	if status != "DRAFT" && status != "ACCEPTED" {
		t.Fatalf("unsupported model status %q", status)
	}
	checks := map[string]string{
		"docs/model/README.md":                fmt.Sprintf("**Current status: %s**", status),
		"docs/model/context-map.cml":          fmt.Sprintf("// Status: %s", status),
		"docs/model/diagrams/context-map.mmd": fmt.Sprintf("%%%% Status: %s", status),
	}
	for _, document := range mdslDocuments(t) {
		checks[document] = fmt.Sprintf("// Status: %s", status)
	}
	for name, marker := range checks {
		if !strings.Contains(readRepoFile(t, name), marker) {
			t.Errorf("%s does not carry status marker %q", name, marker)
		}
	}
}

func TestCMLContextMapMatchesMermaid(t *testing.T) {
	cml := readRepoFile(t, "docs/model/context-map.cml")
	mermaid := readRepoFile(t, "docs/model/diagrams/context-map.mmd")

	contexts := matches(cml, regexp.MustCompile(`(?m)^\s*BoundedContext\s+([A-Za-z_]\w*)`))
	nodes := matches(mermaid, regexp.MustCompile(`(?m)^\s*([A-Za-z_]\w*)\["`))
	rejectDuplicates(t, "CML bounded context", contexts)
	rejectDuplicates(t, "Mermaid node", nodes)
	assertSameSet(t, "CML/Mermaid context", set(nodes), set(contexts))

	cmlEdges := map[string]bool{}
	relationPattern := regexp.MustCompile(`(?m)^\s*([A-Za-z_]\w*)\s+\[[^]]+\]\s*->\s*\[[^]]+\]\s*([A-Za-z_]\w*)`)
	for _, match := range relationPattern.FindAllStringSubmatch(cml, -1) {
		cmlEdges[match[1]+"->"+match[2]] = true
	}
	mermaidEdges := map[string]bool{}
	edgePattern := regexp.MustCompile(`(?m)^\s*([A-Za-z_]\w*)\s+-->\s*(?:\|[^|]*\|\s*)?([A-Za-z_]\w*)`)
	for _, match := range edgePattern.FindAllStringSubmatch(mermaid, -1) {
		mermaidEdges[match[1]+"->"+match[2]] = true
	}
	assertSameSet(t, "CML/Mermaid upstream-downstream edges", mermaidEdges, cmlEdges)
}

func cmlAggregateRoots(t *testing.T) map[string]bool {
	t.Helper()
	cml := readRepoFile(t, "docs/model/context-map.cml")
	entityPattern := regexp.MustCompile(`(?ms)^\s*Entity\s+([A-Za-z_]\w*)\s*\{(.*?)^\s*\}`)
	roots := map[string]bool{}
	for _, match := range entityPattern.FindAllStringSubmatch(cml, -1) {
		if strings.Contains(match[2], "aggregateRoot") {
			roots[match[1]] = true
		}
	}
	if len(roots) == 0 {
		t.Fatal("no CML aggregate roots found")
	}
	return roots
}

func TestCMLAggregateRootsMatchGoAndCRDKinds(t *testing.T) {
	want := cmlAggregateRoots(t)
	goKinds := map[string]bool{}
	rootTypePattern := regexp.MustCompile(`(?s)\+kubebuilder:object:root=true.*?type\s+([A-Za-z_]\w*)\s+struct`)
	files, err := filepath.Glob(filepath.Join(repoRoot, "api/v1alpha1/*_types.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range files {
		content, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, kind := range matches(string(content), rootTypePattern) {
			if !strings.HasSuffix(kind, "List") {
				goKinds[kind] = true
			}
		}
	}
	assertSameSet(t, "CML aggregate-root/Go Kind", goKinds, want)

	crdKinds := map[string]bool{}
	crdFiles, err := filepath.Glob(filepath.Join(repoRoot, "config/crd/bases/*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range crdFiles {
		content, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		kind := kindFromCRDNames(string(content))
		if kind == "" {
			t.Errorf("%s has no spec.names.kind", name)
			continue
		}
		crdKinds[kind] = true
	}
	assertSameSet(t, "CML aggregate-root/CRD Kind", crdKinds, want)
}

func kindFromCRDNames(content string) string {
	namesIndent := -1
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		indent := len(line) - len(strings.TrimLeft(line, " "))
		if namesIndent >= 0 {
			if trimmed != "" && indent <= namesIndent {
				namesIndent = -1
			} else if strings.HasPrefix(trimmed, "kind:") {
				return strings.TrimSpace(strings.TrimPrefix(trimmed, "kind:"))
			}
		}
		if trimmed == "names:" {
			namesIndent = indent
		}
	}
	return ""
}

func TestMDSLPayloadsResolveAndContractsCoverKinds(t *testing.T) {
	mdsl := readMDSL(t)
	types := matches(mdsl, regexp.MustCompile(`(?m)^\s*data\s+type\s+([A-Za-z_]\w*)\s+`))
	endpoints := matches(mdsl, regexp.MustCompile(`(?m)^\s*endpoint\s+type\s+([A-Za-z_]\w*)`))
	rejectDuplicates(t, "MDSL data type", types)
	rejectDuplicates(t, "MDSL endpoint", endpoints)
	knownTypes := set(types)

	payloadPattern := regexp.MustCompile(`\b(?:expecting|delivering)\s+payload\s+([A-Za-z_]\w*)`)
	payloads := matches(mdsl, payloadPattern)
	if len(payloads) == 0 {
		t.Fatal("no MDSL payload references found")
	}
	for _, payload := range payloads {
		if !knownTypes[payload] {
			t.Errorf("MDSL payload %q has no data type declaration", payload)
		}
	}

	knownEndpoints := set(endpoints)
	for kind := range cmlAggregateRoots(t) {
		contract := kind + "Contract"
		if !knownEndpoints[contract] {
			t.Errorf("resource Kind %s has no MDSL %s", kind, contract)
		}
	}
}

func TestArchitectureRules(t *testing.T) {
	ruleDocument := readRules(t)
	if ruleDocument.Version != 1 {
		t.Errorf("rules version = %d, want 1", ruleDocument.Version)
	}
	if len(ruleDocument.Rules) == 0 {
		t.Fatal("no architecture rules defined")
	}

	cml := readRepoFile(t, "docs/model/context-map.cml")
	mdsl := readMDSL(t)
	modelText := cml + "\n" + mdsl
	categories := []string{"BOUNDED_CONTEXT_BOUNDARY", "AGGREGATE_BOUNDARY", "STEREOTYPE", "QUALITY_GOVERNANCE"}
	validCategories := set(categories)
	errorCounts := map[string]int{}
	seenIDs := map[string]bool{}
	severities := map[string]string{}

	for _, rule := range ruleDocument.Rules {
		if rule.ID == "" || seenIDs[rule.ID] {
			t.Errorf("empty or duplicate rule ID %q", rule.ID)
		}
		seenIDs[rule.ID] = true
		severities[rule.ID] = rule.Severity
		if !validCategories[rule.Category] {
			t.Errorf("%s has invalid category %q", rule.ID, rule.Category)
		}
		if rule.Statement == "" || rule.Because == "" || rule.Verification == "" {
			t.Errorf("%s must contain statement, because, and verification", rule.ID)
		}
		if rule.Severity != "ERROR" && rule.Severity != "WARNING" {
			t.Errorf("%s has invalid severity %q", rule.ID, rule.Severity)
		}
		if rule.Severity == "ERROR" {
			errorCounts[rule.Category]++
		}
		if len(rule.Scope.ModelElements) == 0 || len(rule.Scope.CodePaths) == 0 {
			t.Errorf("%s must name model elements and code paths", rule.ID)
		}
		for _, element := range rule.Scope.ModelElements {
			pattern := regexp.MustCompile(`\b` + regexp.QuoteMeta(element) + `\b`)
			if !pattern.MatchString(modelText) {
				t.Errorf("%s references unknown model element %q", rule.ID, element)
			}
		}
		for _, codePath := range rule.Scope.CodePaths {
			if _, err := os.Stat(filepath.Join(repoRoot, codePath)); err != nil {
				t.Errorf("%s references code path %q: %v", rule.ID, codePath, err)
			}
		}
	}

	seenViolations := map[string]bool{}
	for _, violation := range ruleDocument.ExistingViolations {
		if violation.RuleID == "" || violation.Owner == "" || violation.ExitIssue == "" {
			t.Errorf("existing violation must contain ruleId, owner, and exitIssue: %+v", violation)
		}
		if !seenIDs[violation.RuleID] {
			t.Errorf("existing violation references unknown rule %q", violation.RuleID)
		}
		if severities[violation.RuleID] != "WARNING" {
			t.Errorf("existing violation %q must reference a WARNING rule", violation.RuleID)
		}
		if seenViolations[violation.RuleID] {
			t.Errorf("duplicate existing violation for rule %q", violation.RuleID)
		}
		seenViolations[violation.RuleID] = true
	}

	for _, category := range categories {
		if errorCounts[category] == 0 {
			t.Errorf("SOP-12 category %s has no ERROR rule ready for activation", category)
		}
	}
}
