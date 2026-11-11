// Package options provides Allure metadata constructors without the "With" prefix.
//
// This package re-exports allure.With* functions under shorter names for
// cleaner test annotations. Import anonymously to use directly:
//
//	import . "github.com/appfun-dev/imageflow/test/utils/allure/options"
//
// Usage:
//
//	agt.Test(t, "example", body,
//	    AllureID("CLOCK-001"),
//	    TMS("CLOCK-001", "https://..."),
//	    Label("scenario", "test case"),
//	    Epic("Infrastructure"),
//	    Feature("ADR-009"),
//	    ParentSuite("unit"),
//	    Suite("Clock"),
//	)
package options

import (
	gotest "github.com/allure-framework/allure-go/commons/gotest"
)

// ─── Traceability ───────────────────────────────────────────────────────────

// AllureID sets the unique Allure test case identifier.
var AllureID = gotest.WithAllureID

// TMS links the test to a Test Management System entry (e.g., GitHub issue).
var TMS = gotest.WithTMS

// Issue links the test to a bug or issue tracker entry (for regression tests).
var Issue = gotest.WithIssue

// HistoryID sets a custom history identifier for trend analysis.
var HistoryID = gotest.WithHistoryID

// TestCaseID sets an explicit test case ID (overrides auto-generated).
var TestCaseID = gotest.WithTestCaseID

// TestCaseName sets an explicit test case name (overrides function name).
var TestCaseName = gotest.WithTestCaseName

// ─── Hierarchy ──────────────────────────────────────────────────────────────

// Epic groups tests by project phase or major initiative.
var Epic = gotest.WithEpic

// Feature groups tests by functional domain or ADR.
var Feature = gotest.WithFeature

// Story groups tests by user story.
var Story = gotest.WithStory

// ─── Labels ─────────────────────────────────────────────────────────────────

// Label adds a custom label (e.g., "userstory", "scenario", "component").
var Label = gotest.WithLabel

// Tag adds a tag label (shorthand for Label("tag", value)).
var Tag = gotest.WithTag

// Severity sets the test severity level (blocker, critical, normal, minor, trivial).
var Severity = gotest.WithSeverity

// Owner sets the test owner.
var Owner = gotest.WithOwner

// ─── Suite Classification ───────────────────────────────────────────────────

// ParentSuite sets the top-level suite (e.g., "unit", "integration", "e2e").
var ParentSuite = gotest.WithParentSuite

// Suite sets the second-level suite (typically domain or component name).
var Suite = gotest.WithSuite

// SubSuite sets the third-level suite for further grouping.
var SubSuite = gotest.WithSubSuite

// Package sets the package label for organization.
var Package = gotest.WithPackage

// ─── Documentation ──────────────────────────────────────────────────────────

// Description sets the test description in Markdown format.
var Description = gotest.WithDescription

// DescriptionHTML sets the test description in HTML format.
var DescriptionHTML = gotest.WithDescriptionHTML

// DisplayName sets a custom display name for the test.
var DisplayName = gotest.WithDisplayName

// Link adds a documentation or reference link.
var Link = gotest.WithLink

// ─── Parameters ─────────────────────────────────────────────────────────────

// Parameter adds a named parameter to the test (for table-driven tests).
var Parameter = gotest.WithParameter

// ─── Infrastructure ─────────────────────────────────────────────────────────

// Clock overrides the timestamp source for deterministic test timing.
// When used in packages with naming conflicts (e.g., clock.Clock), import
// this package with an alias: import ao "github.com/.../options" then use ao.Clock.
var Clock = gotest.WithClock

// IDGenerator overrides the test ID generation function.
var IDGenerator = gotest.WithIDGenerator

// ─── Type Alias ─────────────────────────────────────────────────────────────

// Option is the Allure test option type.
type Option = gotest.Option
