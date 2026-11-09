"""Synthetic evidence exists ONLY under pytest temporary directories.

Positive fixtures exercise schema acceptance, not actual approvals or execution.
"""

import ast
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest


HELPER_PATH = Path(__file__).resolve().parents[3] / "scripts/aerospike_evidence.py"
SPEC = importlib.util.spec_from_file_location("aerospike_evidence", HELPER_PATH)
evidence = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(evidence)
GATES = tuple(evidence.VALIDATORS)
OPERATOR_SOURCES = (
    "gptcache/manager/scalar_data/aerospike_operator.py",
    "tests/unit_tests/manager/test_aerospike_operator.py",
    "gptcache/manager/scalar_data/_aerospike_operator_runtime.py",
    "tests/unit_tests/manager/test_aerospike_operator_runtime.py",
)
AT = "2026-01-01T00:00:00+00:00"
AFTER = "2026-01-01T00:00:01+00:00"


def write(path, contents):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents, encoding="utf-8")


def reference(phase, name, contents):
    write(phase / name, contents)
    return {"path": name, "sha256": evidence.digest(phase / name)}


def make_fixture(tmp_path, gate):
    root, phase = tmp_path / "source", tmp_path / "phase"
    for relative in evidence.SOURCES[gate]:
        write(root / relative, "synthetic source: " + relative)
    if gate == "model":
        write(root / evidence.MODEL_DOCS[1], "## Group: payload\n## Group: metadata\n")
    if gate == "runbook":
        write(root / "AERO_VALIDATION.md", "synthetic AQL row\nsynthetic ASADM counts\n")
    sources = {name: evidence.digest(root / name) for name in evidence.SOURCES[gate]}
    output = "SYNTHETIC fixture only\nsynthetic AQL row\nsynthetic ASADM counts\n"
    output += "\n".join(sources.values())
    output += "\n" + "\n".join("synthetic::" + name for name in evidence.CASES[gate])
    transcript = reference(phase, "synthetic-command.txt", output)
    commands = {}
    count = len(evidence.CASES[gate])
    for name in (("derive-summary",) if gate == "model" else evidence.COMMANDS[gate]):
        commands[name] = {
            "command": name + " synthetic fixture, not an actual execution",
            "cwd": str(root), "interpreter": {"path": "/synthetic/python", "version": "3.11"},
            "artifact_versions": {"client": "1.0", "server": "8.2"},
            "source_digests": sources.copy(), "started_at": AT, "ended_at": AFTER,
            "exit_code": 0, "counts": {
                "selected": count if name in evidence.TEST_COMMANDS else 0,
                "passed": count if name in evidence.TEST_COMMANDS else 0,
                "failed": 0, "skipped": 0,
            }, "transcript": transcript.copy(),
        }
    cases = {}
    for name in evidence.CASES[gate]:
        if gate == "model":
            command = "derive-summary"
        elif gate == "docs":
            command = "install" if name == "released_install" else "quickstart"
        elif gate == "live" or name in evidence.LIVE_CASES:
            command = "live"
        else:
            command = {
                "manual_write_read": "manual", "literal_aql": "aql",
                "set_counts": "asadm", "final_install": "install",
                "final_lint": "lint", "final_typecheck": "typecheck", "final_unit": "unit",
            }[name]
        cases[name] = {
            "command": command, "nodeid": "synthetic::" + name,
            "status": "passed", "observation": "SYNTHETIC fixture only",
        }
    data = {
        "schema_version": 1, "gate": gate, "source_digests": sources,
        "commands": commands, "cases": cases, "runtime": "live",
    }
    if gate == "model":
        note = reference(phase, "synthetic-approval.txt", "SYNTHETIC approval fixture ONLY")
        approval = {
            "status": "approved", "identity": "synthetic-stakeholder", "at": AFTER,
            "transcript": note, "quote": "SYNTHETIC approval fixture ONLY",
        }
        data.update({
            "modeler": "synthetic-modeler",
            "clarification": {"status": "resolved", "approval": copy.deepcopy(approval)},
            "groups": [{
                "id": name, "started_at": AFTER,
                "walkthrough": {"transcript": note, "quote": "SYNTHETIC approval fixture ONLY"},
                "approval": copy.deepcopy(approval),
            } for name in ("payload", "metadata")],
            "summary": {
                "method": "generated",
                "guide_sha256": sources[evidence.MODEL_DOCS[2]],
                "summary_sha256": sources[evidence.MODEL_DOCS[3]],
                "command": "derive-summary",
            },
            "review": {
                "approval": copy.deepcopy(approval), "findings": [],
                "guide_sha256": sources[evidence.MODEL_DOCS[2]],
                "summary_sha256": sources[evidence.MODEL_DOCS[3]],
            },
            "final_approval": dict(copy.deepcopy(approval), **{
                "at": "2026-01-01T00:00:02+00:00",
                "guide_sha256": sources[evidence.MODEL_DOCS[2]],
                "summary_sha256": sources[evidence.MODEL_DOCS[3]],
            }),
        })
    elif gate == "docs":
        data.update({
            "runtime": "released",
            "artifacts": {kind: {
                "name": name, "version": version, "channel": "released",
                "source": "https://example.invalid/synthetic-release", "sha256": "a" * 64,
            } for kind, name, version in (
                ("client", "aerospike-sdk", "1.0"), ("server", "aerospike-server", "8.2")
            )},
        })
    elif gate == "runbook":
        data["inspections"] = {tool: {
            "command": tool, "output": output, "inspected_by": "synthetic-inspector",
            "inspected_at": AFTER,
        } for tool, output in (("aql", "synthetic AQL row"), ("asadm", "synthetic ASADM counts"))}
    return root, phase, data


def invoke(gate, root, phase, data):
    write(phase / evidence.FILENAMES[gate], json.dumps(data))
    return evidence.main([gate, "--phase-dir", str(phase), "--source-root", str(root)])


@pytest.mark.parametrize("gate", GATES)
def test_valid_synthetic_fixture_only(tmp_path, gate):
    root, phase, data = make_fixture(tmp_path, gate)
    assert invoke(gate, root, phase, data) == 0


@pytest.mark.parametrize("gate", GATES)
@pytest.mark.parametrize("source", OPERATOR_SOURCES)
def test_operator_sources_cannot_escape_staleness_gates(tmp_path, gate, source):
    assert source in evidence.COMMON_SOURCES
    root, phase, data = make_fixture(tmp_path, gate)
    write(root / source, "changed operator source")
    assert invoke(gate, root, phase, data) == 1


@pytest.mark.parametrize("gate", GATES)
@pytest.mark.parametrize("bad", [
    "missing_field", "malformed", "failed_command", "zero_live", "skipped",
    "missing_case", "case_skipped", "stale_source", "missing_digest",
    "stale_command_digest", "missing_command_digest", "missing_transcript",
    "stale_transcript", "missing_observation", "bad_counts", "missing_command",
    "reversed_time", "naive_time", "missing_versions", "boolean_exit", "missing_nodeid",
])
def test_common_fail_closed(tmp_path, gate, bad):
    root, phase, data = make_fixture(tmp_path, gate)
    first = next(iter(data["commands"]))
    record = data["commands"][first]
    case = next(iter(data["cases"]))
    if bad == "missing_field":
        del data["source_digests"]
    elif bad == "malformed":
        data["commands"] = []
    elif bad == "failed_command":
        record["exit_code"] = 1
    elif bad == "zero_live":
        # Even model/docs must reject selected case nodes exceeding test passes.
        record["counts"] = {"selected": 0, "passed": 0, "failed": 0, "skipped": 0}
        record_name = first
        if first not in evidence.TEST_COMMANDS:
            record_name = "unit"
            data["commands"][record_name] = record
        data["cases"][case]["command"] = record_name
    elif bad == "skipped":
        record["counts"] = {"selected": 1, "passed": 0, "failed": 0, "skipped": 1}
    elif bad == "missing_case":
        del data["cases"][case]
    elif bad == "case_skipped":
        data["cases"][case]["status"] = "skipped"
    elif bad == "stale_source":
        write(root / evidence.PROVIDER, "changed")
    elif bad == "missing_digest":
        del data["source_digests"][evidence.PROVIDER]
    elif bad == "stale_command_digest":
        record["source_digests"][evidence.PROVIDER] = "0" * 64
    elif bad == "missing_command_digest":
        del record["source_digests"][evidence.PROVIDER]
    elif bad == "missing_transcript":
        (phase / record["transcript"]["path"]).unlink()
    elif bad == "stale_transcript":
        write(phase / record["transcript"]["path"], "modified")
    elif bad == "missing_observation":
        data["cases"][case]["observation"] = "not in synthetic transcript"
    elif bad == "bad_counts":
        record["counts"]["selected"] = 1000
    elif bad == "missing_command":
        del data["commands"][first]
    elif bad == "reversed_time":
        record["started_at"], record["ended_at"] = AFTER, AT
    elif bad == "naive_time":
        record["started_at"] = "2026-01-01T00:00:00"
    elif bad == "missing_versions":
        record["artifact_versions"] = {}
    elif bad == "boolean_exit":
        record["exit_code"] = False
    elif bad == "missing_nodeid":
        data["cases"][case]["nodeid"] = ""
    assert invoke(gate, root, phase, data) == 1


@pytest.mark.parametrize("bad", [
    "unresolved", "missing_approval", "wrong_order", "omitted_group", "early_group",
    "same_reviewer", "missing_reviewer", "review_findings", "stale_summary",
    "stale_review", "missing_walkthrough", "missing_group_inventory",
])
def test_model_gates(tmp_path, bad):
    root, phase, data = make_fixture(tmp_path, "model")
    if bad == "unresolved":
        data["clarification"]["status"] = "missing"
    elif bad == "missing_approval":
        del data["groups"][0]["approval"]
    elif bad == "wrong_order":
        data["groups"].reverse()
    elif bad == "omitted_group":
        data["groups"].pop()
    elif bad == "early_group":
        data["groups"][1]["started_at"] = AT
    elif bad == "same_reviewer":
        data["review"]["approval"]["identity"] = data["modeler"]
    elif bad == "missing_reviewer":
        data["review"]["approval"]["identity"] = ""
    elif bad == "review_findings":
        data["review"]["findings"] = [{"severity": "high"}]
    elif bad == "stale_summary":
        data["summary"]["guide_sha256"] = "0" * 64
    elif bad == "stale_review":
        data["review"]["summary_sha256"] = "0" * 64
    elif bad == "missing_walkthrough":
        data["groups"][0]["walkthrough"]["quote"] = ""
    elif bad == "missing_group_inventory":
        write(root / evidence.MODEL_DOCS[1], "no inventory")
        new = evidence.digest(root / evidence.MODEL_DOCS[1])
        data["source_digests"][evidence.MODEL_DOCS[1]] = new
        for record in data["commands"].values():
            record["source_digests"][evidence.MODEL_DOCS[1]] = new
    assert invoke("model", root, phase, data) == 1


def manual_model(tmp_path):
    root, phase, data = make_fixture(tmp_path, "model")
    data["commands"]["verify-model-docs"] = data["commands"].pop("derive-summary")
    data["commands"]["verify-model-docs"]["command"] = "synthetic verification ONLY"
    for case in data["cases"].values():
        case["command"] = "verify-model-docs"
    summary = data["summary"]
    summary.update(method="manual-source-bound", command="verify-model-docs")
    summary["provenance"] = copy.deepcopy(data["groups"][0]["walkthrough"])
    previous = data["clarification"]["approval"]["at"]
    for group in data["groups"]:
        group["start_observation"] = dict(copy.deepcopy(group["walkthrough"]), **{
            "recorded_at": group.pop("started_at"), "after_approval_at": previous,
        })
        group["approval"]["status"] = "approved_conditional_exploratory_only"
        previous = group["approval"]["at"]
    return root, phase, data


def test_manual_source_bound_model_zero_tests_and_recorded_starts(tmp_path):
    root, phase, data = manual_model(tmp_path)
    assert data["commands"]["verify-model-docs"]["counts"]["selected"] == 0
    assert invoke("model", root, phase, data) == 0


@pytest.mark.parametrize("bad", [
    "missing_method", "unknown_method", "missing_provenance", "false_quote",
    "missing_verify", "generated_without_derivation", "wrong_command",
    "missing_start", "ambiguous_start", "missing_prior", "wrong_prior",
    "early_observation", "late_observation", "missing_start_quote",
    "unapproved_group", "missing_final", "conditional_final", "early_final",
    "same_time_final", "stale_final_guide", "stale_final_summary",
    "missing_final_quote", "review_before_groups", "no_pair_in_output",
])
def test_manual_model_and_final_approval_fail_closed(tmp_path, bad):
    root, phase, data = manual_model(tmp_path)
    summary, group, final = data["summary"], data["groups"][1], data["final_approval"]
    if bad == "missing_method":
        del summary["method"]
    elif bad == "unknown_method":
        summary["method"] = "imagined"
    elif bad == "missing_provenance":
        del summary["provenance"]
    elif bad == "false_quote":
        summary["provenance"]["quote"] = "not present"
    elif bad == "missing_verify":
        del data["commands"]["verify-model-docs"]
    elif bad == "generated_without_derivation":
        summary.update(method="generated", command="derive-summary")
    elif bad == "wrong_command":
        summary["command"] = "derive-summary"
    elif bad == "missing_start":
        del group["start_observation"]
    elif bad == "ambiguous_start":
        group["started_at"] = AFTER
    elif bad == "missing_prior":
        del group["start_observation"]["after_approval_at"]
    elif bad == "wrong_prior":
        group["start_observation"]["after_approval_at"] = AT
    elif bad == "early_observation":
        group["start_observation"]["recorded_at"] = AT
    elif bad == "late_observation":
        group["start_observation"]["recorded_at"] = final["at"]
    elif bad == "missing_start_quote":
        group["start_observation"]["quote"] = ""
    elif bad == "unapproved_group":
        group["approval"]["status"] = "pending"
    elif bad == "missing_final":
        del data["final_approval"]
    elif bad == "conditional_final":
        final["status"] = "approved_conditional_exploratory_only"
    elif bad == "early_final":
        final["at"] = AT
    elif bad == "same_time_final":
        final["at"] = data["review"]["approval"]["at"]
    elif bad == "stale_final_guide":
        final["guide_sha256"] = "0" * 64
    elif bad == "stale_final_summary":
        final["summary_sha256"] = "0" * 64
    elif bad == "missing_final_quote":
        final["quote"] = ""
    elif bad == "review_before_groups":
        data["review"]["approval"]["at"] = AT
    elif bad == "no_pair_in_output":
        record = data["commands"]["verify-model-docs"]
        record["transcript"] = reference(phase, "no-pair.txt", "SYNTHETIC fixture only")
    assert invoke("model", root, phase, data) == 1


@pytest.mark.parametrize("bad", ["dev_runtime", "dev_artifact", "missing_provenance", "wrong_version"])
def test_docs_requires_released_execution(tmp_path, bad):
    root, phase, data = make_fixture(tmp_path, "docs")
    if bad == "dev_runtime":
        data["runtime"] = "live"
    elif bad == "dev_artifact":
        data["artifacts"]["server"]["version"] = "aerospike-server:dev"
    elif bad == "missing_provenance":
        del data["artifacts"]["client"]["source"]
    elif bad == "wrong_version":
        data["commands"]["quickstart"]["artifact_versions"]["client"] = "other"
    assert invoke("docs", root, phase, data) == 1


@pytest.mark.parametrize("bad", ["missing_inspection", "nonliteral", "uninspected", "wrong_tool"])
def test_runbook_literal_inspected_output_required(tmp_path, bad):
    root, phase, data = make_fixture(tmp_path, "runbook")
    if bad == "missing_inspection":
        del data["inspections"]["aql"]
    elif bad == "nonliteral":
        data["inspections"]["aql"]["output"] = "imagined record"
    elif bad == "uninspected":
        data["inspections"]["aql"]["inspected_by"] = ""
    elif bad == "wrong_tool":
        data["commands"]["aql"]["command"] = "echo simulated"
    assert invoke("runbook", root, phase, data) == 1


@pytest.mark.parametrize("gate", GATES)
def test_missing_and_malformed_evidence_files(tmp_path, gate):
    root, phase, _ = make_fixture(tmp_path, gate)
    args = [gate, "--phase-dir", str(phase), "--source-root", str(root)]
    assert evidence.main(args) == 1
    write(phase / evidence.FILENAMES[gate], "{malformed")
    assert evidence.main(args) == 1
    write(phase / evidence.FILENAMES[gate], '{"gate":"live","gate":"docs"}')
    assert evidence.main(args) == 1


def test_source_grammar_only_and_cli_help():
    ast.parse(HELPER_PATH.read_text(), feature_version=(3, 8))
    for args in (["--help"],) + tuple([gate, "--help"] for gate in GATES):
        result = subprocess.run(
            [sys.executable, str(HELPER_PATH)] + args, capture_output=True, timeout=10
        )
        assert result.returncode == 0


def test_cli_nonzero_and_does_not_modify_evidence(tmp_path):
    root, phase, data = make_fixture(tmp_path, "live")
    data["cases"].clear()
    path = phase / evidence.FILENAMES["live"]
    write(path, json.dumps(data))
    before = path.read_bytes()
    result = subprocess.run([
        sys.executable, str(HELPER_PATH), "live", "--phase-dir", str(phase),
        "--source-root", str(root),
    ], capture_output=True, timeout=10)
    assert result.returncode == 1
    assert path.read_bytes() == before


def test_transcript_path_escape_rejected(tmp_path):
    root, phase, data = make_fixture(tmp_path, "live")
    write(tmp_path / "outside.txt", "SYNTHETIC fixture only")
    data["commands"]["live"]["transcript"] = {
        "path": "../outside.txt", "sha256": evidence.digest(tmp_path / "outside.txt"),
    }
    assert invoke("live", root, phase, data) == 1


def test_live_requires_actual_runtime_and_named_test_observations(tmp_path):
    root, phase, data = make_fixture(tmp_path, "live")
    data["runtime"] = "mock"
    assert invoke("live", root, phase, data) == 1
    data["runtime"] = "live"
    data["cases"][evidence.LIVE_CASES[0]]["nodeid"] = "absent::test"
    assert invoke("live", root, phase, data) == 1
