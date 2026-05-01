import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from brand_gap_inference.quality_gates import QualityGate, build_gate_commands, run_gates


class QualityGateTests(unittest.TestCase):
    def test_build_gate_commands_matches_ci_gate_names(self):
        gates = build_gate_commands("python")

        self.assertEqual(
            [gate.name for gate in gates],
            [
                "unit_tests",
                "phase1_eval",
                "discovery_eval",
                "taxonomy_eval",
                "normalization_eval",
                "product_intelligence_eval",
                "brand_positioning_eval",
                "brand_profile_eval",
                "demand_signal_eval",
                "gap_validation_eval",
                "decision_brief_eval",
                "deep_inference_eval",
            ],
        )
        self.assertIn("brand_gap_inference.demand_signal_eval", gates[8].command)

    def test_build_gate_commands_can_skip_unit_tests(self):
        gates = build_gate_commands("python", include_unit_tests=False)

        self.assertEqual(gates[0].name, "phase1_eval")
        self.assertNotIn("unit_tests", [gate.name for gate in gates])

    def test_run_gates_writes_pass_fail_status_and_honors_fail_fast(self):
        gates = [
            QualityGate("first", (sys.executable, "--version")),
            QualityGate("second", (sys.executable, "-m", "missing_module")),
            QualityGate("third", (sys.executable, "-m", "unused")),
        ]

        completed = [
            subprocess.CompletedProcess(gates[0].command, 0, stdout="ok", stderr=""),
            subprocess.CompletedProcess(gates[1].command, 1, stdout="", stderr="boom"),
        ]

        with patch("brand_gap_inference.quality_gates.subprocess.run", side_effect=completed) as run_mock:
            with patch("builtins.print"):
                report = run_gates(gates, repo_root=Path.cwd(), fail_fast=True)

        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["total_gates"], 2)
        self.assertEqual(report["passed_gates"], 1)
        self.assertEqual(report["failed_gates"], 1)
        self.assertEqual(run_mock.call_count, 2)
        self.assertEqual(report["results"][1]["stderr"], "boom")


if __name__ == "__main__":
    unittest.main()
