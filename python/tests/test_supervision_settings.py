"""Unit tests for shared agent-run lifecycle settings and defaults."""

import importlib.util
import unittest

from agent_supervisor._lifecycle import supervision_settings
from agent_supervisor.microsoft import _settings as microsoft_settings


class SupervisionSettingsTests(unittest.TestCase):
    def test_shared_defaults_match_documentation(self):
        self.assertEqual(supervision_settings(None), (300.0, 5.0))

    def test_overrides_are_normalized_to_seconds(self):
        self.assertEqual(
            supervision_settings({"timeout": 180, "shutdown_grace": 3}),
            (180.0, 3.0),
        )

    def test_partial_override_keeps_other_default(self):
        self.assertEqual(
            supervision_settings({"timeout": 60}),
            (60.0, 5.0),
        )

    def test_rejects_unknown_options_and_invalid_values(self):
        invalid = (
            ({"unknown": 1}, TypeError),
            ([], TypeError),
            ({"timeout": 0}, ValueError),
            ({"timeout": float("inf")}, ValueError),
            ({"timeout": float("nan")}, ValueError),
            ({"shutdown_grace": -1}, ValueError),
            ({"shutdown_grace": 61}, ValueError),
        )
        for settings, error in invalid:
            with self.subTest(settings=settings), self.assertRaises(error):
                supervision_settings(settings)

    def test_microsoft_adapter_uses_shared_defaults_and_validation(self):
        self.assertEqual(microsoft_settings(None), (300.0, 5.0))
        self.assertEqual(
            microsoft_settings({"timeout": 20, "shutdown_grace": 1}),
            (20.0, 1.0),
        )
        with self.assertRaises(TypeError):
            microsoft_settings({"unexpected": True})

    @unittest.skipUnless(importlib.util.find_spec("agents"), "install the openai extra")
    def test_openai_adapter_uses_shared_defaults_and_validation(self):
        from agent_supervisor.openai import Runner

        self.assertEqual(Runner._settings(None), (300.0, 5.0))
        self.assertEqual(
            Runner._settings({"timeout": 20, "shutdown_grace": 1}),
            (20.0, 1.0),
        )
        with self.assertRaises(TypeError):
            Runner._settings({"unexpected": True})
