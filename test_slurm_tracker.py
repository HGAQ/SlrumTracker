"""Regression checks for partition CPU accounting without a live Slurm cluster."""

import contextlib
import io
import unittest
from unittest.mock import patch

import slurm_tracker as tracker


def sinfo_row(partition, nodes, node_state, cpu_states):
    return tracker.DELIM.join(
        [partition, "up", "2-00:00:00", str(nodes), node_state, cpu_states]
    )


class PartitionCpuTests(unittest.TestCase):
    def test_grouped_mixed_and_heterogeneous_nodes(self):
        # %C counts the whole row: never multiply these counts by %D.
        sinfo_output = "\n".join([
            sinfo_row("compute*", 2, "mixed", "24/40/0/64"),
            sinfo_row("compute*", 1, "idle", "0/64/0/64"),
            sinfo_row("compute*", 1, "allocated", "32/0/0/32"),
            sinfo_row("compute*", 1, "draining", "8/0/24/32"),
            sinfo_row("compute*", 1, "down", "0/0/16/16"),
            sinfo_row("gpu", 1, "idle", "0/16/0/16"),
        ])
        partition_summaries, parse_warnings = tracker.parse_sinfo_partitions_output(sinfo_output)
        self.assertEqual(parse_warnings, [])
        compute_summary = partition_summaries["compute"]
        self.assertEqual(compute_summary.cpu_load_text, "64/208")
        self.assertEqual(compute_summary.cpus_idle, 104)
        self.assertEqual(compute_summary.cpus_other, 40)
        self.assertEqual(compute_summary.nodes_total, 6)
        self.assertEqual(compute_summary.nodes_mixed, 2)
        self.assertEqual(partition_summaries["gpu"].cpu_load_text, "0/16")
        cpu_payload = tracker.partition_to_dict(compute_summary)
        self.assertEqual(cpu_payload["cpus_total"], 208)
        self.assertTrue(cpu_payload["cpus_available"])

    def test_invalid_counts_hide_partial_totals_in_either_order(self):
        for invalid_counts in ("N/A", "1/2/3", "-1/33/0/32", "1/2/3/99", "1+/2/3/6"):
            for invalid_first in (False, True):
                with self.subTest(cpu_states=invalid_counts, invalid_first=invalid_first):
                    sinfo_rows = [
                        sinfo_row("compute", 1, "mixed", "8/24/0/32"),
                        sinfo_row("compute", 1, "idle", invalid_counts),
                    ]
                    if invalid_first:
                        sinfo_rows.reverse()
                    partition_summaries, parse_warnings = tracker.parse_sinfo_partitions_output(
                        "\n".join(sinfo_rows)
                    )
                    self.assertEqual(len(parse_warnings), 1)
                    compute_summary = partition_summaries["compute"]
                    self.assertEqual(compute_summary.nodes_total, 2)
                    self.assertEqual(compute_summary.cpu_load_text, "-")
                    self.assertIsNone(tracker.partition_to_dict(compute_summary)["cpus_total"])

    def test_missing_cpu_column_does_not_show_partial_capacity(self):
        sinfo_output = "\n".join([
            sinfo_row("compute", 1, "idle", "0/32/0/32"),
            tracker.DELIM.join(["compute", "up", "infinite", "1", "mixed"]),
        ])
        partition_summaries, parse_warnings = tracker.parse_sinfo_partitions_output(sinfo_output)
        self.assertEqual(len(parse_warnings), 1)
        self.assertFalse(partition_summaries["compute"].cpus_available)

    def test_zero_capacity_and_queue_only_partition(self):
        partition_summaries, parse_warnings = tracker.parse_sinfo_partitions_output(
            sinfo_row("empty", 0, "n/a", "0/0/0/0")
        )
        self.assertEqual(parse_warnings, [])
        self.assertEqual(partition_summaries["empty"].cpu_load_text, "0/0")
        combined_summaries = tracker.combine_partition_summaries(
            partition_summaries, {"queue_only": {"queued": 2}}, {}
        )
        queue_summary = next(
            partition_summary for partition_summary in combined_summaries
            if partition_summary.partition == "queue_only"
        )
        self.assertEqual(queue_summary.jobs_queued, 2)
        self.assertEqual(queue_summary.cpu_load_text, "-")
        self.assertIsNone(tracker.partition_to_dict(queue_summary)["cpus_idle"])

    def test_local_loader_and_terminal_use_cpu_counts(self):
        with patch.object(tracker, "run_command", return_value=(
            sinfo_row("compute", 1, "mixed", "8/24/0/32"), None
        )) as slurm_command:
            partition_summaries, parse_warnings = tracker.load_sinfo_partitions(
                tracker.CommandRunner(backend="local")
            )
        self.assertIn("%C", slurm_command.call_args.args[0][-1])
        terminal_output = io.StringIO()
        with contextlib.redirect_stdout(terminal_output):
            tracker.render([], list(partition_summaries.values()), parse_warnings,
                           ":memory:", "test", 10, False, False)
        self.assertIn("CPUS A/T", terminal_output.getvalue())
        self.assertIn("8/32", terminal_output.getvalue())

    def test_ssh_batch_uses_same_cpu_parser(self):
        with tracker.init_db(":memory:") as tracker_database:
            with patch.object(tracker, "run_ssh_batch", return_value=({
                "squeue": "", "sinfo": sinfo_row("compute", 1, "mixed", "8/24/0/32"),
                "partition_queue": "",
            }, [])) as ssh_batch:
                displayed_jobs, partition_summaries, parse_warnings = tracker.poll_jobs_ssh_batch(
                    tracker_database, tracker.CommandRunner(backend="ssh"),
                    "test_user", [], 7, False, 200,
                )
        self.assertEqual(displayed_jobs, [])
        self.assertEqual(parse_warnings, [])
        self.assertEqual(partition_summaries[0].cpu_load_text, "8/32")
        ssh_commands = dict(ssh_batch.call_args.args[1])
        self.assertIn("%C", ssh_commands["sinfo"][-1])

    def test_pending_reasons_are_current_and_partition_specific(self):
        active_user_jobs = {
            "1": tracker.Job("1", "PENDING", partition="small", reason="(Priority)"),
            "2": tracker.Job("2", "PENDING", partition="small", reason="Priority"),
            "3": tracker.Job("3", "PENDING", partition="small", reason="(Dependency)"),
            "4": tracker.Job("4", "RUNNING", partition="small", reason="node01"),
            "5": tracker.Job("5", "PENDING", partition="batch", reason="Resources"),
        }
        partition_summaries = tracker.combine_partition_summaries({}, {}, active_user_jobs)
        partition_payloads = {
            partition_summary.partition: tracker.partition_to_dict(partition_summary)
            for partition_summary in partition_summaries
        }
        self.assertEqual(partition_payloads["small"]["user_pending_reasons"],
                         {"Priority": 2, "Dependency": 1})
        self.assertEqual(partition_payloads["batch"]["user_pending_reasons"], {"Resources": 1})
        self.assertEqual(partition_payloads["small"]["user_queued"], 3)
        self.assertEqual(partition_payloads["small"]["user_running"], 1)
        refreshed_summaries = tracker.combine_partition_summaries({}, {}, {})
        self.assertEqual(refreshed_summaries, [])

    def test_shared_nodes_are_reported_per_partition(self):
        sinfo_output = "\n".join([
            sinfo_row("small", 1, "mixed", "160/32/0/192"),
            sinfo_row("batch", 1, "mixed", "160/32/0/192"),
        ])
        partition_summaries, parse_warnings = tracker.parse_sinfo_partitions_output(sinfo_output)
        self.assertEqual(parse_warnings, [])
        self.assertEqual(partition_summaries["small"].cpus_idle, 32)
        self.assertEqual(partition_summaries["batch"].cpus_idle, 32)


if __name__ == "__main__":
    unittest.main()
