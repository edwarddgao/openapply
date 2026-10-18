import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from scripts.gmail_imap import WORKDAY_LINK_RE
from scripts.overnightctl import LABEL, plist
from scripts.run_overnight import AUTH_FAILURE_PHRASES, OvernightRun, good_previous_partition, has_apply_process, is_auth_failure, jsonl_tail, merge_slug_lines, parse_json_object
import scripts.apply as apply_pipeline


class OvernightHelpersTest(unittest.TestCase):
    def test_parse_json_object_ignores_cli_prefix(self) -> None:
        value = parse_json_object('Using keyring backend: keyring\n{"token_valid": true}\n')
        self.assertTrue(value['token_valid'])

    def test_competing_pipeline_detection_ignores_shell_mentions(self) -> None:
        listing = (
            '123 1 /bin/zsh -lc ps | rg scripts/apply.py\n'
            '456 1 /Library/Frameworks/Python scripts/apply.py --pool fresh\n'
        )
        self.assertFalse(has_apply_process(listing.splitlines()[0]))
        self.assertTrue(has_apply_process(listing.splitlines()[1]))

    def test_jsonl_tail_reads_only_new_records(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'status.jsonl'
            path.write_text(
                '\n'.join(json.dumps({'id': str(index), 'status': 'submitted'}) for index in range(3)) + '\n',
                encoding='utf-8',
            )
            self.assertEqual([row['id'] for row in jsonl_tail(path, 3)], ['2'])

    def test_merge_slug_lines_keeps_local_order_and_appends_upstream_only(self) -> None:
        merged, added = merge_slug_lines('zeta\nalpha\n\nalpha\n', 'alpha\nbeta\n gamma \nzeta\n')
        self.assertEqual(merged, 'zeta\nalpha\nbeta\ngamma\n')
        self.assertEqual(added, 2)
        self.assertEqual(merge_slug_lines('a\n', 'a\n'), ('a\n', 0))

    def test_auth_failure_phrases_match_real_cli_errors(self) -> None:
        self.assertTrue(is_auth_failure('Failed to authenticate: OAuth session expired and could not be refreshed'))
        self.assertTrue(is_auth_failure('Not logged in · Please run /login'))
        self.assertFalse(is_auth_failure('Blocked: the form requires a login we do not have'))
        # run_overnight keeps its own copy so it need not import apply.py.
        self.assertEqual(AUTH_FAILURE_PHRASES, apply_pipeline.AUTH_FAILURE_PHRASES)
        self.assertEqual(is_auth_failure('Not logged in'), apply_pipeline.is_auth_failure('Not logged in'))

    def test_launch_agent_is_scheduled_and_not_run_at_load(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        value = plist(repo, 5, 0, 200)
        self.assertEqual(value['Label'], LABEL)
        self.assertEqual(value['StartCalendarInterval'], {'Hour': 5, 'Minute': 0})
        self.assertFalse(value['RunAtLoad'])
        self.assertEqual(value['ExitTimeOut'], 60)
        self.assertIn('run_overnight.py', ' '.join(value['ProgramArguments']))
        self.assertNotIn('caffeinate', ' '.join(value['ProgramArguments']))

    def test_pipeline_stop_kills_tracked_subagents(self) -> None:
        process = subprocess.Popen(['/bin/sleep', '30'], start_new_session=True)
        try:
            with apply_pipeline.ACTIVE_SUBAGENTS_LOCK:
                apply_pipeline.ACTIVE_SUBAGENTS.add(process)
            apply_pipeline.request_stop()
            self.assertIsNotNone(process.wait(timeout=2))
        finally:
            with apply_pipeline.ACTIVE_SUBAGENTS_LOCK:
                apply_pipeline.ACTIVE_SUBAGENTS.discard(process)
            apply_pipeline.STOP_EVENT.clear()
            if process.poll() is None:
                process.kill()

    @mock.patch('scripts.apply.time.sleep')
    @mock.patch('scripts.apply.subprocess.run')
    def test_chrome_profile_copy_retries_transient_exit_23(self, run: mock.Mock, sleep: mock.Mock) -> None:
        run.side_effect = [
            subprocess.CompletedProcess(['rsync'], 23, '', 'file vanished'),
            subprocess.CompletedProcess(['rsync'], 0, '', ''),
        ]

        apply_pipeline.copy_chrome_profile(Path('/source'), Path('/destination'))

        self.assertEqual(run.call_count, 2)
        sleep.assert_called_once_with(0.5)

    @mock.patch('scripts.apply.copy_chrome_profile', side_effect=subprocess.CalledProcessError(23, ['rsync']))
    @mock.patch('scripts.apply.shutil.which', return_value='/usr/bin/rsync')
    def test_failed_chrome_profile_copy_cleans_temporary_directory(
        self,
        _which: mock.Mock,
        _copy: mock.Mock,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'source'
            (source / 'Default').mkdir(parents=True)
            (source / 'Local State').write_text('{}', encoding='utf-8')
            chrome = root / 'Google Chrome'
            chrome.touch()
            temporary = root / 'temporary-profile'
            args = SimpleNamespace(
                chrome_source_profile=str(source),
                chrome_binary=str(chrome),
                cdp_port=9222,
            )
            with mock.patch('scripts.apply.tempfile.mkdtemp', return_value=str(temporary)):
                with self.assertRaises(subprocess.CalledProcessError):
                    apply_pipeline.start_cdp_chrome(args, 9222)
            self.assertFalse(temporary.exists())

    def test_workday_link_matches_myworkdaysite_tenants(self) -> None:
        links = {
            'https://wd1.myworkdaysite.com/recruiting/snapchat/snap/passwordreset/tok/?redirect=x': ('snapchat', 'passwordreset'),
            'https://nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite/activate/tok/': ('nvidia', 'activate'),
        }
        for link, expected in links.items():
            match = WORKDAY_LINK_RE.search(link)
            self.assertEqual((match.group(1) or match.group(2), match.group(3)), expected)


    def test_failed_source_falls_back_to_last_good_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            for date, size in (('2026-10-16', 1000), ('2026-10-17', 40), ('2026-10-18', 30)):
                for source in ('workday', 'ashby'):
                    part = repo / 'data' / f'date={date}' / f'source={source}'
                    part.mkdir(parents=True)
                    (part / 'part-0.parquet').write_bytes(b'x' * (size if source == 'workday' else 500))
            run_dir = repo / 'run'
            run_dir.mkdir()
            (run_dir / 'refresh_stats.json').write_text(json.dumps({
                'workday': {'tenants_ok': 300, 'tenants_err': 5300, 'jobs': 900},
                'ashby': {'tenants_ok': 4200, 'tenants_err': 1100, 'jobs': 65000},
            }), encoding='utf-8')
            run = SimpleNamespace(repo=repo, run_dir=run_dir, date='2026-10-18', summary={}, save_summary=lambda: None)
            with tempfile.TemporaryFile('w+') as log:
                OvernightRun.fall_back_failed_sources(run, log)
            today = repo / 'data' / 'date=2026-10-18'
            self.assertEqual((today / 'source=workday' / 'part-0.parquet').stat().st_size, 1000)
            self.assertEqual((today / 'source=ashby' / 'part-0.parquet').stat().st_size, 500)
            self.assertEqual([note['used_date'] for note in run.summary['source_fallbacks']], ['2026-10-16'])
            self.assertIsNone(good_previous_partition(repo / 'data', 'workday', '2026-10-16'))


if __name__ == '__main__':
    unittest.main()
