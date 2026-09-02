import base64
import copy
import gzip
import importlib.util
import json
import pathlib
import sqlite3
import tempfile
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
STATE = REPO_ROOT / 'data' / 'hogona_worker_canonical_state.sql.gz.b64'
WORKER_PATH = REPO_ROOT / 'worker' / 'worker.py'


def load_worker():
    spec = importlib.util.spec_from_file_location('hogona_worker', WORKER_PATH)
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    return worker


class CanonicalStateTests(unittest.TestCase):
    def setUp(self):
        """Recreate disposable SQLite state from the checked-in canonical artifact."""
        self.assertTrue(STATE.is_file(), f'canonical state artifact missing: {STATE}')
        encoded = STATE.read_bytes()
        sql = gzip.decompress(base64.b64decode(encoded, validate=True)).decode('utf-8')
        self.temporary_directory = tempfile.TemporaryDirectory()
        database = pathlib.Path(self.temporary_directory.name) / 'hogona_worker.sqlite'
        self.connection = sqlite3.connect(database)
        self.connection.executescript(sql)
        self.connection.execute('PRAGMA foreign_keys=ON')

    def tearDown(self):
        self.connection.close()
        self.temporary_directory.cleanup()

    def test_canonical_state_has_gold_counts(self):
        state = dict(self.connection.execute(
            'SELECT status, COUNT(*) FROM jobs GROUP BY status'
        ).fetchall())
        self.assertEqual(sum(state.values()), 611)
        self.assertEqual(state, {'completed': 225, 'exported': 386})

    def test_canonical_state_has_no_invalid_terminal_rows(self):
        self.assertEqual(self.connection.execute(
            "SELECT COUNT(*) FROM jobs WHERE status IN ('exported', 'failed') "
            "AND COALESCE(output_data, '') != ''"
        ).fetchone()[0], 0)
        self.assertEqual(self.connection.execute(
            "SELECT COUNT(*) FROM jobs WHERE status = 'completed' "
            "AND (output_data IS NULL OR output_data = '')"
        ).fetchone()[0], 0)

    def test_completed_results_follow_strict_worker_contract(self):
        worker = load_worker()
        rows = self.connection.execute(
            "SELECT job_id, input_data, output_data FROM jobs WHERE status = 'completed'"
        ).fetchall()
        self.assertEqual(len(rows), 225)

        for job_id, input_data, output_data in rows:
            with self.subTest(job_id=job_id):
                result = json.loads(output_data)
                validated, logical = worker.validate(result, json.loads(input_data), job_id)
                # Persisted output is already normalized; validation must not alter it.
                self.assertEqual(validated, result)
                self.assertEqual(logical, output_data)

    def test_validator_rejects_output_schema_drift(self):
        worker = load_worker()
        job_id, input_data, output_data = self.connection.execute(
            "SELECT job_id, input_data, output_data FROM jobs WHERE status = 'completed' "
            "ORDER BY job_id LIMIT 1"
        ).fetchone()
        baseline = json.loads(output_data)
        supplied_input = json.loads(input_data)

        invalid_results = []
        extra_top_level = copy.deepcopy(baseline)
        extra_top_level['confidence'] = 1
        invalid_results.append(extra_top_level)
        missing_place_field = copy.deepcopy(baseline)
        del missing_place_field['place_fields']['summary']
        invalid_results.append(missing_place_field)
        extra_planning_field = copy.deepcopy(baseline)
        extra_planning_field['planning_attributes']['confidence'] = None
        invalid_results.append(extra_planning_field)

        for invalid_result in invalid_results:
            with self.subTest(result=invalid_result):
                with self.assertRaises(ValueError):
                    worker.validate(invalid_result, supplied_input, job_id)


if __name__ == '__main__':
    unittest.main()
