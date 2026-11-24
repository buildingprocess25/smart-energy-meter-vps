"""Control API rules. MQTT, PostgreSQL and .env are replaced, so nothing here reaches a broker or a database.

Run: python -m unittest discover -s backend/tests
"""
import contextlib
import importlib
import json
import os
import sys
import tempfile
import threading
import types
import unittest
from unittest import mock

TOKEN = 'test-token-0123456789abcdef'
BACKEND = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


class FakeCursor:
    def __init__(self, log, rows):
        self.log, self.rows, self.rowcount = log, rows, 0

    def execute(self, sql, params=None):
        self.log.append((' '.join(sql.split()), params))

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def fetchall(self):
        return []


def load_app():
    paho_client = types.ModuleType('paho.mqtt.client')
    paho_client.Client = mock.MagicMock()
    paho_client.MQTTv311 = 4
    psycopg2 = types.ModuleType('psycopg2')
    psycopg2.OperationalError = type('OperationalError', (Exception,), {})
    psycopg2.InterfaceError = type('InterfaceError', (Exception,), {})
    psycopg2.pool = types.ModuleType('psycopg2.pool')
    psycopg2.pool.ThreadedConnectionPool = mock.MagicMock(side_effect=RuntimeError('no database in tests'))
    dotenv = types.ModuleType('dotenv')
    dotenv.load_dotenv = lambda *a, **k: None
    stubs = {
        'paho': types.ModuleType('paho'), 'paho.mqtt': types.ModuleType('paho.mqtt'),
        'paho.mqtt.client': paho_client, 'psycopg2': psycopg2, 'psycopg2.pool': psycopg2.pool, 'dotenv': dotenv,
    }
    stubs['paho.mqtt'].client = paho_client
    sys.modules.update(stubs)
    for name in ('DATABASE_URL', 'SPARTA_DATABASE_URL', 'MQTT_BROKER', 'MQTT_USERNAME', 'MQTT_PASSWORD'):
        os.environ.pop(name, None)
    sys.path.insert(0, BACKEND)
    with mock.patch('threading.Thread') as thread:
        thread.return_value.is_alive.return_value = False
        return importlib.import_module('app')


class ControlApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cwd = os.getcwd()
        os.chdir(tempfile.mkdtemp())
        with open('active_captures.json', 'w') as f:
            json.dump({'BOOT': {'active': True, 'session_id': 'sb', 'session_name': 'N', 'device_id': 'BOOT'}}, f)
        cls.mod = load_app()
        cls.boot = dict(cls.mod._capture_states.get('BOOT') or {})

    @classmethod
    def tearDownClass(cls):
        os.chdir(cls.cwd)

    def setUp(self):
        self.sql, self.rows = [], []

        @contextlib.contextmanager
        def cursor():
            yield FakeCursor(self.sql, self.rows)

        for target, value in {
            'get_db_cursor': cursor, 'db_pool': object(), '_start_thread': mock.MagicMock(),
            '_mqtt_client': mock.MagicMock(), '_pending_state_sync': {},
            '_touched_devices': set(), '_state_load_complete': True,
            '_finalize_in_background': mock.MagicMock(),
        }.items():
            patcher = mock.patch.object(self.mod, target, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        env = mock.patch.dict(os.environ, {'PORTAL_CONTROL_TOKEN': TOKEN}, clear=False)
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop('DASHBOARD_UI_ENABLED', None)
        self.mod._capture_states.clear()
        self.client = self.mod.app.test_client()

    def call(self, method, path, body=None, token=TOKEN):
        headers = {'X-Portal-Token': token} if token else {}
        return self.client.open(path, method=method, json=body, headers=headers)

    def record(self, device, session='s1'):
        state = self.mod._get_device_capture_state(device)
        state.update({'active': True, 'session_id': session, 'session_name': 'Lama', 'device_id': device})
        return state

    def test_importing_the_app_really_restarts_recordings(self):
        self.assertEqual(self.boot.get('session_id'), 'sb')
        self.assertIsNotNone(self.boot.get('_thread'))

    # access
    def test_writes_need_the_token(self):
        for method, path in [
            ('POST', '/api/capture/start'), ('POST', '/api/capture/stop'), ('POST', '/api/capture/delete-session'),
            ('POST', '/api/capture/clear-all'), ('DELETE', '/api/devices/D1'),
        ]:
            for token in (None, 'wrong', TOKEN[:-1]):
                self.assertEqual(self.call(method, path, {}, token).status_code, 401, (path, token))
        self.assertEqual(self.sql, [])

    def test_a_non_ascii_token_is_refused(self):
        res = self.client.post('/api/capture/stop', headers={'X-Portal-Token': 'tok\xe9n'})
        self.assertEqual(res.status_code, 401)

    def test_writes_stay_closed_without_a_configured_token(self):
        for value in ('', 'short'):
            with mock.patch.dict(os.environ, {'PORTAL_CONTROL_TOKEN': value}):
                self.assertEqual(self.call('POST', '/api/capture/stop', {'deviceId': 'D1'}, value or None).status_code, 503)

    def test_a_token_that_is_too_short_closes_reads_too(self):
        for ui in ('', '1'):
            with mock.patch.dict(os.environ, {'PORTAL_CONTROL_TOKEN': 'short', 'DASHBOARD_UI_ENABLED': ui}):
                self.assertEqual(self.call('GET', '/api/capture/status', token='short').status_code, 503)
                self.assertEqual(self.call('GET', '/health', token=None).status_code, 200)

    def test_reads_need_the_token_once_the_old_dashboard_is_off(self):
        self.assertEqual(self.call('GET', '/api/capture/status', token=None).status_code, 401)
        self.assertEqual(self.call('GET', '/api/capture/status').status_code, 200)
        with mock.patch.dict(os.environ, {'DASHBOARD_UI_ENABLED': '1'}):
            self.assertEqual(self.call('GET', '/api/capture/status', token=None).status_code, 200)

    def test_reads_stay_open_until_a_token_is_configured(self):
        with mock.patch.dict(os.environ, {'PORTAL_CONTROL_TOKEN': ''}):
            self.assertEqual(self.call('GET', '/api/capture/status', token=None).status_code, 200)

    def test_the_old_dashboard_and_its_script_are_off_by_default(self):
        for path in ('/', '/app.js', '/anything'):
            self.assertEqual(self.call('GET', path, token=None).status_code, 410, path)
        self.assertEqual(self.call('GET', '/health', token=None).status_code, 200)

    def test_static_files_stay_closed_whatever_the_path_looks_like(self):
        for configured in (TOKEN, ''):
            with mock.patch.dict(os.environ, {'PORTAL_CONTROL_TOKEN': configured}):
                for path in ('/api/../app.js', '/api/%2e%2e/app.js', '/api/..%2fapp.js', '//app.js', '/api/../index.html', '/health/../app.js'):
                    res = self.call('GET', path, token=None)
                    self.assertNotEqual(res.status_code, 200, (path, configured))
                    self.assertNotIn(b'mqtt', res.get_data())

    def test_errors_keep_their_status_and_hide_details(self):
        self.assertEqual(self.call('POST', '/api/capture/status', {}).status_code, 405)
        with mock.patch.object(self.mod, 'get_db_cursor', side_effect=RuntimeError('secret host')):
            res = self.call('POST', '/api/capture/rename-session', {'sessionId': 's', 'name': 'x'})
        self.assertEqual(res.status_code, 500)
        self.assertNotIn('secret', res.get_data(as_text=True))

    # capture
    def test_stop_needs_a_device_and_stops_only_that_one(self):
        self.record('D1')
        self.record('D2', 's2')
        self.assertEqual(self.call('POST', '/api/capture/stop', {}).status_code, 400)
        self.assertTrue(self.mod._capture_states['D1']['active'])
        self.assertEqual(self.call('POST', '/api/capture/stop', {'deviceId': 'D2'}).status_code, 200)
        self.assertTrue(self.mod._capture_states['D1']['active'])
        self.assertFalse(self.mod._capture_states['D2']['active'])

    def test_saving_one_device_leaves_the_other_rows_alone(self):
        self.record('D1')
        self.call('POST', '/api/capture/stop', {'deviceId': 'D1'})
        deletes = [(s, p) for s, p in self.sql if s.startswith('DELETE FROM capture_states')]
        self.assertEqual(deletes, [('DELETE FROM capture_states WHERE device_id = %s', ('D1',))])

    def test_a_failed_state_delete_is_retried_with_the_next_save(self):
        self.record('D1')
        with mock.patch.object(self.mod, 'get_db_cursor', side_effect=RuntimeError('db down')):
            self.assertEqual(self.call('POST', '/api/capture/stop', {'deviceId': 'D1'}).status_code, 200)
        self.record('D2', 's2')
        self.mod._save_capture_states('D2')
        self.assertIn(('DELETE FROM capture_states WHERE device_id = %s', ('D1',)), self.sql)

    def test_starting_a_device_keeps_states_that_failed_to_load(self):
        self.rows.append(([], None))
        self.call('POST', '/api/capture/start', {'deviceId': 'D2', 'phases': ['R']})
        self.assertFalse([s for s, _ in self.sql if s.startswith('DELETE FROM capture_states')])

    def test_interval_needs_a_device_and_is_saved(self):
        self.record('D1')
        self.record('D2', 's2')
        self.assertEqual(self.call('POST', '/api/capture/interval', {'interval': 60}).status_code, 400)
        self.assertEqual(self.call('POST', '/api/capture/interval', {'deviceId': 'D1', 'interval': 'x'}).status_code, 400)
        self.assertEqual(self.call('POST', '/api/capture/interval', {'deviceId': 'D1', 'interval': 60}).status_code, 200)
        self.assertEqual(self.mod._capture_states['D1']['interval'], 60)
        self.assertEqual(self.mod._capture_states['D2']['interval'], 15)
        saved = [p for s, p in self.sql if s.startswith('INSERT INTO capture_states')]
        self.assertEqual([p[0] for p in saved], ['D1'])
        self.assertEqual(json.loads(saved[0][1])['interval'], 60)

    def test_start_refuses_a_device_that_is_not_registered(self):
        self.assertEqual(self.call('POST', '/api/capture/start', {'deviceId': 'x' * 60, 'phases': ['R']}).status_code, 404)
        self.mod._start_thread.assert_not_called()

    def test_start_still_works_while_the_database_is_down(self):
        with mock.patch.object(self.mod, 'get_db_cursor', side_effect=RuntimeError('db down')):
            res = self.call('POST', '/api/capture/start', {'deviceId': 'D1', 'phases': ['R']})
        self.assertEqual((res.status_code, res.get_json()['persisted']), (200, False))

    def test_start_twice_is_refused(self):
        self.rows.extend([([], None), ([], None)])
        body = {'deviceId': 'D1', 'phases': ['R']}
        self.assertEqual(self.call('POST', '/api/capture/start', body).status_code, 200)
        self.assertEqual(self.call('POST', '/api/capture/start', body).status_code, 409)

    # sessions
    def test_a_running_session_cannot_be_deleted_cleared_or_shifted(self):
        self.record('D1', 's1')
        self.assertEqual(self.call('POST', '/api/capture/delete-session', {'sessionId': 's1'}).status_code, 409)
        self.assertEqual(self.call('DELETE', '/api/devices/D1').status_code, 409)
        self.assertEqual(self.sql, [])
        self.assertEqual(self.call('POST', '/api/capture/delete-session', {'sessionId': 'old'}).status_code, 200)

    def test_a_session_that_is_still_finishing_cannot_be_deleted(self):
        self.record('D1', 's1')
        self.call('POST', '/api/capture/stop', {'deviceId': 'D1'})
        self.assertEqual(self.call('POST', '/api/capture/delete-session', {'sessionId': 's1'}).status_code, 409)
        self.mod._finalize_bg('s1', 'D1', 0, None, None, None, 0)
        self.assertEqual(self.call('POST', '/api/capture/delete-session', {'sessionId': 's1'}).status_code, 200)

    def test_renaming_a_running_session_renames_its_next_rows(self):
        self.record('D1', 's1')
        self.call('POST', '/api/capture/rename-session', {'sessionId': 's1', 'name': 'Baru'})
        self.assertEqual(self.mod._capture_states['D1']['session_name'], 'Baru')

    def test_session_sensor_rename_keeps_the_case_of_the_phase(self):
        self.call('POST', '/api/capture/rename-session-sensor', {'sessionId': 's1', 'phase': 'Freezer_01', 'name': 'X'})
        self.assertEqual(self.sql[-1][1], ('X', 's1', 'Freezer_01'))

    # devices
    def test_unknown_devices_are_not_created_or_changed(self):
        for path, body in [
            ('rename', {'name': 'X', 'store_id': '2JC2'}), ('sensors/R/rename', {'name': 'X'}),
            ('sensors/R/color', {'color': '#112233'}), ('sensors/R/enabled', {'enabled': True}),
        ]:
            self.assertEqual(self.call('POST', f'/api/devices/NOPE/{path}', body).status_code, 404, path)
        self.assertFalse([s for s, _ in self.sql if s.startswith(('INSERT', 'UPDATE'))])

    def test_enabled_must_be_a_boolean(self):
        self.assertEqual(self.call('POST', '/api/devices/D1/sensors/R/enabled', {'enabled': 'false'}).status_code, 400)

    def test_sensor_edits_lock_the_row(self):
        self.rows.append(([{'phase': 'R', 'name': 'a'}],))
        self.assertEqual(self.call('POST', '/api/devices/D1/sensors/R/rename', {'name': 'b'}).status_code, 200)
        self.assertTrue(self.sql[0][0].endswith('FOR UPDATE'))

    def test_commands_the_portal_does_not_use_are_gone(self):
        self.record('D1')
        for path in (
            'capture/clear-all', 'capture/shift_time', 'devices/Plug-1/relay',
            'devices/D1/hourly-capture', 'hourly-capture/trigger-all',
        ):
            self.assertEqual(self.call('POST', f'/api/{path}', {'deviceId': 'D1', 'state': 'ON'}).status_code, 410, path)
        self.mod._mqtt_client.publish.assert_not_called()
        self.assertEqual(self.sql, [])

    def test_a_recording_phase_cannot_be_switched_off_or_removed(self):
        self.record('D1')['enabled_phases'] = ['R']
        self.assertEqual(self.call('POST', '/api/devices/D1/sensors/r/enabled', {'enabled': False}).status_code, 409)
        self.assertEqual(self.call('DELETE', '/api/devices/D1/sensors/R').status_code, 409)
        self.rows.append(([{'phase': 'S'}],))
        self.assertEqual(self.call('POST', '/api/devices/D1/sensors/S/enabled', {'enabled': False}).status_code, 200)

    def test_a_list_body_is_a_bad_request(self):
        for path in ('capture/start', 'capture/stop', 'capture/interval', 'capture/rename-session', 'devices/D1/rename'):
            self.assertEqual(self.call('POST', f'/api/{path}', [1]).status_code, 400, path)

    # state that survives a restart
    def persisted(self):
        """Replays the statements sent to capture_states."""
        rows = {}
        for sql, params in self.sql:
            if sql.startswith('INSERT INTO capture_states'):
                rows[params[0]] = json.loads(params[1])
            elif sql.startswith('DELETE FROM capture_states'):
                rows.pop(params[0], None)
        return rows

    def test_a_slow_save_cannot_bring_a_stopped_recording_back(self):
        self.record('D1')
        started, release = threading.Event(), threading.Event()
        real = self.mod.get_db_cursor

        @contextlib.contextmanager
        def slow_first():
            if not started.is_set():
                started.set()
                release.wait(5)
            with real() as cur:
                yield cur

        with mock.patch.object(self.mod, 'get_db_cursor', slow_first):
            saver = threading.Thread(target=self.mod._save_capture_states, args=('D1',))
            saver.start()
            started.wait(5)
            stopper = threading.Thread(target=self.mod._stop_device_capture, args=('D1',))
            stopper.start()
            stopper.join(0.3)
            release.set()
            saver.join(5)
            stopper.join(5)
        self.assertEqual(self.persisted(), {})

    def test_a_failing_state_file_does_not_stop_the_database_write(self):
        self.record('D1')
        with mock.patch('builtins.open', side_effect=OSError('disk full')):
            self.assertTrue(self.mod._save_capture_states('D1'))
        self.assertEqual(list(self.persisted()), ['D1'])

    def failing_for(self, bad_device, error):
        real = self.mod.get_db_cursor

        @contextlib.contextmanager
        def cursor():
            with real() as cur:
                execute = cur.execute

                def check(sql, params=None):
                    if params and params[0] == bad_device:
                        raise error
                    execute(sql, params)
                cur.execute = check
                yield cur
        return mock.patch.object(self.mod, 'get_db_cursor', cursor)

    def test_one_broken_row_does_not_hold_back_other_devices(self):
        self.record('A-bad')
        self.record('EM-1', 's2')
        with self.failing_for('A-bad', ValueError('value too long')):
            self.assertFalse(self.mod._save_capture_states('A-bad'))
            self.assertTrue(self.mod._save_capture_states('EM-1'))
        self.assertEqual(list(self.persisted()), ['EM-1'])

    def test_an_unreachable_database_is_tried_once_per_save(self):
        tries = []

        def down():
            tries.append(1)
            raise sys.modules['psycopg2'].OperationalError('gone')
        for device in ('D1', 'D2', 'D3'):
            self.record(device, device)
            self.mod._mark_state_pending(device)
        with mock.patch.object(self.mod, 'get_db_cursor', down):
            self.assertFalse(self.mod._save_capture_states('D1'))
        self.assertEqual(len(tries), 1)

    def test_a_stuck_database_does_not_block_commands_forever(self):
        self.record('D1')
        held, release = threading.Event(), threading.Event()

        def hold():
            with self.mod._state_io_lock:
                held.set()
                release.wait(5)
        holder = threading.Thread(target=hold)
        holder.start()
        held.wait(5)
        with mock.patch.object(self.mod, '_STATE_LOCK_WAIT_S', 0.1):
            res = self.call('POST', '/api/capture/stop', {'deviceId': 'D1'})
        release.set()
        holder.join(5)
        self.assertEqual((res.status_code, res.get_json()['persisted']), (200, False))
        self.mod._retry_state_work()
        self.assertIn(('DELETE FROM capture_states WHERE device_id = %s', ('D1',)), self.sql)

    def test_a_save_that_finishes_late_keeps_the_newer_request_pending(self):
        self.record('D1')
        writing, release = threading.Event(), threading.Event()
        real = self.mod.get_db_cursor

        @contextlib.contextmanager
        def slow_first():
            first = not writing.is_set()
            writing.set()
            if first:
                release.wait(5)
            with real() as cur:
                yield cur

        with mock.patch.object(self.mod, 'get_db_cursor', slow_first), mock.patch.object(self.mod, '_STATE_LOCK_WAIT_S', 0.1):
            saver = threading.Thread(target=self.mod._save_capture_states, args=('D1',))
            saver.start()
            writing.wait(5)
            self.assertEqual(self.mod._stop_device_capture('D1'), (True, False))
            release.set()
            saver.join(5)
        self.assertEqual(list(self.persisted()), ['D1'])
        self.mod._retry_state_work()
        self.assertEqual(self.persisted(), {})
        with open(self.mod.STATE_FILE) as f:
            self.assertEqual(json.load(f), {})

    def test_stop_says_when_it_could_not_be_stored(self):
        self.record('D1')
        with mock.patch.object(self.mod, 'get_db_cursor', side_effect=RuntimeError('db down')):
            self.assertIs(self.call('POST', '/api/capture/stop', {'deviceId': 'D1'}).get_json()['persisted'], False)
        self.mod._retry_state_work()
        self.assertIn(('DELETE FROM capture_states WHERE device_id = %s', ('D1',)), self.sql)
        self.record('D1')
        self.assertIs(self.call('POST', '/api/capture/stop', {'deviceId': 'D1'}).get_json()['persisted'], True)

    def test_a_session_stays_busy_until_its_worker_has_really_ended(self):
        self.record('D1', 's1')
        self.call('POST', '/api/capture/stop', {'deviceId': 'D1'})
        worker = mock.MagicMock()
        worker.is_alive.side_effect = [True, True, True, False, False]
        seen = []
        worker.join.side_effect = lambda timeout=None: seen.append(self.mod._is_session_busy('s1'))
        self.mod._finalize_bg('s1', 'D1', 0, threading.Event(), worker, None, 0)
        self.assertEqual(seen, [True, True])
        self.assertFalse(self.mod._is_session_busy('s1'))

    def load_rows(self, rows, fail=False):
        @contextlib.contextmanager
        def cursor():
            if fail:
                raise RuntimeError('db down')
            cur = FakeCursor(self.sql, [])
            cur.fetchall = lambda: rows
            yield cur
        return mock.patch.object(self.mod, 'get_db_cursor', cursor)

    def saved(self, session):
        return {'active': True, 'session_id': session, 'session_name': 'N', 'device_id': 'D1', 'interval': 30}

    def test_recordings_resume_after_a_restart(self):
        with self.load_rows([('D1', self.saved('s1'))]):
            self.mod._load_capture_states()
        self.assertEqual(self.mod._capture_states['D1']['session_id'], 's1')
        self.mod._start_thread.assert_called_once_with('D1')

    def test_a_failed_load_is_retried_and_blocks_deletes_until_then(self):
        with mock.patch.dict(os.environ, {'DATABASE_URL': 'postgresql://x'}):
            with self.load_rows([], fail=True):
                self.mod._load_capture_states()
            self.assertFalse(self.mod._state_load_complete)
            for method, path, body in [('POST', '/api/capture/delete-session', {'sessionId': 's1'}), ('DELETE', '/api/devices/D1', None)]:
                self.assertEqual(self.call(method, path, body).status_code, 503, path)
            with self.load_rows([('D1', self.saved('s1'))]):
                self.mod._retry_state_work()
        self.assertTrue(self.mod._state_load_complete)
        self.assertEqual(self.mod._capture_states['D1']['session_id'], 's1')

    def test_a_late_load_never_replaces_what_happened_since_boot(self):
        self.mod._state_load_complete = False
        self.rows.append(([], None))
        self.call('POST', '/api/capture/start', {'deviceId': 'D1', 'phases': ['R']})
        new = self.mod._capture_states['D1']['session_id']
        self.record('D2', 's2')
        self.call('POST', '/api/capture/stop', {'deviceId': 'D2'})
        with self.load_rows([('D1', self.saved('old')), ('D2', self.saved('s2'))]):
            self.mod._retry_state_work()
        self.assertEqual(self.mod._capture_states['D1']['session_id'], new)
        self.assertFalse(self.mod._capture_states['D2']['active'])

    def test_a_late_sample_keeps_the_name_and_count_of_its_own_session(self):
        meta = {'session_name': 'Lama', 'sensor_names': {}, 'device_name': 'D1', 'store_id': None}
        state = self.record('D1', 's2')
        state['session_name'] = 'Baru'
        extras = types.ModuleType('psycopg2.extras')
        sent = []
        extras.execute_values = lambda cur, sql, rows: sent.extend(rows)
        with mock.patch.dict(sys.modules, {'psycopg2.extras': extras}):
            self.mod._do_capture_io('D1', 's1', 1_700_000_000, 15, [None], [0], ['R'], 0, meta)
            self.assertEqual((sent[0][11], sent[0][12]), ('s1', 'Lama'))
            self.assertEqual(state['count'], 0)
            self.mod._do_capture_io('D1', 's2', 1_700_000_000, 15, [None], [0], ['R'], 0, meta)
        self.assertEqual(state['count'], 1)

    def test_a_failed_rename_restores_the_name_in_memory(self):
        self.record('D1', 's1')
        with mock.patch.object(self.mod, 'get_db_cursor', side_effect=RuntimeError('db down')):
            self.assertEqual(self.call('POST', '/api/capture/rename-session', {'sessionId': 's1', 'name': 'Baru'}).status_code, 500)
            self.assertEqual(self.call('POST', '/api/devices/D1/rename', {'name': 'X', 'store_id': 'y'}).status_code, 500)
        self.assertEqual(self.mod._capture_states['D1']['session_name'], 'Lama')
        self.assertEqual(self.mod._capture_states['D1']['device_name'], 'D1')

    def test_the_name_in_memory_changes_before_the_rows(self):
        self.record('D1', 's1')
        seen = []

        @contextlib.contextmanager
        def cursor():
            seen.append(self.mod._capture_states['D1']['session_name'])
            yield FakeCursor(self.sql, [])
        with mock.patch.object(self.mod, 'get_db_cursor', cursor):
            self.call('POST', '/api/capture/rename-session', {'sessionId': 's1', 'name': 'Baru'})
        self.assertEqual(seen[0], 'Baru')


if __name__ == '__main__':
    unittest.main()
