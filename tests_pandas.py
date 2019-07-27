from unittest import TestCase
from infra_assign import flask_app


class PandaCSVTestCase(TestCase):

    def setUp(self):
        flask_app.file_name = 'unittest.csv'
        with flask_app.app.test_client() as c:
            self.client = c

    def test_get_top_n_users(self):
        resp = self.client.post('/top_n_users', json={'top': 2})
        json_resp = resp.get_json()
        result = json_resp.get('result')
        self.assertEqual(len(result.get('uid_list')), 2)
        self.assertEqual(result.get('uid_list')[0], '5c00a8f685db9ec46dbc13d7')

    def test_get_ton_n_failure(self):
        resp = self.client.post('/top_n_users', json={'top': 'bs'})
        json_resp = resp.get_json()
        self.assertEqual(json_resp.get('status'), 501)
        self.assertEqual(json_resp.get('result'), 'top-n value should be an integer')

    def test_get_top_n_failure_exit_codes(self):
        resp = self.client.post('/top_n_failure_code', json={'top': 1})
        json_resp = resp.get_json().get('result')
        self.assertEqual(len(json_resp.get('exit_codes')), 1)
        self.assertEqual(json_resp.get('exit_codes')[0], 1)

    def test_get_build_success_rate(self):
        resp = self.client.get('/build_success_rate')
        json_resp = resp.get_json()
        self.assertEqual(json_resp.get('percentage_success_rate'), 75.0)

    def test_build_count_in_time_window(self):
        resp = self.client.post('/builds_in_window', json={'start_date': '1/11/2018',
                                                           'end_date': '3/11/2018'})
        import pdb;pdb.set_trace()
        result = resp.get_json().get('result')
        self.assertEqual(result['count'], 4)
        self.assertTrue('5c00a8f685db9ec46dbc13dc' in result['uid_list'])
