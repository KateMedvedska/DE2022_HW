"""
Tests for main.py
"""
from unittest import TestCase, mock
from .. import main


class MainFunctionTestCase(TestCase):
    """
    classmethod setUpClass do not work in my env/project
    correct work only with next code
    """
    main.app.testing = True
    client = main.app.test_client()

    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()


    @mock.patch('lect_02.ht_lect_02.general.api_client.get_sales')
    def test_return_400_date_param_missed_path_param_missed(
            self,
            get_sales_mock: mock.MagicMock
    ):
        """
        Raise 400 HTTP code when no 'date' param
        """
        resp = self.client.post(
            '/',
            json={
                'auth': 'djfsdkfjhdskf',
                'raw_dir': '/foo/bar/',
                # no 'date' set!
            },
        )
        self.assertEqual(400, resp.status_code)

    @mock.patch('lect_02.ht_lect_02.general.api_client.get_sales')
    def test_return_400_raw_dir_param_missed(
            self,
            get_sales_mock: mock.MagicMock
    ):
        """
        Raise 400 HTTP code when no 'date' param
        """
        resp = self.client.post(
            '/',
            json={
                'auth': 'djfsdkfjhdskf',
                'date': '2022-01-01',
                # no 'raw_dir' set!
            },
        )
        self.assertEqual(400, resp.status_code)

    @mock.patch('lect_02.ht_lect_02.general.api_client.get_sales')
    def test_api_get_sales_called(
            self,
            get_sales_mock: mock.MagicMock
    ):
        """
        Test whether api_client.get_sales is called with proper params
        """
        auth = None
        fake_date = '2022-01-01'
        path = '/foo/bar/'
        self.client.post(
            '/',
            json={
                'auth': auth,
                'date': fake_date,
                'raw_dir': path,
            },
        )
        get_sales_mock.assert_called_with(auth=auth, date=fake_date, raw_dir=path)

    @mock.patch('lect_02.ht_lect_02.general.api_client.get_sales')
    def test_return_201_when_all_is_ok(
            self,
            get_sales_mock: mock.MagicMock
    ):
        get_sales_mock.return_value = None
        resp = self.client.post(
            '/',
            json={
                'auth': 'djfsdkfjhdskf',
                'date': '2022-01-01',
                'raw_dir': '/foo/bar/'
            },
        )
        self.assertEqual(201, resp.status_code)
