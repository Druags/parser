import time
import unittest

from parsers.main import start_driver, get_users_info_test

import pandas as pd

import data


class TestQueries(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.start_time = time.time()
        self.driver = start_driver()

    def tearDown(self):
        t = time.time() - self.start_time
        print('%s: %.3f' % (self.id(), t))

    def test_positive_get_user_info(self):
        links = pd.DataFrame({'url': [919160]})
        result = get_users_info_test(self.driver, links)
        self.assertCountEqual(data.my_user_info.iloc[0].values,
                              result.iloc[0].values)

    def test_negative_get_user_info(self):
        links = pd.DataFrame({'url': [123]})
        result = get_users_info_test(self.driver, links)
        self.assertTrue(result.empty)


