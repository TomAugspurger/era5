import unittest

import stactools.era5


class TestModule(unittest.TestCase):

    def test_version(self):
        self.assertIsNotNone(stactools.era5.__version__)
