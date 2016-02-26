import unittest

import dwmon

class CronTests(unittest.TestCase):

    def test_parse_requirements(self):
        cron_string = "CHECKHOURS0-9 CHECKMINUTES0-10 " \
            "WEEKDAYS MINNUM5 MAXNUM20 LOOKBACKSECONDS3600"
        result = dwmon.parse_requirements(cron_string)
        self.assertTrue(result["check_hours_lower"] == 0)
        self.assertTrue(result["check_hours_upper"] == 9)
        self.assertTrue(result["check_minutes_lower"] == 0)
        self.assertTrue(result["check_minutes_upper"] == 10)
        self.assertFalse(result["include_weekends"])
        self.assertTrue(result["include_weekdays"])
        self.assertTrue(result["min_num"] == 5)
        self.assertTrue(result["max_num"] == 20)
        self.assertTrue(result["lookback_seconds"] == 3600)

    def test_parse_requirements_star(self):
        cron_string = "CHECKHOURS0-9 CHECKMINUTES*/10 " \
            "WEEKDAYS MINNUM5 MAXNUM20 LOOKBACKSECONDS3600"
        result = dwmon.parse_requirements(cron_string)
        self.assertTrue(result["check_minutes_lower"] is None)
        self.assertTrue(result["check_minutes_upper"] is None)
        self.assertTrue(result["check_minutes_star"] == 10)

    def test_switched_hours_range(self):
        # 9 to 5 is a reverse range, bad! (shoul be 9 - 17 probably
        cron_string = "CHECKHOURS9-5 CHECKMINUTES0-0 " \
            "WEEKDAYS MINNUM5 MAXNUM20 LOOKBACKSECONDS1000"
        with self.assertRaisesRegexp(AssertionError, "bad hours relationship"):
            dwmon.parse_requirements(cron_string)

    def test_switched_minutes_range(self):
        # 9 to 5 is a reverse range, bad! (shoul be 9 - 17 probably
        cron_string = "CHECKHOURS0-5 CHECKMINUTES9-5 " \
            "WEEKDAYS MINNUM5 MAXNUM20 LOOKBACKSECONDS1000"
        with self.assertRaisesRegexp(
                AssertionError, "bad minutes relationship"):
            dwmon.parse_requirements(cron_string)

    def test_missing_hours(self):
        cron_string = "CHECKMINUTES9-5 " \
            "WEEKDAYS MINNUM5 MAXNUM20 LOOKBACKSECONDS1000"
        with self.assertRaisesRegexp(Exception, "missing CHECKHOURS"):
            dwmon.parse_requirements(cron_string)

    def test_switched_max_min(self):
        cron_string = "CHECKHOURS0-5 CHECKMINUTES9-15 " \
            "WEEKDAYS MINNUM500 MAXNUM20 LOOKBACKSECONDS1000"
        with self.assertRaisesRegexp(AssertionError, "bad minnum/maxnum"):
            dwmon.parse_requirements(cron_string)

    def test_minutes_out_of_range(self):
        cron_string = "CHECKHOURS0-5 CHECKMINUTES0-60 " \
            "WEEKDAYS MINNUM5 MAXNUM20 LOOKBACKSECONDS1000"
        with self.assertRaisesRegexp(AssertionError, "out of range minutes"):
            dwmon.parse_requirements(cron_string)

    def test_time_pattern_252pm_saturday(self):
        cron_string_1 = "CHECKHOURS12-18 CHECKMINUTES50-55 " \
            "WEEKDAYS WEEKENDS MINNUM5 MAXNUM20 LOOKBACKSECONDS3600"
        cron_string_2 = "CHECKHOURS12-18 CHECKMINUTES54-55 " \
            "WEEKDAYS WEEKENDS MINNUM5 MAXNUM20 LOOKBACKSECONDS3600"
        cron_string_3 = "CHECKHOURS12-18 CHECKMINUTES50-55 " \
            "WEEKDAYS MINNUM5 MAXNUM20 LOOKBACKSECONDS3600"
        epoch = 1455997930

        requirements_1 = dwmon.parse_requirements(cron_string_1)
        requirements_2 = dwmon.parse_requirements(cron_string_2)
        requirements_3 = dwmon.parse_requirements(cron_string_3)

        self.assertTrue(dwmon.matches_time_pattern(requirements_1, epoch))
        self.assertFalse(dwmon.matches_time_pattern(requirements_2, epoch))
        self.assertFalse(dwmon.matches_time_pattern(requirements_3, epoch))

