import unittest

from validation.check_research_complete import _wait_for_minute_status


class ResearchAuditWaitTests(unittest.TestCase):
    def test_waits_for_all_three_real_proof_flags(self):
        now = [0]
        states = iter([
            {'verification_status': 'waiting_core_update'},
            {'deep_verified': True, 'cache_verified': True, 'current_date_ready': False},
            {'deep_verified': True, 'cache_verified': True, 'current_date_ready': True},
        ])
        def read(*, wait):
            self.assertTrue(wait)
            return next(states)
        result = _wait_for_minute_status(read, 10, clock=lambda: now[0],
                                        pause=lambda seconds: now.__setitem__(0, now[0] + seconds))
        self.assertTrue(result['current_date_ready'])
        self.assertEqual(now[0], 2)

    def test_timeout_keeps_unverified_status(self):
        now = [0]
        pending = {'verification_status': 'waiting_core_update', 'deep_verified': False}
        result = _wait_for_minute_status(lambda **kwargs: pending, 2.5,
                                        clock=lambda: now[0],
                                        pause=lambda seconds: now.__setitem__(0, now[0] + seconds))
        self.assertIs(result, pending)
        self.assertEqual(now[0], 2.5)

    def test_real_failure_does_not_retry_or_hide_error(self):
        failed = {'verification_status': 'failed', 'verification_error': 'receipt mismatch'}
        result = _wait_for_minute_status(lambda **kwargs: failed, 180, clock=lambda: 0,
                                        pause=lambda seconds: self.fail('must not retry failed proof'))
        self.assertIs(result, failed)


if __name__ == '__main__':
    unittest.main()
