"""Risk revisions use disposable accounts and synthetic quotes only."""
from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import yaml

from autotrade.locking import ProcessLock
from paperlive.ledger import Ledger
from paperlive.risk_policy import migrate_risk_policy
from tests.test_paperlive_ledger import EXAMPLE, START, instrument, quote


class PaperRiskPolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.old = yaml.safe_load(EXAMPLE.read_text())
        self.old['runtime_dir'] = str(self.root/'account')
        self.old['broker']['initial_cash'] = 50000
        self.old['risk'].update(max_symbol_weight=.15, max_total_weight=.75,
                                max_order_notional=7500, max_drawdown=.15,
                                kill_switch_file=str(self.root/'STOP'))
        self.new = deepcopy(self.old)
        self.new['risk'].update(max_symbol_weight=.25, max_total_weight=.95,
                                max_order_notional=12500, max_drawdown=.20)
        self.clock = patch('paperlive.ledger._now', return_value=START)
        self.clock.start()
        self.addCleanup(self.clock.stop)
        self.ledger = Ledger(self.old)
        self.closed = START.replace(hour=19)
        self.stop = Path(self.old['risk']['kill_switch_file'])
        self.stop.write_text('synthetic owned waiting marker\n')

    def migrate(self, **kwargs):
        return migrate_risk_policy(self.old, self.new, 'user explicitly selected aggressive paper limits',
                                   now=kwargs.pop('now', self.closed), **kwargs)

    def read(self, statement, params=()):
        with sqlite3.connect(self.ledger.path) as con:
            return con.execute(statement, params).fetchall()

    def test_revision_preserves_account_history_stop_and_reconciliation(self):
        self.stop.unlink()
        inst = instrument()
        order = self.ledger.submit('BUY',inst.symbol,100,'10.05',inst,START,True,'original-intent')
        q = quote(START+timedelta(seconds=1),inst)
        self.ledger.match({q.symbol:q},{inst.symbol:inst},q.timestamp,True)
        self.assertEqual(order['status'],'OPEN')
        self.stop.write_text('same identity must survive\n')
        before = {table:self.read('SELECT * FROM '+table) for table in
                  ['account','orders','fills','lots','curve','quote_books']}
        started = self.read("SELECT value FROM meta WHERE key='started_at'")
        stop = (self.stop.stat().st_ino,self.stop.stat().st_mtime_ns,self.stop.read_bytes())
        result = self.migrate()
        self.assertTrue(result['applied'])
        self.assertEqual(result['changed_keys'], ['max_drawdown','max_order_notional','max_symbol_weight','max_total_weight'])
        for table, rows in before.items():
            self.assertEqual(self.read('SELECT * FROM '+table), rows)
        self.assertEqual(self.read("SELECT value FROM meta WHERE key='started_at'"), started)
        self.assertEqual((self.stop.stat().st_ino,self.stop.stat().st_mtime_ns,self.stop.read_bytes()),stop)
        updated = Ledger(self.new,existing_only=True)
        with updated._connection() as con:
            self.assertTrue(updated._reconcile(con)['ok'])
        records = self.read("SELECT payload FROM audit WHERE event='risk_policy_changed'")
        self.assertEqual(len(records),1)
        self.assertEqual(json.loads(records[0][0])['new_risk']['max_order_notional'],12500)

    def test_stale_objects_and_old_config_cannot_mutate_after_revision(self):
        self.migrate()
        with self.assertRaisesRegex(ValueError,'execution_config_changed'):
            self.ledger.snapshot(now=self.closed)
        with self.assertRaisesRegex(ValueError,'execution_config_changed'):
            Ledger(self.old)
        self.assertTrue(Ledger(self.new).snapshot(now=self.closed)['reconciliation']['ok'])

    def test_exact_retry_is_idempotent_and_audit_is_immutable(self):
        first = self.migrate();second = self.migrate(now=self.closed+timedelta(seconds=1))
        self.assertTrue(first['applied']);self.assertFalse(second['applied'])
        self.assertTrue(second['already_applied'])
        self.assertEqual(first['revision_id'],second['revision_id'])
        self.assertEqual(len(self.read("SELECT * FROM audit WHERE event='risk_policy_changed'")),1)
        with sqlite3.connect(self.ledger.path) as con:
            with self.assertRaises(sqlite3.IntegrityError):
                con.execute("DELETE FROM audit WHERE event='risk_policy_changed'")

    def test_open_orders_are_preserved_and_block_revision(self):
        self.stop.unlink();inst=instrument()
        self.ledger.submit('BUY',inst.symbol,100,'10.05',inst,START,True,'open-intent')
        self.stop.write_text('paused')
        before=self.read('SELECT * FROM orders')
        with self.assertRaisesRegex(ValueError,'open_orders'):
            self.migrate()
        self.assertEqual(before,self.read('SELECT * FROM orders'))
        self.assertEqual(self.read("SELECT value FROM meta WHERE key='execution_config'")[0][0],self.ledger.fingerprint)

    def test_running_observer_missing_stop_and_open_session_are_rejected(self):
        with ProcessLock(Path(self.old['runtime_dir'])/'runner.lock'):
            with self.assertRaises(RuntimeError):self.migrate()
        self.stop.unlink()
        with self.assertRaisesRegex(ValueError,'stop_marker'):self.migrate()
        self.stop.write_text('paused')
        with self.assertRaisesRegex(ValueError,'closed_session'):self.migrate(now=START)

    def test_initial_cash_fees_market_rules_and_stop_path_cannot_change(self):
        cases=[('broker','initial_cash',60000),('broker','commission_rate',.001),
               ('market','max_quote_age_seconds',9999),('risk','kill_switch_file',str(self.root/'OTHER')),
               ('risk','max_daily_orders',99)]
        for section,key,value in cases:
            with self.subTest(section=section,key=key):
                candidate=deepcopy(self.new);candidate[section][key]=value
                with self.assertRaises(ValueError):
                    migrate_risk_policy(self.old,candidate,'explicit reason',now=self.closed)
        self.assertEqual(self.read("SELECT value FROM meta WHERE key='execution_config'")[0][0],self.ledger.fingerprint)

    def test_corrupt_account_and_missing_account_cannot_be_repaired_by_revision(self):
        with sqlite3.connect(self.ledger.path) as con:
            con.execute("UPDATE account SET cash='60000' WHERE id=1")
        with self.assertRaisesRegex(ValueError,'reconciliation_failed'):self.migrate()
        other=deepcopy(self.old);other['runtime_dir']=str(self.root/'missing')
        candidate=deepcopy(self.new);candidate['runtime_dir']=other['runtime_dir']
        with self.assertRaisesRegex(ValueError,'existing_regular_paper_database'):
            migrate_risk_policy(other,candidate,'reason',now=self.closed)
        self.assertFalse(Path(other['runtime_dir']).exists())

    def test_stop_race_rolls_back_fingerprint_and_audit(self):
        from paperlive import risk_policy
        original=risk_policy._stop_identity;calls=[0]
        def changed(path):
            calls[0]+=1
            if calls[0]==3:path.write_text('manual STOP replacement')
            return original(path)
        with patch('paperlive.risk_policy._stop_identity',side_effect=changed):
            with self.assertRaisesRegex(ValueError,'stop_marker_changed'):self.migrate()
        self.assertEqual(self.read("SELECT value FROM meta WHERE key='execution_config'")[0][0],self.ledger.fingerprint)
        self.assertEqual(self.read("SELECT * FROM audit WHERE event='risk_policy_changed'"),[])
        self.assertEqual(self.stop.read_text(),'manual STOP replacement')

    def test_new_limits_take_effect_without_changing_fee_or_cash_rules(self):
        self.migrate();self.stop.unlink()
        new_ledger=Ledger(self.new);inst=instrument()
        next_day=START+timedelta(days=1)
        # 10,050 notional exceeds the original 7,500 cap but stays within the
        # approved 25% single-stock and 12,500 order caps including fees.
        accepted=new_ledger.submit('BUY',inst.symbol,1000,'10.05',inst,next_day,True,'new-limit')
        self.assertEqual(accepted['status'],'OPEN')
        rejected=new_ledger.submit('BUY',inst.symbol,1300,'10.05',inst,next_day,True,'over-limit')
        self.assertEqual(rejected['reason'],'max_order_notional')
        snapshot=new_ledger.snapshot(now=next_day)
        self.assertEqual(snapshot['account']['initial_cash'],'50000.00')
        self.assertTrue(snapshot['reconciliation']['ok'])


if __name__=='__main__':
    unittest.main()
