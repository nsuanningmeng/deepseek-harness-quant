"""Deterministic, isolated synthetic fixtures; never used as production market data."""
import copy
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd
import yaml

from data import research_factors as factors


class FactorResearchTests(unittest.TestCase):
    def setUp(self):
        self.cfg = factors.load_config()

    def fields(self, n=30):
        index = pd.bdate_range("2025-01-02", periods=n).strftime("%Y-%m-%d")
        prices = pd.DataFrame({"600000.SH": np.arange(n,dtype=float)+100}, index=index)
        return {"open":prices,"high":prices+1,"low":prices-1,"close":prices+.5,
                "volume":prices*10,"turn":prices/100,"is_st":prices*0}

    def test_holding_period_uses_next_open_and_exact_mature_exit(self):
        fields = self.fields()
        gross, net = factors.forward_returns(fields,20,self.cfg)
        self.assertAlmostEqual(gross.iloc[0,0],121/101-1)
        costs=self.cfg["costs"]
        buy=(costs["commission_bps_each_side"]+costs["slippage_bps_each_side"])/10000
        sell=(costs["commission_bps_each_side"]+costs["slippage_bps_each_side"]+costs["sell_stamp_duty_bps"])/10000
        self.assertAlmostEqual(net.iloc[0,0],121*(1-sell)/(101*(1+buy))-1)
        self.assertTrue(gross.tail(21).isna().all().all())
        self.assertTrue(net.tail(21).isna().all().all())

    def test_exact_session_suspension_or_one_price_never_fills_forward(self):
        fields=self.fields()
        fields["volume"].iloc[1,0]=0
        gross,_=factors.forward_returns(fields,20,self.cfg)
        self.assertTrue(pd.isna(gross.iloc[0,0]))
        fields=self.fields();fields["high"].iloc[21,0]=fields["low"].iloc[21,0]
        gross,_=factors.forward_returns(fields,20,self.cfg)
        self.assertTrue(pd.isna(gross.iloc[0,0]))

    def test_factor_formula_is_causal_and_missing_turn_is_not_neutral_rank(self):
        fields=self.fields(300)
        bars=pd.DataFrame([{**{key:frame.iloc[i,0] for key,frame in fields.items()},"date":date,"code":"600000.SH"}
                           for i,date in enumerate(fields["close"].index)])
        _,before,_=factors.factor_panels(bars,self.cfg)
        changed=bars.copy();changed.loc[changed.index>250,"close"]*=3
        _,after,_=factors.factor_panels(changed,self.cfg)
        for name in before:
            pd.testing.assert_frame_equal(before[name].iloc[:251],after[name].iloc[:251])
        missing=bars.copy();missing.loc[290,"turn"]=np.nan
        _,raw,_=factors.factor_panels(missing,self.cfg)
        self.assertTrue(pd.isna(raw["turn_low"].iloc[299,0]))
        self.assertEqual(self.cfg["factors"]["new_high_250"]["direction"],0)

    def test_rank_ic_null_variance_and_hac_overlap_correction(self):
        frame=pd.DataFrame([[1,2,3],[3,2,1],[1,1,1]],index=["a","b","c"])
        label=pd.DataFrame([[3,4,5],[3,4,5],[3,4,5]],index=frame.index)
        ic,count=factors.rank_ic(frame,label,3)
        self.assertAlmostEqual(ic.loc["a"],1)
        self.assertAlmostEqual(ic.loc["b"],-1)
        self.assertTrue(pd.isna(ic.loc["c"]))
        self.assertIsNone(factors.statistics(pd.Series([1.,1.,1.]),20)["icir"])
        values=pd.Series(.2+.1*np.sin(np.arange(300)/12))
        stat=factors.statistics(values,20)
        naive=values.mean()/(values.std(ddof=1)/np.sqrt(len(values)))
        self.assertLess(abs(stat["hac_t"]),abs(naive))

    def test_missing_data_fails_without_completed_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg=copy.deepcopy(self.cfg);cfg["runtime_dir"]=tmp
            with patch.object(factors.context,"read_bars",side_effect=ValueError("缺少真实日线")):
                with self.assertRaises(ValueError):
                    factors.build(cfg)
            state=json.loads((Path(tmp)/"factors-state.json").read_text())
            self.assertEqual(state["status"],"failed")
            self.assertEqual(sorted(path.name for path in Path(tmp).iterdir()),["factors-state.json"])

    def test_full_build_has_seven_actual_factors_and_explicit_sample_limits(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);cache=root/"cache";cache.mkdir();runtime=root/"runtime";runtime.mkdir()
            dates=pd.bdate_range("2023-01-02",periods=430).strftime("%Y-%m-%d")
            codes=[f"600{i:03}.SH" for i in range(8)]
            universe=root/"universe.json";universe.write_text(json.dumps({"symbols":codes}))
            research=root/"research.yaml";research.write_text(yaml.safe_dump({"cache_dir":str(cache),"analysis":{"scope":"hs300","universe_file":str(universe)}}))
            (runtime/"bootstrap-manifest.json").write_text(json.dumps({"source":"tushare-official","completed":{"daily:"+day:{"rows":8} for day in dates},"coverage":{"daily":{"end":dates[-1]}}}))
            with sqlite3.connect(cache/"bars.db") as connection:
                connection.execute("CREATE TABLE daily_bar(code TEXT,date TEXT,open REAL,high REAL,low REAL,close REAL,volume REAL,turn REAL,is_st INTEGER,source TEXT,adjust TEXT,pct_chg REAL)")
                rows=[]
                for i,day in enumerate(dates):
                    for j,code in enumerate(codes):
                        price=100+(.02+j*.003)*i+np.sin(i/(8+j))*2
                        for adjust in ("qfq","none"):
                            rows.append((code,day,price,price+1,price-1,price+.2,1000.,1+j*.1+np.sin(i/40)*.1,0,"tushare-isolated-test-fixture",adjust,.1*np.sin(i/(8+j))))
                connection.executemany("INSERT INTO daily_bar VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",rows)
            cfg=copy.deepcopy(self.cfg);cfg.update(output_dir=str(root/"output"),report_dir=str(root/"report"),runtime_dir=str(runtime),minimum_cross_section=6,minimum_mature_sections=20)
            with patch.dict(os.environ,{"LWQUANT_RESEARCH_CONFIG":str(research),"LWQUANT_CACHE_DIR":str(cache)}):
                result=factors.build(cfg,end=dates[-1])
            self.assertEqual(result["factor_count"],len(factors.FACTOR_FUNCS)+1)
            manifest=json.loads((root/"output"/f"factor_manifest_{dates[-1]}.json").read_text())
            self.assertEqual({row["code"] for row in manifest["factors"]},{*factors.FACTOR_FUNCS,"turn_low"})
            self.assertIn("不是历史时点",manifest["analysis"]["limitation"])
            snapshot=pd.read_csv(root/"output/daily_scores"/f"daily_{dates[-1]}.csv")
            self.assertTrue(snapshot["new_high_250_rank"].isna().all())
            health=pd.read_csv(root/"output/health"/f"health_{dates[-1]}.csv")
            self.assertEqual(len(health),7)
            self.assertEqual(health.loc[health.factor=="new_high_250","status"].iloc[0],"停用")
            report=json.loads((root/"report/factor_pool_report.json").read_text())
            self.assertEqual(report["data_date"],dates[-1]);self.assertEqual(len(report["factors"]),7)
            frc=json.loads((root/"output/ui_data"/f"frc_mine_rules_{dates[-1]}.json").read_text())
            self.assertIn("不是实际风险已解除",frc["note"])
            self.assertTrue(all("coefficient" in rule for rule in frc["rules"]))


if __name__=="__main__":
    unittest.main()
