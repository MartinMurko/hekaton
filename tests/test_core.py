import unittest

from seedwatch import fields
from seedwatch.aggregate import MinuteAggregator
from seedwatch.predict import binom_kl, kl
from seedwatch.profile import _cron, classify_text
from seedwatch.template import entities, template

T0 = 1792040400000  # 2026-10-15 05:00 UTC, ms


def line(ts, level, logger, msg, server="Alpha.ROUTING.ALL.vp-routing-blue-deploy-d5dcffb77-fnmbl", corr="c1"):
    return (f'CreateTime:{ts}\tnull\t{{"timeMillis":{ts},"level":"{level}","logger":"{logger}",'
            f'"server":"{server}","correlationId":"{corr}","message":"{msg}"}}').encode()


class TemplateTest(unittest.TestCase):
    def test_hosts_become_one_template_with_entities(self):
        a = "Error while Echo for EIS_BICISO.BMKS.PRIMARY.L:10.33.73.83:38730.R:10.140.5.102:10069 caused by X"
        b = "Error while Echo for EIS_MASTERCARD_DMS.BNET4.PRIMARY.L:10.33.73.83:40444.R:172.17.62.60:16035 caused by X"
        self.assertEqual(template(a), template(b))
        self.assertEqual(entities(a), ["EIS_BICISO.BMKS.PRIMARY"])

    def test_numbers_and_files_masked(self):
        self.assertEqual(template("Continue last incomplete run after EPP BAIP002N_EPP with a timeout of 60000 ms."),
                         "Continue last incomplete run after EPP <E> with a timeout of <n> ms.")


class FieldsTest(unittest.TestCase):
    def test_nested_escaped_record_is_not_top_level(self):
        raw = b'CreateTime:1\tnull\t{"message":"{\\"level\\":\\"ERROR\\"}","level":"INFO","timeMillis":1792040400000}'
        self.assertEqual(fields.fstr(raw, fields.K_LEVEL), b"INFO")

    def test_salvage_truncated_record(self):
        raw = (b'CreateTime:1792040400000\tnull\t{"server":"x","message":"{\\"level\\":\\"ERROR\\",'
               b'\\"logger\\":\\"txm-core.X\\",\\"timeMillis\\":1792040399000,\\"throwable\\":\\"java.lang.Foo')
        ts, lvl, logger, _ = fields.oplog_head(raw)
        self.assertEqual((ts, lvl, logger), (1792040399000, b"ERROR", b"txm-core.X"))


class AggregatorTest(unittest.TestCase):
    def test_dedup_counts_repeats_per_minute(self):
        rows = []
        agg = MinuteAggregator("vit-x", "oplog", lambda r, e: rows.append((r, e)))
        for i in range(1000):
            agg.add(line(T0 + i * 50, "ERROR", "A.fail", "Boom for EIS_X.Y.PRIMARY id 42", corr=f"c{i}"))
        agg.add(line(T0 + 1000, "INFO", "A.tick", "ok"))
        agg.flush()
        (row, events), = rows
        self.assertEqual(row["n"], 1001)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["count"], 1000)

    def test_silent_minutes_flush_by_watermark(self):
        rows = []
        agg = MinuteAggregator("vit-x", "oplog", lambda r, e: rows.append(r))
        agg.add(line(T0, "INFO", "A.tick", "ok"))
        agg.add(line(T0 + 10 * 60000, "INFO", "A.tick", "ok"))
        self.assertEqual([r["t"] for r in rows], [T0 // 1000])


class NoiseTest(unittest.TestCase):
    def test_expected_error_wording_is_weed(self):
        self.assertEqual(classify_text("Init", "Expected error handler initialized successfully")[0], "weed")
        self.assertIsNone(classify_text("Conn", "Connection failed successfully retried? no: timeout"))

    def test_daily_cron_detected(self):
        counts = {d * 1440 + 8: 2 for d in range(5)}
        period, phase, share = _cron(counts, startup=set())
        self.assertEqual((period, phase), (1440, 8))


class ScoreTest(unittest.TestCase):
    def test_kl_zero_at_expectation(self):
        self.assertAlmostEqual(kl(10, 10), 0.0)
        self.assertGreater(kl(0, 400), 300)       # silence is very surprising
        self.assertEqual(binom_kl(1, 30, 0.5), 0.0)  # fewer than expected never scores


if __name__ == "__main__":
    unittest.main()
