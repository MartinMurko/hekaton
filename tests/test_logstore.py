import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from seedwatch.explorer import Explorer
from tests.test_explorer import source, T


class StoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root/'vit-3').mkdir()
        (self.root/'vit-3/risk.jsonl').write_text(json.dumps({'t': T, 'n': 5, 'risk': 1})+'\n')
        self.raw = self.root/'vp-logs-vit-3-errorlog.txt'
        self.raw.write_bytes(b''.join([
            source((T+60)*1000, 'Future EIS_X.FUTURE.PRIMARY code=500', level='ERROR'),
            source(T*1000, 'Operation alpha EIS_X.A.PRIMARY code=200', instance='aa'),
            source((T+1)*1000, 'Operation beta EIS_X.B.PRIMARY code=200', instance='bb'),
            source((T+2)*1000, 'Operation gamma EIS_X.C.PRIMARY code=500', level='ERROR'),
            source((T+3)*1000, 'Straße warning 123', level='WARN'),
            source((T+4)*1000, 'Trace 345', level='TRACE'),
            source((T-1)*1000, 'Before window'),
        ]))
        self.legacy = Explorer(self.root, [self.raw])
        self.x = Explorer(self.root, [self.raw], indexed=True)
        self.addCleanup(self.x.close)
        self.prepare()

    def prepare(self):
        store = self.x.store('vit-3')
        for _ in range(1000):
            state = store.ensure()
            if state['ready'] or state['error']:
                break
            time.sleep(.01)
        self.assertIsNone(state['error'])
        self.assertTrue(state['ready'])

    def query(self, x=None, **kw):
        return (x or self.x).query({'env': 'vit-3', 'start': T, 'end': T+60, **kw})

    def compare(self, **kw):
        old, new = self.query(self.legacy, **kw), self.query(**kw)
        self.assertEqual(new['engine'], 'duckdb')
        self.assertEqual(new['scanned_bytes'], 0)
        for key in ('total_records', 'interval_records', 'total_rows', 'hidden_records', 'complete'):
            self.assertEqual(old[key], new[key], (key, kw))
        return old, new

    def test_time_severity_search_and_service_counts_match_source_reader(self):
        for params in [{}, {'min_level': 'INFO'}, {'min_level': 'WARN'}, {'min_level': 'ERROR'},
                       {'level': 'WARN+'}, {'level': 'INFO'}, {'text': 'STRASSE'},
                       {'service': 'CRYPT'}, {'text': 'A.state'}, {'start': T+1, 'end': T+3}]:
            with self.subTest(params=params):
                self.compare(**params)

    def test_groups_originals_and_entity_counts_match(self):
        old, new = self.compare()
        a = {g['id']: g for g in old['rows']}
        for g in new['rows']:
            before = a[g['id']]
            for key in ('pattern_id', 'pattern', 'count', 'first', 'last', 'instances', 'message'):
                self.assertEqual(g[key], before[key], key)
            self.assertEqual({e['name']: e['count'] for e in g['entities']},
                             {e['name']: e['count'] for e in before['entities']})
            from seedwatch.fields import full
            original = self.x.source({'env': 'vit-3', **g['source']})['raw'].encode()
            self.assertEqual(g['message'], full(original)['message'])

    def test_individual_records_pagination_and_hidden_patterns(self):
        old, new = self.compare(mode='records', offset=1, limit=2)
        self.assertEqual([r['source'] for r in old['rows']], [r['source'] for r in new['rows']])
        pid = new['rows'][0]['pattern_id']
        for mode in ('records', 'grouped'):
            self.compare(mode=mode, exclude=pid)
            self.compare(mode=mode, only=pid)

    def test_queries_do_not_parse_or_normalize_source_again(self):
        with patch('seedwatch.fields.full', side_effect=AssertionError('raw parse')), \
             patch('seedwatch.explorer.pattern', side_effect=AssertionError('normalize')):
            r = self.query(text='Operation', min_level='INFO')
            self.assertEqual(r['total_records'], 3)
            self.query(mode='records', min_level='WARN')

    def test_replay_boundaries_extension_and_snapshot_only_initial_query(self):
        r = self.query(end=T+120, asof=T+60, advance='0')
        self.assertEqual(r['total_records'], 5)
        self.assertTrue(all(row['t'] < (T+60)*1000 for row in r['rows']))
        extended = self.x.query({'job': r['job'], 'extend_to': T+120, 'asof': T+120})
        self.assertEqual(extended['total_records'], 6)
        self.assertEqual(extended['scanned_bytes'], 0)
        with self.assertRaises(ValueError):
            self.x.query({'job': r['job'], 'asof': T})

    def test_custom_rules_use_literal_wildcards_and_preserve_outcomes(self):
        rule = {'name': 'Operations', 'template': 'Operation <*>', 'level': '', 'service': '', 'logger': 'A.state'}
        state = self.x.update_rules({'env': 'vit-3', 'action': 'save', 'revision': 0, 'rule': rule})
        # Same persisted rule state, independently evaluated by Python and SQL.
        old, new = self.compare()
        self.assertEqual(new['total_rows'], 4)  # 200 codes merge; 500 stays separate.
        self.assertEqual({r['rule_name'] for r in new['rows'] if r['rule_id']}, {'Operations'})
        preview = self.x.preview_rule({'env': 'vit-3', 'job': new['job'], 'id': state['rules'][0]['id'], 'rule': rule})
        self.assertEqual(preview['matched'], 3)
        self.assertEqual(preview['earlier_rules'], [])
        self.assertEqual(self.query(advance='0')['scanned_bytes'], 0)
        # SQL LIKE metacharacters stay literal in the dashboard's rule language.
        literal = {**rule, 'template': 'Operation [_%] <*>', 'name': 'Literal'}
        self.x.update_rules({'env': 'vit-3', 'action': 'save', 'revision': 1, 'rule': literal})
        self.compare()

    def test_persisted_store_reopens_without_parsing(self):
        self.x.close()
        self.x = Explorer(self.root, [self.raw], indexed=True)
        self.addCleanup(self.x.close)
        with patch('seedwatch.fields.full', side_effect=AssertionError('rebuild')):
            self.prepare()
            self.assertEqual(self.query()['total_records'], 5)

    def test_rule_metacharacters_are_literal_when_the_rule_really_matches(self):
        with self.raw.open('ab') as f:
            f.write(source((T+5)*1000, 'Literal [_%]\\ value42'))
        self.prepare()
        rule = {'name': 'Literal', 'template': 'Literal [_%]\\ <*>', 'level': '', 'service': '', 'logger': ''}
        self.x.update_rules({'env': 'vit-3', 'action': 'save', 'revision': 0, 'rule': rule})
        old, new = self.compare()
        self.assertEqual(sum(bool(r['rule_id']) for r in new['rows']), 1)
        self.assertEqual({r['pattern_id'] for r in old['rows']}, {r['pattern_id'] for r in new['rows']})

    def test_source_growth_invalidates_store_and_queries(self):
        r = self.query()
        with self.raw.open('ab') as f:
            f.write(source((T+5)*1000, 'Additional record'))
        with self.assertRaisesRegex(ValueError, 'Sources changed'):
            self.x.query({'job': r['job']})
        self.prepare()
        self.assertEqual(self.query()['total_records'], 6)

    def test_interrupted_build_resumes_without_duplicates(self):
        from seedwatch.logstore import LogStore
        self.x.close()
        # Force an interrupted metadata state after a committed batch.
        self.x = Explorer(self.root, [self.raw], indexed=True)
        self.addCleanup(self.x.close)
        self.prepare()
        store = self.x.store('vit-3')
        with store.cursor() as db:
            meta = json.loads(db.execute('SELECT value FROM metadata').fetchone()[0])
            meta['complete'] = False
            LogStore._save_meta(db, meta)
        self.x.close()
        self.x = Explorer(self.root, [self.raw], indexed=True)
        self.addCleanup(self.x.close)
        self.prepare()
        self.assertEqual(self.query()['total_records'], 5)


if __name__ == '__main__':
    unittest.main()
