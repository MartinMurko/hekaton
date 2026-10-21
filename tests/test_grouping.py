import json
from pathlib import Path
import tempfile
import unittest

from seedwatch.explorer import Explorer
from seedwatch.grouping import clean_rule, matches
from tests.test_explorer import source, T


class GroupingTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        for env in ('vit-3', 'vit-2'):
            d = self.root/env
            d.mkdir()
            (d/'risk.jsonl').write_text(json.dumps({'t': T, 'risk': 1, 'n': 3})+'\n')
        self.raw = self.root/'vp-logs-vit-3-errorlog.txt'
        self.raw.write_bytes(b''.join([
            source(T*1000, 'Operation alpha ended code=200', level='ERROR'),
            source((T+1)*1000, 'Operation beta ended code=200', level='ERROR'),
            source((T+2)*1000, 'Operation gamma ended code=500', level='ERROR'),
            source((T+60)*1000, 'Operation future ended code=200', level='ERROR'),
        ]))
        self.x = Explorer(self.root, [self.raw])
        self.rule = {'name': 'Operation outcomes', 'template': 'Operation <*> ended code=<*>',
                     'logger': 'A.state', 'service': 'ROUTING', 'level': 'ERROR'}

    def query(self, **kw):
        return self.x.query({'env': 'vit-3', 'start': T, 'end': T+60, **kw})

    def save(self, **kw):
        return self.x.update_rules({'env': 'vit-3', 'action': 'save', 'rule': self.rule,
                                    'revision': self.x.list_rules('vit-3')['revision'], **kw})

    def test_literal_wildcard_matching_has_no_regex_or_glob_semantics(self):
        rule = clean_rule({'name': 'Literal', 'template': 'Value [a-z]+ (ok): <*>.'})
        self.assertTrue(matches(rule, 'Value [a-z]+ (ok): anything.', '', '', 'INFO'))
        self.assertFalse(matches(rule, 'Value abc ok: anything.', '', '', 'INFO'))
        self.assertFalse(matches(rule, 'prefix Value [a-z]+ (ok): anything.', '', '', 'INFO'))
        self.assertFalse(matches(rule, 'Value [a-z]+ (ok): anything. suffix', '', '', 'INFO'))
        rule = clean_rule({'name': 'Multiple', 'template': 'A <*> B <*> C'})
        self.assertTrue(matches(rule, 'A x B y B z C', '', '', 'INFO'))
        self.assertFalse(matches(rule, 'A x C B y', '', '', 'INFO'))
        self.assertTrue(matches(rule, 'A\n x B   y C', '', '', 'INFO'))

    def test_rule_validation(self):
        for template in ('', '<*>', '<*> <*>', 'x'*2001, 'x<*>'*21):
            with self.subTest(template=template[:30]), self.assertRaises(ValueError):
                clean_rule({'name': 'Invalid', 'template': template})
        with self.assertRaises(ValueError):
            clean_rule({'name': '', 'template': 'literal'})
        with self.assertRaises(ValueError):
            clean_rule({**self.rule, 'level': 'BAD'})

    def test_scopes_and_full_message_match(self):
        rule = clean_rule(self.rule)
        msg = 'Operation alpha ended code=200'
        self.assertTrue(matches(rule, msg, 'A.state', 'ROUTING', 'ERROR'))
        for lg, svc, lv in [('B.state', 'ROUTING', 'ERROR'), ('A.state', 'CRYPT', 'ERROR'),
                            ('A.state', 'ROUTING', 'INFO')]:
            self.assertFalse(matches(rule, msg, lg, svc, lv))

    def test_saved_rule_regroups_but_preserves_outcome_codes_and_sources(self):
        before = self.query()
        self.assertEqual(before['total_rows'], 3)
        state = self.save()
        after = self.query()
        self.assertEqual(after['total_rows'], 2)
        self.assertEqual(after['total_records'], 3)
        self.assertNotEqual(before['job'], after['job'])
        self.assertEqual({r['rule_id'] for r in after['rows']}, {state['rules'][0]['id']})
        self.assertEqual(sorted(r['count'] for r in after['rows']), [1, 2])
        self.assertTrue(all('code=' in r['pattern'] for r in after['rows']))
        for row in after['rows']:
            self.assertIn(row['message'], self.x.source({'env': 'vit-3', **row['source']})['raw'])
        original = self.query(mode='records')
        self.assertEqual(len(original['rows']), 3)
        self.assertTrue(all(r['rule_id'] for r in original['rows']))
        with self.assertRaisesRegex(ValueError, 'expired'):
            self.x.query({'job': before['job']})

    def test_persistence_edit_delete_and_environment_isolation(self):
        state = self.save()
        rid = state['rules'][0]['id']
        other = Explorer(self.root, [self.raw])
        self.assertEqual(other.list_rules('vit-3'), state)
        self.assertEqual(other.list_rules('vit-2')['rules'], [])
        edited = self.save(id=rid, rule={**self.rule, 'name': 'Renamed'})
        self.assertEqual(edited['rules'][0]['name'], 'Renamed')
        self.assertEqual(len(edited['rules']), 1)
        with self.assertRaisesRegex(ValueError, 'changed'):
            self.x.update_rules({'env': 'vit-3', 'action': 'delete', 'id': rid, 'revision': 0})
        self.x.update_rules({'env': 'vit-3', 'action': 'delete', 'id': rid,
                             'revision': edited['revision']})
        self.assertEqual(self.query()['total_rows'], 3)
        self.assertEqual(Explorer(self.root, [self.raw]).list_rules('vit-3')['rules'], [])
        removed = self.x.list_rules('vit-3')
        self.assertEqual(removed['removed'][0]['rule']['name'], 'Renamed')
        restored = self.x.update_rules({'env': 'vit-3', 'action': 'restore', 'id': rid,
                                       'revision': removed['revision']})
        self.assertEqual(restored['rules'][0]['name'], 'Renamed')
        self.assertEqual(restored['removed'], [])
        self.assertEqual(self.query()['total_rows'], 2)

    def test_preview_is_a_bounded_snapshot_with_no_read_or_mutation(self):
        r = self.query(end=T+120, asof=T+60)
        request = {'env': 'vit-3', 'job': r['job'], 'asof': T+60,
                   'rule': {**self.rule, 'template': 'Operation <*> ended code=200'}}
        preview = self.x.preview_rule(request)
        self.assertEqual((preview['tested'], preview['matched'], preview['patterns']), (3, 2, 2))
        self.assertTrue(all(e['t'] < (T+60)*1000 for e in preview['examples']))
        self.assertEqual(self.query(advance='0')['scanned_bytes'], r['scanned_bytes'])
        self.assertFalse((self.root/'grouping-rules.json').exists())
        with self.assertRaisesRegex(ValueError, 'replay cursor'):
            self.x.preview_rule({**request, 'asof': T})
        with self.assertRaises(ValueError):
            self.x.preview_rule({**request, 'env': 'vit-2'})
        hidden = self.x.preview_rule({**request, 'exclude': [r['rows'][0]['pattern_id']]})
        self.assertEqual(hidden['tested'], 2)

    def test_preview_caps_examples_and_reports_overlapping_rule(self):
        state = self.save()
        self.raw.write_bytes(b''.join(source((T+i%50)*1000, f'Operation item{i} ended code=200', level='ERROR')
                                     for i in range(600)))
        r = self.query(mode='records')
        preview = self.x.preview_rule({'env': 'vit-3', 'job': r['job'], 'rule': self.rule})
        self.assertEqual(preview['tested'], 500)
        self.assertEqual(len(preview['examples']), 6)
        self.assertEqual(preview['earlier_rules'], ['Operation outcomes'])
        editing = self.x.preview_rule({'env': 'vit-3', 'job': r['job'], 'rule': self.rule,
                                      'id': state['rules'][0]['id']})
        self.assertEqual(editing['earlier_rules'], [])

    def test_first_matching_rule_wins_and_replay_can_extend(self):
        first = self.save()
        self.save(rule={**self.rule, 'name': 'Second', 'template': 'Operation <*>'})
        r = self.query()
        self.assertEqual({row['rule_name'] for row in r['rows']}, {'Operation outcomes'})
        extended = self.x.query({'job': r['job'], 'extend_to': T+120, 'asof': T+120})
        self.assertEqual(extended['total_records'], 4)
        self.assertEqual({row['rule_id'] for row in extended['rows']}, {first['rules'][0]['id']})


if __name__ == '__main__':
    unittest.main()
