import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from seedwatch.explorer import Explorer, pattern

T = 1791966480


def source(ts, message, level='INFO', instance='aa'):
    # Intentionally wrong Kafka arrival time to exercise event-time lookup.
    return b'CreateTime:1791000000000\tnull\t' + json.dumps({
        'timeMillis': ts, 'level': level, 'logger': 'A.state',
        'server': 'Alpha.ROUTING.ALL.vp-routing-'+instance,
        'message': message}, separators=(',', ':')).encode() + b'\n'


class ExplorerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        d = self.root/'vit-3'; d.mkdir()
        (d/'risk.jsonl').write_text(json.dumps({'t':T,'risk':25,'n':5})+'\n')
        (d/'oplog.minutes.jsonl').write_text(json.dumps({'t':T,'pod':{'ROUTING/aa':5}})+'\n')
        (d/'alerts.json').write_text('[]')
        self.raw = self.root/'vp-logs-vit-3-errorlog.txt'
        self.raw.write_bytes(b''.join([
            source((T+60)*1000, 'future'),
            source(T*1000, 'Failed=0 address 10.1.2.3', instance='aa'),
            source((T+1)*1000, 'Failed=0 address 10.2.3.4', instance='bb'),
            source((T+2)*1000, 'Failed=1 address 10.2.3.4', level='ERROR'),
            source((T-1)*1000, 'outside'),
        ]))
        self.x = Explorer(self.root,[self.raw])

    def query(self, **kw):
        return self.x.query({'env':'vit-3','start':T,'end':T+60,**kw})

    def test_out_of_order_exact_boundaries_and_info(self):
        # Tiny blocks place the late/older records in different source blocks.
        with patch('seedwatch.explorer.BLOCK_BYTES', 1):
            r=self.query()
        self.assertTrue(r['complete'])
        self.assertEqual(r['total_records'],3)
        self.assertEqual(r['total_rows'],2)
        normal=next(g for g in r['rows'] if g['level']=='INFO')
        self.assertEqual(normal['count'],2)
        self.assertEqual(normal['instances'],['ROUTING/aa','ROUTING/bb'])
        self.assertEqual(normal['first'],T*1000)
        self.assertEqual(normal['last'],(T+1)*1000)

    def test_preserve_outcomes_and_response_codes(self):
        self.assertNotEqual(pattern('Failed=0'),pattern('Failed=1'))
        self.assertNotEqual(pattern('response "00"'),pattern('response "05"'))
        self.assertNotEqual(pattern('status=READY'),pattern('status=FAILED'))
        self.assertNotEqual(pattern('return code 0x00000000'),pattern('return code 0x00000001'))
        self.assertNotEqual(pattern('Response {"code":200}'),pattern('Response {"code":500}'))
        self.assertEqual(pattern('Client:[id: 0x238fdcce]'),pattern('Client:[id: 0x123abcde]'))

    def test_filter_pagination_and_group_counts(self):
        r=self.query(text='failed=0',service='ROUTING',level='INFO')
        self.assertEqual((r['total_records'],r['total_rows']),(2,1))
        self.assertEqual(r['interval_records'],3)
        all_rows=self.query(mode='records')['rows']
        page=self.query(mode='records',offset=1,limit=1)['rows']
        self.assertEqual(page,[all_rows[1]])
        self.assertEqual(self.query(level='WARN+')['total_records'],1)
        self.assertEqual(self.query(service='CRYPT')['total_records'],0)

    def test_minimum_severity_includes_higher_levels_in_both_views(self):
        levels=['TRACE','DEBUG','INFO','WARN','WARNING','ERROR','SEVERE','CRITICAL','FATAL','META']
        self.raw.write_bytes(b''.join(source((T+i)*1000,'event '+lv,level=lv) for i,lv in enumerate(levels)))
        expected={'TRACE':9,'DEBUG':8,'INFO':7,'WARN':6,'ERROR':4,'CRITICAL':2}
        for mode in ['grouped','records']:
            for minimum,count in expected.items():
                with self.subTest(mode=mode,minimum=minimum):
                    r=self.query(min_level=minimum,mode=mode)
                    self.assertEqual(r['total_records'],count)
                    self.assertEqual(r['interval_records'],10)
            all_levels=self.query(min_level='',mode=mode)
            self.assertEqual(all_levels['total_records'],10)
            self.assertIn('TRACE',{r['level'] for r in all_levels['rows']})
            self.assertIn('FATAL',{r['level'] for r in all_levels['rows']})
            severe=self.query(min_level='CRITICAL',mode=mode)
            self.assertEqual({r['level'] for r in severe['rows']},{'CRITICAL','FATAL'})
        self.assertEqual(self.query(level='WARN+')['total_records'],6)

    def test_invalid_minimum_severity_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'minimum log level'):
            self.query(min_level='not-a-level')

    def test_minimum_severity_composes_with_hidden_patterns_and_replay(self):
        r=self.query(min_level='WARN',end=T+120,asof=T+60)
        self.assertEqual(r['total_records'],1)
        self.assertEqual(r['end'],T+60)
        hidden=self.query(min_level='WARN',exclude=r['rows'][0]['pattern_id'])
        self.assertEqual(hidden['total_records'],0)
        self.assertEqual(hidden['hidden_records'],1)

    def test_replay_cap_applies_before_retrieval(self):
        r=self.query(end=T+120,asof=T+60)
        self.assertEqual(r['end'],T+60)
        self.assertEqual(r['total_records'],3)
        with self.assertRaises(ValueError):
            self.query(asof=T)

    def test_source_offsets_and_path_validation(self):
        row=self.query(level='ERROR')['rows'][0]
        result=self.x.source({'env':'vit-3',**row['source']})
        self.assertIn('Failed=1',result['raw'])
        with self.assertRaises(ValueError):
            self.x.source({'env':'vit-3','file':'../../secret','offset':0})
        with self.assertRaises(ValueError):
            self.x.source({'env':'vit-3','file':self.raw.name,'offset':2})
        with self.assertRaises(ValueError):
            self.x.source({'env':'vit-3',**row['source'],'asof':T})

    def test_progressive_scanning_finishes_instead_of_rejecting_long_ranges(self):
        x=Explorer(self.root,[self.raw],scan_limit=1)
        r=x.query({'env':'vit-3','start':T,'end':T+3*3600})
        self.assertTrue(r['pending'])
        self.assertIsNone(r['notice'])
        for _ in range(20):
            if not r['pending']:
                break
            r=x.query({'job':r['job']})
        self.assertTrue(r['complete'])
        self.assertEqual(r['total_records'],4)
        self.assertEqual(r['progress'],1)

    def test_paused_snapshot_does_not_read_more_source(self):
        x=Explorer(self.root,[self.raw],scan_limit=1)
        r=x.query({'env':'vit-3','start':T,'end':T+60})
        paused=x.query({'job':r['job'],'advance':'0'})
        self.assertEqual(r['scanned_bytes'],paused['scanned_bytes'])
        self.assertEqual(r['rows'],paused['rows'])

    def test_pattern_filters_apply_in_both_views_and_preserve_counts(self):
        all_rows=self.query()
        pid=next(g['pattern_id'] for g in all_rows['rows'] if g['level']=='INFO')
        for mode in ['grouped','records']:
            hidden=self.query(exclude=pid,mode=mode)
            self.assertEqual(hidden['total_records'],1)
            self.assertEqual(hidden['hidden_records'],2)
            self.assertEqual(hidden['interval_records'],3)
            only=self.query(only=pid,mode=mode)
            self.assertEqual(only['total_records'],2)

    def test_hiding_a_pattern_hides_it_across_services(self):
        a=source(T*1000,'Recurring heartbeat for EIS_X.A.PRIMARY')
        self.raw.write_bytes(a+a.replace(b'Alpha.ROUTING',b'Alpha.CRYPT'))
        r=self.query()
        self.assertEqual(r['total_rows'],2)
        pid=r['rows'][0]['pattern_id']
        self.assertEqual({g['pattern_id'] for g in r['rows']},{pid})
        hidden=self.query(exclude=pid)
        self.assertEqual(hidden['total_records'],0)
        self.assertEqual(hidden['hidden_records'],2)

    def test_timestamps_task_ids_durations_and_entities_are_variables(self):
        a='Task with TaskRunId a8e2015d04724a559ef263422a7adba7 finished at 2026-10-14T08:37:50.139+0000. Execution time 39 msec. State OKAY.'
        b='Task with TaskRunId b8e2015d04724a559ef263422a7adba8 finished at 2026-10-14T09:42:51.149+0000. Execution time 22 msec. State OKAY.'
        self.assertEqual(pattern(a),pattern(b))
        self.assertEqual(pattern('Continue last incomplete run after EPP BAIP001C_EPP with a timeout of 60000 ms.'),
                         pattern('Continue last incomplete run after EPP BAIP003N_EPP with a timeout of 60000 ms.'))
        self.assertEqual(pattern('Error while Echo for EIS_BICISO.BMKS.PRIMARY code is: HOST_TIMEOUT'),
                         pattern('Error while Echo for EIS_BICISO.CMSATM.PRIMARY code is: HOST_TIMEOUT'))
        self.assertEqual(pattern('Loading BPM (jar): account.authorize.bpmn'),
                         pattern('Loading BPM (jar): deviceHandling.preAuthorize.bpmn'))
        self.assertEqual(pattern('Starting: MetricsComponent.StartMetrics'),
                         pattern('Starting: TaskScheduler.StartTasks'))

    def test_grouping_spans_minutes_and_retains_original_sources(self):
        self.raw.write_bytes(source(T*1000,'Echo for EIS_X.A.PRIMARY failed=1',level='ERROR')+
                             source((T+125)*1000,'Echo for EIS_X.B.PRIMARY failed=1',level='ERROR'))
        r=self.query(end=T+3*3600)
        self.assertEqual(r['total_rows'],1)
        g=r['rows'][0]
        self.assertEqual(g['count'],2)
        self.assertEqual({e['name'] for e in g['entities']},{'EIS_X.A.PRIMARY','EIS_X.B.PRIMARY'})
        self.assertEqual(len(g['samples']),2)
        self.assertEqual(g['last']-g['first'],125000)

    def test_fallback_is_explicitly_warn_only(self):
        (self.root/'vit-3'/'filtered.jsonl').write_text(json.dumps({
            'ts':T*1000,'level':'WARN','logger':'X','pod':'ROUTING/aa','msg':'Warning sample'})+'\n')
        x=Explorer(self.root,[])
        self.assertEqual(x.workspace('vit-3')['coverage'],'prepared-warn-only')
        r=x.query({'env':'vit-3','start':T,'end':T+60})
        self.assertEqual(r['coverage'],'prepared-warn-only')
        self.assertTrue(r['rows'][0]['source']['prepared'])

    def test_index_invalidates_when_file_changes(self):
        blocks=self.x._index(self.raw)
        with self.raw.open('ab') as f:
            f.write(source((T+3600)*1000,'later'))
        refreshed=self.x._index(self.raw)
        self.assertGreater(refreshed[-1][3],blocks[-1][3])

    def test_hiding_and_restoring_reuse_scanned_results(self):
        r=self.query()
        pid=r['rows'][0]['pattern_id']
        hidden=self.query(exclude=pid)
        self.assertEqual(hidden['job'],r['job'])
        self.assertEqual(hidden['scanned_bytes'],r['scanned_bytes'])
        self.assertTrue(all(row['pattern_id']!=pid for row in hidden['rows']))
        only=self.x.query({'job':r['job'],'only':pid,'advance':'0'})
        self.assertEqual(only['total_records']+hidden['total_records'],r['total_records'])
        restored=self.x.query({'job':r['job'],'advance':'0'})
        self.assertEqual(restored['rows'],r['rows'])
        self.assertEqual(restored['hidden_records'],0)

    def test_replay_extends_only_the_new_interval_without_duplicates(self):
        for mode in ['grouped','records']:
            with self.subTest(mode=mode):
                r=self.query(mode=mode)
                extended=self.x.query({'job':r['job'],'extend_to':T+120,'asof':T+120})
                fresh=Explorer(self.root,[self.raw]).query({'env':'vit-3','start':T,'end':T+120,'mode':mode})
                self.assertEqual(extended['job'],r['job'])
                self.assertEqual(extended['total_records'],4)
                self.assertEqual(extended['rows'],fresh['rows'])
                # The next delta is empty, not another pass over the cumulative interval.
                again=self.x.query({'job':r['job'],'extend_to':T+180,'asof':T+180})
                self.assertEqual(again['rows'],fresh['rows'])
                self.assertEqual(again['total_records'],4)
                with self.assertRaisesRegex(ValueError,'replay cursor'):
                    self.x.query({'job':r['job'],'asof':T+60})

    def test_replay_extension_respects_watermark_and_exclusions(self):
        r=self.query()
        pid=next(row['pattern_id'] for row in r['rows'] if row['level']=='INFO')
        extended=self.x.query({'job':r['job'],'extend_to':T+180,'asof':T+120,'exclude':pid})
        self.assertEqual(extended['end'],T+120)
        self.assertEqual(extended['total_records'],2)
        self.assertEqual(extended['hidden_records'],2)

    def test_workspace_lists_only_present_severity_levels(self):
        (self.root/'vit-3'/'oplog.minutes.jsonl').write_text(json.dumps({
            't':T,'pod':{'ROUTING/aa':5},'lv':{'INFO':3,'ERROR':1,'WARN':1,'META':2,'DEBUG':0}})+'\n')
        self.assertEqual(self.x.workspace('vit-3')['levels'],['INFO','WARN','ERROR'])


if __name__=='__main__':
    unittest.main()
