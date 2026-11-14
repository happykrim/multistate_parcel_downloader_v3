"""Offline regression tests for v3.0; no internet or external GIS service needed."""
import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import multistate_parcel_downloader_v3 as m


class NormalizationTests(unittest.TestCase):
    def test_wyoming_actual_value_and_mailing(self):
        src = m.lookup_source(m.read_registry(None), 'WY', 'Natrona')
        row = m.normalize({'FID': 11, 'parcelnb': '001229', 'ownername1': 'JONES BETTY',
                           'mailaddres': '90 JOHN ST', 'mailcity': 'CASPER', 'mailstate': 'WY',
                           'mailzipcod': '82601', 'locationad': '22 MOUNTAIN RD',
                           'landgrossa': 55.5, 'actualvalu': 251250}, src, 'FID')
        self.assertEqual(row['id'], '001229')
        self.assertEqual(row['full_name'], 'JONES BETTY')
        self.assertEqual(row['mailing_address'], '90 JOHN ST, CASPER, WY 82601')
        self.assertEqual(row['acres'], 55.5)
        self.assertEqual(row['market_value'], 251250.0)
        self.assertEqual(row['market_value_source'], 'actualvalu [ASSESSOR_ACTUAL_VALUE]')
        self.assertEqual(row['city'], '')  # Do not use mailing city as situs city

    def test_colorado_appraised_not_market(self):
        src = m.lookup_source(m.read_registry(None), 'CO', 'Weld')
        row = m.normalize({'OBJECTID': 7, 'parcel_id': '991', 'owner': 'TOWN LLC',
                           'ownerAdd': '100 FARM WAY', 'ownAddCty': 'DENVER',
                           'ownAddStt': 'CO', 'ownAddZip': '80201', 'situsAdd': '15 RANCH RD',
                           'sitAddCty': 'GREELEY', 'landAcres': 80.0,
                           'apprValTot': '$220,500', 'asedValTot': 12345}, src, 'OBJECTID')
        self.assertEqual(row['mailing_address'], '100 FARM WAY, DENVER, CO 80201')
        self.assertEqual(row['city'], 'GREELEY')
        self.assertEqual(row['appraised_value'], 220500)
        self.assertEqual(row['taxable_value'], 12345)
        self.assertEqual(row['assessed_value'], '')
        self.assertEqual(row['market_value'], '')

    def test_oklahoma_parcel_size_unit_and_totals(self):
        src = m.lookup_source(m.read_registry(None), 'OK', 'Canadian')
        row = m.normalize({'OBJECTID': 19, 'parcel_id': 'ABC10', 'owners_name': 'DOE A',
                           'parcel_size': 4356000, 'parcel_size_units': 'sq ft',
                           'total_val': 200000, 'mail_address': '200 ST',
                           'mail_city': 'EL RENO', 'mail_state': 'OK', 'mail_zip': '73036'},
                          src, 'OBJECTID')
        self.assertAlmostEqual(row['acres'], 100.0, places=4)
        self.assertEqual(row['total_value'], 200000)
        self.assertEqual(row['market_value'], '')
        self.assertEqual(row['state'], 'OK')

    def test_new_mexico_source_fields(self):
        src = m.lookup_source(m.read_registry(None), 'NM', 'Bernalillo')
        row = m.normalize({'OBJECTID': 1, 'UPC': '000123', 'OWNER': 'TEST OWNER',
                           'CompleteSiteAddress': '1 TALL TREE NM',
                           'CompleteOwnerAddress': '10 BOX ST, ALBUQUERQUE NM 87103',
                           'ACREAGE': 63.0}, src, 'OBJECTID')
        self.assertEqual(row['id'], '000123')
        self.assertEqual(row['acres'], 63.0)
        self.assertEqual(row['mailing_address'], '10 BOX ST, ALBUQUERQUE NM 87103')

    def test_geometry_equal_area_square(self):
        # 100m x 100m in EPSG:5070 is 2.47105381 acres.
        area = m.equal_area_acres({'rings': [[[0,0], [100,0], [100,100], [0,100], [0,0]]]})
        self.assertAlmostEqual(area, 10000 / m.SQM_PER_ACRE, places=8)
        hole = m.equal_area_acres({'rings': [
            [[0,0], [100,0], [100,100], [0,100], [0,0]],
            [[20,20], [20,80], [80,80], [80,20], [20,20]]
        ]})
        self.assertAlmostEqual(hole, 6400 / m.SQM_PER_ACRE, places=8)

    def test_multi_id_split_and_invalid_acreage_id(self):
        self.assertEqual(m.parcel_id_parts('4170,562943,562944'),
                         ('4170', '4170 | 562943 | 562944', 3))
        self.assertEqual(m.parcel_id_parts('0.843 AC'), ('', '', 0))
        src = m.lookup_source(m.read_registry(None), 'WY', 'Natrona')
        row = m.normalize({'FID': 22, 'parcelnb': '4170,562943'}, src, 'FID')
        self.assertEqual(row['id'], '4170')
        self.assertEqual(row['parcel_id_count'], 2)
        self.assertIn('MULTI', row['id_quality'])
        self.assertEqual(row['parcel_id_raw'], '4170,562943')

    def test_missing_identity_not_falsified(self):
        src = m.lookup_source(m.read_registry(None), 'CO', 'Weld')
        row = m.normalize({'OBJECTID': 44, 'owner': 'EXAMPLE'}, src, 'OBJECTID')
        self.assertEqual(row['id'], 'GIS_OID:44')
        self.assertIn('NOT_CAD_ID', row['id_quality'])
        self.assertEqual(row['acres'], '')
        self.assertIn('missing_acres', row['data_quality_flags'])


class OutputTests(unittest.TestCase):
    def test_local_unknown_county_csv_pipeline(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            inp = d / 'san_juan.csv'
            with inp.open('w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['OBJECTID', 'UPC', 'OWNER', 'ACREAGE', 'SITUSADD'])
                writer.writeheader()
                writer.writerow({'OBJECTID': '1', 'UPC': '12345', 'OWNER': 'JANE SMITH',
                                 'ACREAGE': '70', 'SITUSADD': '123 MAIN ST'})
                writer.writerow({'OBJECTID': '2', 'UPC': '22222', 'OWNER': 'ALICE JONES',
                                 'ACREAGE': '12', 'SITUSADD': '12 MAIN ST'})
                writer.writerow({'OBJECTID': '3', 'UPC': '33333', 'OWNER': 'LARRY BROWN',
                                 'ACREAGE': '', 'SITUSADD': '98 MAIN ST'})
            ret = m.main(['--state','NM','--counties','San Juan','--local-file',str(inp),
                          '--min-acres','50','--output',str(d/'out')])
            self.assertEqual(ret, 0)
            output = d / 'out/NM/san_juan/output'
            with (output/'san_juan_production.csv').open(encoding='utf-8-sig', newline='') as f:
                results = list(csv.DictReader(f))
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]['id'], '12345')
            with (output/'san_juan_raw_all_fields.csv').open(encoding='utf-8-sig', newline='') as f:
                raws = list(csv.DictReader(f))
            self.assertEqual(len(raws), 3)
            report = json.loads((output/'san_juan_report.json').read_text())
            self.assertEqual(report['field_coverage']['filtered_out_rows'], 2)

    def test_local_geopackage_geometry_fallback(self):
        import geopandas as gpd
        from shapely.geometry import Polygon
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            gpkg = d / 'county.gpkg'
            data = gpd.GeoDataFrame({'UPC': ['AB01'], 'OWNER': ['JANE SMITH']},
                                    geometry=[Polygon([(0, 0), (100, 0), (100, 100), (0, 100)])],
                                    crs='EPSG:5070')
            data.to_file(gpkg, layer='parcels', driver='GPKG')
            code = m.main(['--state', 'NM', '--counties', 'San Juan', '--local-file', str(gpkg),
                           '--output', str(d / 'out'), '--geometry-acres', 'auto'])
            self.assertEqual(code, 0)
            csv_path = d / 'out/NM/san_juan/output/san_juan_production.csv'
            with csv_path.open(encoding='utf-8-sig', newline='') as fh:
                row = list(csv.DictReader(fh))[0]
            self.assertAlmostEqual(float(row['acres']), 10000 / m.SQM_PER_ACRE, places=5)
            self.assertIn('GEOMETRY', row['acres_source'])

    def test_mock_arcgis_resume_and_batches(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            calls = []
            def fake_api(s, url, params, **kwargs):
                calls.append((url, dict(params)))
                if url.endswith('/0'):
                    return {'name': 'Fake Layer', 'geometryType': 'esriGeometryPolygon',
                            'objectIdField': 'OBJECTID', 'maxRecordCount': 2,
                            'fields': [{'name': n, 'type': 'esriFieldTypeOID' if n == 'OBJECTID' else 'esriFieldTypeString'}
                                       for n in ['OBJECTID', 'parcel_id', 'owner', 'landAcres', 'situsAdd', 'countyName']]}
                if params.get('returnIdsOnly') == 'true':
                    return {'objectIds': [3,2,1]}
                if params.get('returnCountOnly') == 'true':
                    return {'count': 3}
                if 'objectIds' in params:
                    ids = list(map(int, params['objectIds'].split(',')))
                    # Simulate truncation: return first ID only per response, requiring recursive recovery.
                    feats = [{'attributes': {'OBJECTID': i, 'parcel_id': f'00{i}',
                                             'owner': f'PERSON {i}', 'landAcres': 52 + i,
                                             'countyName': 'Weld', 'situsAdd': f'{i} FIRST STREET'}}
                             for i in ids[:1]]
                    return {'features': feats, 'exceededTransferLimit': len(ids)>1}
                raise ValueError('unexpected query ' + repr(params))
            with patch.object(m, 'api_json', side_effect=fake_api):
                params = ['--state','CO','--counties','Weld','--output',str(d/'out'),
                          '--batch-size','2','--geometry-acres','never','--delay','0','--assessor-enrich','off']
                self.assertEqual(m.main(params), 0)
                chunk_calls = sum('objectIds' in p for u,p in calls)
                self.assertGreaterEqual(chunk_calls, 3)
                report_path = d / 'out/CO/weld/output/weld_report.json'
                report = json.loads(report_path.read_text())
                self.assertEqual(report['downloaded_rows'], 3)
                self.assertEqual(report['missing_object_ids'], 0)
                with (d/'out/CO/weld/output/weld_production.csv').open(encoding='utf-8-sig') as f:
                    self.assertEqual(len(list(csv.DictReader(f))), 3)
                self.assertEqual(m.main(params), 0)
                self.assertEqual(sum('objectIds' in p for u,p in calls), chunk_calls)

    def test_retry_partial_missing_object_id_on_resume(self):
        with tempfile.TemporaryDirectory() as d:
            state = {'serve_missing': False}
            def fake_api(session, url, params, **kw):
                if url.endswith('/0'):
                    return {'name': 'Fake Layer', 'geometryType': 'esriGeometryPolygon',
                            'objectIdField': 'OBJECTID', 'maxRecordCount': 2,
                            'fields': [{'name':'OBJECTID','type':'esriFieldTypeOID'},
                                       {'name':'parcel_id','type':'esriFieldTypeString'},
                                       {'name':'landAcres','type':'esriFieldTypeDouble'},
                                       {'name':'countyName','type':'esriFieldTypeString'}]}
                if 'returnIdsOnly' in params:
                    return {'objectIds':[1,2]}
                if 'returnCountOnly' in params:
                    return {'count':2}
                ids = [int(x) for x in params['objectIds'].split(',')]
                return {'features':[{'attributes': {'OBJECTID': x, 'parcel_id':str(x),
                                                    'landAcres': 80, 'countyName':'Weld'}}
                                    for x in ids if x == 1 or state['serve_missing']]}
            with patch.object(m, 'api_json', side_effect=fake_api):
                args=['--state','CO','--counties','Weld','--output',str(Path(d)/'out'),
                      '--geometry-acres','never','--delay','0','--assessor-enrich','off']
                self.assertEqual(m.main(args), 1)
                report = json.loads((Path(d)/'out/CO/weld/output/weld_report.json').read_text())
                self.assertEqual(report['missing_object_ids'], 1)
                state['serve_missing'] = True
                self.assertEqual(m.main(args), 0)
                report = json.loads((Path(d)/'out/CO/weld/output/weld_report.json').read_text())
                self.assertEqual(report['missing_object_ids'], 0)
                self.assertEqual(report['downloaded_rows'], 2)

    def test_count_mismatch_refuses_incomplete_result(self):
        src = m.lookup_source(m.read_registry(None), 'CO', 'Weld')
        def fake_api(s, url, params, **kwargs):
            return {'objectIds':[1,2]} if 'returnIdsOnly' in params else {'count':3}
        with patch.object(m, 'api_json', side_effect=fake_api):
            with self.assertRaises(m.GISFailure):
                m.layer_ids(None, src)

    def test_compressed_name_repair(self):
        self.assertIsNotNone(m.repair_owner_name_scalar)
        corrected, method, conf, changed = m.repair_owner_name_scalar('PADDOCKROBERT')
        self.assertTrue(changed)
        self.assertEqual(corrected, 'PADDOCK ROBERT')


if __name__ == '__main__':
    unittest.main(verbosity=2)
