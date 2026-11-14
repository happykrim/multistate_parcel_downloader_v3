"""Offline end-to-end regression cases for assessor-source joins, mapping, and QA."""
import csv
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import multistate_parcel_downloader_v3 as m
import county_enrichment_v31 as e


def write_csv(path,rows):
    keys=list(rows[0])
    with path.open('w',encoding='utf-8-sig',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=keys);writer.writeheader();writer.writerows(rows)


def zip_csv(path,members):
    with zipfile.ZipFile(path,'w',compression=zipfile.ZIP_DEFLATED) as z:
        for name,rows in members.items():
            stream=io.StringIO();wr=csv.DictWriter(stream,fieldnames=list(rows[0]));wr.writeheader();wr.writerows(rows)
            z.writestr(name,stream.getvalue())


class AddressMappingTests(unittest.TestCase):
    def test_bernalillo_street_and_city_from_two_lines(self):
        src=m.lookup_source(m.read_registry(None),'NM','Bernalillo')
        row=m.normalize({'OBJECTID':1,'UPC':'001','OWNER':'SMITH JOHN',
            'SITUSADD':'12225 EAGLE ROCK AVE NE','SITUSADD2':'ALBUQUERQUE NM 87122',
            'OWNADD':'10513 DELICADO PL NE','OWNADD2':'ALBUQUERQUE NM 87111-1710'},src,'OBJECTID')
        self.assertEqual(row['property_address'],'12225 EAGLE ROCK AVE NE')
        self.assertEqual(row['property_city'],'ALBUQUERQUE')
        self.assertEqual(row['property_zip'],'87122')
        self.assertEqual(row['mailing_address'],'10513 DELICADO PL NE, ALBUQUERQUE, NM 87111-1710')
        self.assertIn('SITUSADD2',row['property_city_source'])
        self.assertEqual(row['mailing_zip'],'87111-1710')

    def test_bernalillo_site_state_inferred_not_mailing(self):
        src=m.lookup_source(m.read_registry(None),'NM','Bernalillo')
        row=m.normalize({'OBJECTID':2,'UPC':'002','SITUSADD':'9220 LA TIERRA CT NE',
                        'SITUSADD2':'ALBUQUERQUE 87122',
                        'OWNADD':'1 OLD COURT','OWNADD2':'DENVER 80202'},src,'OBJECTID')
        self.assertEqual(row['property_city'],'ALBUQUERQUE')
        self.assertEqual(row['property_zip'],'87122')
        self.assertEqual(row['mailing_city'],'')
        self.assertEqual(row['mailing_state'],'')
        self.assertEqual(row['mailing_zip'],'')

    def test_canadian_unincorporated_not_a_city(self):
        src=m.lookup_source(m.read_registry(None),'OK','Canadian')
        row=m.normalize({'OBJECTID':1,'parcel_id':'222','situs_city':'UNINCORPORATED',
                         'situs_zip':'73036','mail_city':'EL RENO'},src,'OBJECTID')
        self.assertEqual(row['city'],'')
        self.assertEqual(row['property_city_raw'],'UNINCORPORATED')
        self.assertIn('unincorporated_not_city',row['data_quality_flags'])
        self.assertEqual(row['zip'],'73036')

    def test_missing_market_zero_is_gap_not_fake_completion(self):
        src=m.lookup_source(m.read_registry(None),'WY','Natrona')
        row=m.normalize({'FID':1,'parcelnb':'123','actualvalu':0,'landgrossa':50},src,'FID')
        self.assertEqual(row['market_value'],0)
        self.assertIn('missing_or_zero_market_value',row['data_quality_flags'])

class BulkJoinTests(unittest.TestCase):
    def test_el_paso_assessor_zip_fill_original_missing_and_upgrade_geometry(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp=Path(tmp)
            parcel=tmp/'raw.csv';output=tmp/'enriched.csv'
            src=m.lookup_source(m.read_registry(None),'CO','El Paso')
            values=[m.normalize({'OBJECTID':i+1,'parcel_id':str(pid),'owner':'GOOD SOURCE' if i==1 else ''},
                                src,'OBJECTID',geometry_acres=44.4) for i,pid in enumerate(['5331106089','0000001222','0000001333'])]
            write_csv(parcel,values)
            z=tmp/'EPC-Parcel-Data-20261110.zip'
            zip_csv(z,{'EPC-Parcel-Data.csv':[
                {'PARCEL':'5331106089','OWNER1':'ASSR OWNER','MAILADR':'10 MAIL AVE',
                 'MAILCITY':'DENVER','MAILSTATE':'CO','MAILZIPCODE':'80202','LOCATION':'77 RANCH RD',
                 'LOCATIONZIP':'80901','Acreage':'50','MARKETVALUE':'700000','ASSESSEDVALUE':'70000'},
                {'PARCEL':'0000001222','OWNER1':'OTHER OWNER','MAILADR':'29 ELSEWHERE',
                 'MAILCITY':'TAMPA','MAILSTATE':'FL','MAILZIPCODE':'33602','LOCATION':'8 MAIN ST',
                 'LOCATIONZIP':'80904','Acreage':'61','MARKETVALUE':'400000','ASSESSEDVALUE':'20000'},
                {'PARCEL':'0000001333','OWNER1':'AMBIGUOUS ONE','MAILADR':'MAIL1',
                 'MAILCITY':'DENVER','MAILSTATE':'CO','MAILZIPCODE':'80202','LOCATION':'SITE1',
                 'LOCATIONZIP':'80901','Acreage':'70','MARKETVALUE':'777000','ASSESSEDVALUE':'70000'},
                {'PARCEL':'0000001333','OWNER1':'AMBIGUOUS TWO','MAILADR':'MAIL2',
                 'MAILCITY':'DENVER','MAILSTATE':'CO','MAILZIPCODE':'80202','LOCATION':'SITE2',
                 'LOCATIONZIP':'80901','Acreage':'77','MARKETVALUE':'777000','ASSESSEDVALUE':'70000'},
            ]})
            db,n=e.build_index(tmp/'test.db',z,'el_paso',lambda msg:None)
            stats=e.apply_bulk(parcel,output,db,'el_paso',src,m.normalize,m.Source,m.as_text,m.positive,
                               m.number,m.FIELDS,lambda msg:None)
            db.close()
            with output.open(encoding='utf-8-sig',newline='') as f:results=list(csv.DictReader(f))
            r0,r1,r2=results
            self.assertEqual(n,4)
            self.assertEqual(stats['join_counts'],{'matched':2,'ambiguous':1})
            self.assertEqual(r0['full_name'],'ASSR OWNER')
            self.assertEqual(r0['full_name_raw'],'ASSR OWNER')
            self.assertEqual(r0['mailing_address'],'10 MAIL AVE, DENVER, CO 80202')
            self.assertEqual(r0['property_zip'],'80901')
            self.assertEqual(float(r0['acres']),50)
            self.assertTrue(r0['acres_source'].startswith('county_assessor'))
            self.assertEqual(float(r0['geometry_acres']),44.4)
            self.assertEqual(float(r0['market_value']),700000)
            self.assertEqual(r1['full_name'],'GOOD SOURCE') # preserve original existing owner
            self.assertEqual(float(r1['acres']),61)
            self.assertEqual(r2['full_name'],'')
            self.assertIn('ambiguous_assessor_join',r2['enrichment_review_flags'])
            self.assertNotIn('5331106089','|'.join([r['parcel_id'] for r in results[1:]]))

    def test_weld_zip_multi_table_owner_and_inventory_zip(self):
        with tempfile.TemporaryDirectory() as d:
            d=Path(d);src=m.lookup_source(m.read_registry(None),'CO','Weld')
            orig=m.normalize({'OBJECTID':1,'parcel_id':'095910101005','account':'R1989486',
                              'landAcres':0.26,'owner':'SOURCE OWNER','situsAdd':'11 MAIN RD'},src,'OBJECTID')
            write_csv(d/'start.csv',[orig])
            archive=d/'Assessor-All.zip'
            zip_csv(archive,{
              'OWNERSHIPGIS.csv':[{'PARCELNB':'095910101005','ACCOUNTNO':'R1989486','NAME':'COUNTY OWNER',
                                   'ADDRESS1':'11 MAIN RD','ADDRESS2':'','CITY':'GREELEY','STATE':'CO',
                                   'ZIPCODE':'80634','STREETNO':'11','STREETALP':'','STREETDIR':'',
                                   'STREETNAME':'MAIN','STREETSUF':'RD','LOCCITY':'GREELEY','ACRES':'0.25'}],
              'ACCTCURRENTINVNTRY.csv':[{'PARCELNB':'095910101005','ACCOUNTNO':'R1989486',
                                        'LOCZIP':'80634','LOCCITY':'GREELEY','GROSSACRES':'0.25',
                                        'STREETNO':'11','STREETALP':'','STREETDIR':'',
                                        'STREETNAME':'MAIN','STREETSUF':'RD'}],
            })
            cur=d/'start.csv';data=[]
            for kind in ['weld_ownership','weld_current_inventory']:
                db,n=e.build_index(d/(kind+'.db'),archive,kind,lambda x:None)
                nxt=d/(kind+'.csv')
                data.append(e.apply_bulk(cur,nxt,db,kind,src,m.normalize,m.Source,m.as_text,m.positive,
                                         m.number,m.FIELDS,lambda x:None))
                db.close();cur=nxt
            with cur.open(encoding='utf-8-sig',newline='') as fh:row=next(csv.DictReader(fh))
            self.assertEqual(row['full_name'],'SOURCE OWNER')
            self.assertEqual(row['mailing_address'],'11 MAIN RD, GREELEY, CO 80634')
            self.assertEqual(row['property_zip'],'80634')
            self.assertAlmostEqual(float(row['acres']),0.26) # valid source acres retained
            self.assertEqual(row['property_city'],'GREELEY')

    def test_missing_queue_and_meaningful_coverage(self):
        with tempfile.TemporaryDirectory() as d:
            d=Path(d);f=d/'records.csv';src=m.lookup_source(m.read_registry(None),'OK','Canadian')
            r=m.normalize({'OBJECTID':3,'parcel_id':'111','situs_city':'UNINCORPORATED','landAcres':40},src,'OBJECTID')
            write_csv(f,[r]);cov=m.coverage_report(f)
            self.assertEqual(cov['fields']['property_city']['valid'],0)
            self.assertEqual(cov['fields']['acres']['valid'],1)
            out=m.missing_data_queue(f,d/'queue.csv')
            self.assertEqual(out['queued_records'],1)
            with (d/'queue.csv').open(encoding='utf-8-sig') as ff: q=next(csv.DictReader(ff))
            self.assertEqual(q['priority'],'HIGH')
            self.assertIn('missing_owner',q['missing_fields'])

    def test_sample_selection_seed_is_reproducible(self):
        ids=list(range(1,1000))
        import random
        a=sorted(random.Random(42).sample(ids,100))
        b=sorted(random.Random(42).sample(ids,100))
        self.assertEqual(a,b)
        self.assertNotEqual(a,ids[:100])

class CliEnrichmentTests(unittest.TestCase):
    def test_el_paso_complete_cli_with_local_assessor_zip(self):
        with tempfile.TemporaryDirectory() as td:
            d=Path(td); raw=d/'base.csv'
            write_csv(raw,[{'OBJECTID':'1','parcel_id':'5331106089','owner':'',
                            'situsAdd':'','landAcres':'','account':''}])
            assessor=d/'EPC-Parcel-Data-20261110.zip'
            zip_csv(assessor,{'EPC-Parcel-Data.csv':[{'PARCEL':'5331106089','OWNER1':'NAMED OWNER',
                       'LOCATION':'123 CO RANCH ROAD','LOCATIONZIP':'80817',
                       'MAILADR':'55 EAST MAIN','MAILCITY':'DENVER','MAILSTATE':'CO',
                       'MAILZIPCODE':'80202','Acreage':'76.5','MARKETVALUE':'512400',
                       'ASSESSEDVALUE':'54120'}]})
            result=m.main(['--state','CO','--counties','El Paso','--local-file',str(raw),
                  '--assessor-file',f'CO:El Paso={assessor}','--assessor-enrich','require',
                  '--output',str(d/'output')])
            self.assertEqual(result,0)
            final=d/'output/CO/el_paso/output/el_paso_production.csv'
            with final.open(encoding='utf-8-sig',newline='') as fh:r=next(csv.DictReader(fh))
            self.assertEqual(r['full_name'],'NAMED OWNER')
            self.assertEqual(r['legal_address'],'123 CO RANCH ROAD')
            self.assertEqual(float(r['acres']),76.5)
            self.assertEqual(float(r['market_value']),512400)
            report=json.loads((final.parent/'el_paso_report.json').read_text())
            self.assertEqual(report['assessor_enrichment']['phases'][0]['join_counts']['matched'],1)

    def test_custom_assessor_bulk_config_for_future_county(self):
        with tempfile.TemporaryDirectory() as td:
            d=Path(td);base=d/'base.csv';sup=d/'extra.csv'
            write_csv(base,[{'OBJECTID':'1','UPC':'12345','OWNER':'','ACREAGE':''}])
            write_csv(sup,[{'PARCEL':'12345','OWNER_FULL':'TEST OWNER','MAIL_STREET':'1 LANE',
                            'MAIL_CITY':'TULSA','MAIL_STATE':'OK','MAIL_ZIP':'74101',
                            'LAND_ACRES':'88'}])
            rules={'OK':{'Tulsa':{'type':'bulk','local_file':str(sup),
                    'stages':[{'kind':'custom_bulk','parcel_field':'PARCEL',
                               'field_map':{'parcel_id':['PARCEL'],'full_name':['OWNER_FULL'],
                                  'mailing_line1':['MAIL_STREET'],'mailing_city':['MAIL_CITY'],
                                  'mailing_state':['MAIL_STATE'],'mailing_zip':['MAIL_ZIP'],
                                  'acres':['LAND_ACRES']}}]}}}
            rules_file=d/'rules.json';rules_file.write_text(json.dumps(rules))
            rc=m.main(['--state','OK','--counties','Tulsa','--local-file',str(base),
                      '--assessor-rules-json',str(rules_file),'--assessor-enrich','require',
                      '--output',str(d/'o')])
            self.assertEqual(rc,0)
            with (d/'o/OK/tulsa/output/tulsa_production.csv').open(encoding='utf-8-sig') as fh:
                row=next(csv.DictReader(fh))
            self.assertEqual(row['full_name'],'TEST OWNER')
            self.assertEqual(row['acres'],'88.0')
            self.assertIn('TULSA',row['mailing_address'])

class BernalilloSecondaryTests(unittest.TestCase):
    def test_service_lookup_enriches_only_missing_fields(self):
        with tempfile.TemporaryDirectory() as d:
            d=Path(d);src=m.lookup_source(m.read_registry(None),'NM','Bernalillo')
            base=m.normalize({'OBJECTID':3,'UPC':'001','OWNER':'PRIMARY OWNER',
                     'SITUSADD':'55 HIGH ST','SITUSADD2':'ALBUQUERQUE NM 87122'},src,'OBJECTID')
            write_csv(d/'source.csv',[base])
            def fake_api(sess,url,params,**kw):
                self.assertIn("UPC IN ('001')",params['where'])
                return {'features':[{'attributes':{'OBJECTID':77,'UPC':'001','OWNER':'DIFFERENT OWNER',
                    'SITUSADD':'55 HIGH ST','SITUSCITY':'ALBUQUERQUE','SITUSZIP':'87122',
                    'LANDVALUE':6000,'IMPTVALUE':10000,'TOTVALUE':16000,'NETTAXABLE':11000,
                    'ACREAGE':1.5,'TAXYR':'2026'}}]}
            result=e.apply_bernalillo(d/'source.csv',d/'new.csv',src,
                    lambda :None,fake_api,m.normalize,m.Source,m.as_text,m.positive,m.number,m.FIELDS,0,lambda x:None)
            with (d/'new.csv').open(encoding='utf-8-sig',newline='') as fh:r=next(csv.DictReader(fh))
            self.assertEqual(result['join_counts']['matched'],1)
            self.assertEqual(r['full_name'],'PRIMARY OWNER')
            self.assertEqual(r['market_value'],'')
            self.assertEqual(float(r['total_value']),16000)
            self.assertEqual(r['taxable_value'],'11000.0')

if __name__=='__main__':unittest.main(verbosity=2)
