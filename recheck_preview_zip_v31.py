#!/usr/bin/env python3
"""Offline re-audit any v3.0 preview ZIP using corrected v3.1 field mappings.

This only reprocesses supplied raw values and saved geometry-acre estimates;
it does NOT simulate a successful county assessor enrichment or create owner data.
"""
import argparse
import csv
import io
import json
import re
import zipfile
from pathlib import Path

import multistate_parcel_downloader_v3 as m

COLUMNS=['full_name','legal_address','mailing_address','acres','city','zip','market_value','mailing_city','mailing_state','mailing_zip']

def records(z,member):
    with z.open(member) as b,io.TextIOWrapper(b,encoding='utf-8-sig',newline='') as t:
        yield from csv.DictReader(t)

def run(src_zip,out_dir):
    out_dir.mkdir(parents=True,exist_ok=True)
    results=[]
    with zipfile.ZipFile(src_zip) as z:
        all_names=z.namelist()
        raw_paths=[n for n in all_names if re.fullmatch(r'(WY|CO|OK|NM)/[^/]+/[^/]+/output/[^/]+_raw_all_fields\.csv',n)]
        if not raw_paths:raise RuntimeError('No v3.0 county preview raw_all_fields.csv found')
        for name in raw_paths:
            state,county_slug,preview,*_=name.split('/')
            co=county_slug.replace('_',' ').title()
            try:src=m.lookup_source(m.read_registry(None),state,co)
            except m.GISFailure:src=m.Source(state=state,county=co)
            old_path=name.replace('_raw_all_fields.csv','_all_normalized.csv')
            if old_path not in all_names:raise RuntimeError(f'No saved normalized/geometry file for {name}')
            originals=list(records(z,old_path))
            raw=list(records(z,name))
            if len(originals)!=len(raw):raise RuntimeError(f'Row-count mismatch for {name}')
            oid_field='FID' if state=='WY' else 'OBJECTID'
            enhanced=[m.normalize(attrs,src,oid_field,geometry_acres=m.positive(previous.get('geometry_acres')))
                      for attrs,previous in zip(raw,originals)]
            folder=out_dir/state/county_slug
            folder.mkdir(parents=True,exist_ok=True)
            m.csv_output(folder/(county_slug+'_normalized_v31_no_assessor.csv'),m.FIELDS,enhanced)
            outcsv=folder/(county_slug+'_missing_data_queue.csv')
            queue=m.missing_data_queue(folder/(county_slug+'_normalized_v31_no_assessor.csv'),outcsv)
            def old_population(row,f):
                # v3.0 coverage logic considered any nonempty value present,
                # including zero market valuations and UNINCORPORATED as a city.
                return bool(str(row.get(f) or '').strip())
            def yes(row,f):
                if f in ('acres','market_value'):return m.positive(row.get(f)) is not None
                if f=='city':return m.city_is_valid(row.get('city'))
                if f=='zip':return bool(m.clean_zip(row.get('zip')))
                return bool(m.as_text(row.get(f)))
            item={'state':state,'county':co,'sample_rows':len(enhanced),
                  'note':'mapping-only, no live assessor enrichment',
                  'missing_data_queued':queue['queued_records']}
            for f in COLUMNS:
                item[f+'_before']=sum(old_population(r,f) for r in originals)
                item[f+'_after']=sum(yes(r,f) for r in enhanced)
            results.append(item)
    results.sort(key=lambda x:(x['state'],x['county']))
    m.csv_output(out_dir/'preview_v30_vs_v31_quality.csv',list(results[0]),results)
    m.write_json(out_dir/'preview_v30_vs_v31_quality.json',{'version':m.VERSION,
                       'note':'Original ZIP only. Official assessor supplemental sources were not downloaded.',
                       'results':results})
    for d in results:
        gains=[f'{f}: {d[f+"_before"]}->{d[f+"_after"]}' for f in COLUMNS if d[f+'_after']!=d[f+'_before']]
        print(f"{d['state']}:{d['county']} {d['sample_rows']} rows | "+(', '.join(gains) or 'No mapped-field changes'))
    return results

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('preview_zip',type=Path)
    p.add_argument('--output',type=Path,default=Path('preview_v31_reaudit'))
    args=p.parse_args()
    run(args.preview_zip,args.output)
