"""Conservative, source-aware assessor recovery for multi-state parcel downloader v3.1.

County-specific bulk files and secondary parcel GIS layers are *supplements*:
never overwrite a valid primary value except a GIS geometry-acre estimate with
an assessor acreage value. All joins use scoped parcel/account identifiers,
and ambiguous matches are reported rather than guessed.
"""
from __future__ import annotations

import csv
import io
import json
import re
import sqlite3
import tempfile
import time
import zipfile
from collections import Counter
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

EL_PASO_PAGE = 'https://assessor.elpasoco.com/assessordata/'
WELD_PAGE = 'https://www.weld.gov/Government/Departments/Assessor/Data-Download'
BERNALILLO_LAYER = 'https://coageo.cabq.gov/cabqgeo/rest/services/agis/AddressReport/MapServer/4'

COUNTY_SOURCES = {
    ('CO','elpaso'): {'type':'bulk','landing_page':EL_PASO_PAGE,'file_hint':'EPC-Parcel-Data','link_rx':r'EPC-Parcel-Data-\d{8}\.zip','provider':'El Paso County Assessor (weekly parcel file)'},
    ('CO','weld'): {'type':'bulk','landing_page':WELD_PAGE,'file_hint':'OWNERSHIPGIS','link_rx':r'All-Data-CSV','provider':'Weld County Assessor (weekly all-data CSV)'},
    ('NM','bernalillo'): {'type':'arcgis','url':BERNALILLO_LAYER,'key':'UPC','provider':'Bernalillo County Assessor via City of Albuquerque (spring/fall snapshot)'},
}

# Deliberately keyed to the documented assessor file layouts. Never promote
# total/assessed/taxable values into the market_value field.
MAPPINGS = {
 'el_paso': {
  'parcel_id':['PARCEL'], 'full_name':['OWNER1'], 'full_name_secondary':['OWNER2'],
  'property_address':['LOCATION'], 'property_zip':['LOCATIONZIP'],
  'mailing_line1':['MAILADR','MAILINTLPROBADR'], 'mailing_city':['MAILCITY'],
  'mailing_state':['MAILSTATE'], 'mailing_zip':['MAILZIPCODE'],
  'acres':['Acreage'], 'market_value':['MARKETVALUE'], 'assessed_value':['ASSESSEDVALUE'],
  'legal_description':['PARTIALLEGAL'], 'land_use':['LANDCODEDESCR'],
  'property_class':['LANDCODE'], 'sale_date':['sdate'], 'sale_price':['sprice'],
  'source_data_year':['LastUpdate'],
 },
 'weld_ownership': {
  'parcel_id':['PARCELNB'], 'account_id':['ACCOUNTNO'], 'full_name':['NAME','BUSINESSNAME'],
  'mailing_line1':['ADDRESS1'], 'mailing_line2':['ADDRESS2'],
  'mailing_city':['CITY'], 'mailing_state':['STATE'], 'mailing_zip':['ZIPCODE'],
  'property_address':['WELD_SITE_ADDR'], 'property_city':['LOCCITY'],
  'legal_description':['LEGAL'], 'acres':['ACRES'], 'sale_price':['SALEP'],
  'sale_date':['SALEDT'], 'land_value':['LANDACT'], 'improvement_value':['IMPACT'],
 },
 'weld_current_inventory': {
  'account_id':['ACCOUNTNO'], 'property_city':['LOCCITY'], 'property_zip':['LOCZIP'],
  'acres':['GROSSACRES'], 'property_address':['WELD_SITE_ADDR'],
 },
 'bernalillo': {
  'parcel_id':['UPC'], 'full_name':['OWNER'], 'property_address':['SITUSADD'],
  'property_city':['SITUSCITY'], 'property_zip':['SITUSZIP'],
  'mailing_line1':['OWNADD'], 'mailing_line2':['OWNADD2'],
  'mailing_city':['OWNCITY'], 'mailing_state':['OWNSTATE'], 'mailing_zip':['OWNZIPCODE'],
  'legal_description':['LEGALDESC'], 'acres':['ACREAGE'], 'land_value':['LANDVALUE'],
  'improvement_value':['IMPTVALUE'], 'total_value':['TOTVALUE'],
  'taxable_value':['NETTAXABLE'], 'property_class':['PROPCLASS'], 'tax_year':['TAXYR'],
 },
}

FILL_FIELDS = [
 'full_name','full_name_secondary','property_address','property_city','property_zip',
 'mailing_address','mailing_city','mailing_state','mailing_zip','acres',
 'market_value','actual_value','appraised_value','assessed_value','taxable_value',
 'land_value','improvement_value','total_value','legal_description','property_class',
 'land_use','sale_date','sale_price','tax_year',
]
PRIORITY_FIELDS = ['full_name','mailing_address','property_address','property_city','property_zip',
                   'acres','market_value','assessed_value','land_value','total_value']

class EnrichmentError(Exception): pass


def key(value):
    """Exact text key: preserve leading zeros. Normalize only CSV integer float artifact."""
    s = str(value if value is not None else '').strip().strip('"\'')
    if not s or s.lower() in ('nan','none','null'):
        return ''
    s = re.sub(r'^(\d+)\.0+$',r'\1',s)
    return s.upper()


class _Links(HTMLParser):
    def __init__(self):
        super().__init__(); self.urls=[]
    def handle_starttag(self, tag, attrs):
        if tag.lower()=='a':
            u=dict(attrs).get('href')
            if u: self.urls.append(u)


def locate_official_zip(session, spec):
    page=spec['landing_page']
    response=session.get(page,timeout=(10,30)); response.raise_for_status()
    links=_Links(); links.feed(response.text)
    candidates=[]
    for relative in links.urls:
        absolute=urljoin(response.url, relative)
        if re.search(spec['link_rx'],absolute,re.I):
            host=urlparse(absolute).hostname or ''
            # A county website might redirect to its dedicated asset host.
            if spec['file_hint']=='EPC-Parcel-Data' and host not in {'assessor.elpasoco.com','epc-assets.elpasoco.com'}:
                continue
            if spec['file_hint']=='OWNERSHIPGIS' and not (host.endswith('weld.gov') or host=='weld.gov'):
                continue
            candidates.append(absolute)
    if not candidates:
        raise EnrichmentError('No official assessor ZIP link found on '+page)
    return sorted(candidates)[-1]


def download_zip(session,url,folder,logger):
    folder.mkdir(parents=True,exist_ok=True)
    fname=Path(urlparse(url).path).name or 'county_assessor.zip'
    if not fname.casefold().endswith('.zip'):
        fname='county_assessor.zip'
    dest=folder / fname
    if dest.exists() and zipfile.is_zipfile(dest):
        logger('[ENRICH] Cached county assessor ZIP: '+str(dest));return dest
    tmp=dest.with_suffix('.zip.part')
    logger('[ENRICH] Downloading assessor archive: '+url)
    with session.get(url,stream=True,timeout=(20,300)) as r:
        r.raise_for_status()
        with tmp.open('wb') as fh:
            bytes_done=0; last=0
            for chunk in r.iter_content(1024*1024):
                if not chunk: continue
                fh.write(chunk);bytes_done+=len(chunk)
                if bytes_done-last>=40*1024*1024:
                    logger(f'[ENRICH] downloaded {bytes_done/1024/1024:.0f} MiB')
                    last=bytes_done
    if not zipfile.is_zipfile(tmp):
        raise EnrichmentError(f'Assessor archive is not a valid ZIP: {tmp}')
    tmp.replace(dest);return dest


def zip_member(archive,kind,member_hint=''):
    with zipfile.ZipFile(archive) as z:
        names=[n for n in z.namelist() if not n.endswith('/') and Path(n).suffix.lower() in ('.csv','.txt','.dbf')]
        if kind=='el_paso':
            # Prefer a parcel file rather than any attached sales report.
            c=[n for n in names if 'parcel' in Path(n).stem.lower()]
            if not c and len(names)==1: c=names
        elif kind=='weld_ownership':
            c=[n for n in names if 'ownershipgis' in Path(n).stem.lower()]
        elif kind=='weld_current_inventory':
            c=[n for n in names if 'acctcurrentinvntry' in Path(n).stem.lower()]
        elif kind=='custom_bulk':
            if member_hint:
                c=[n for n in names if member_hint.casefold() in Path(n).stem.casefold()]
            else:c=names if len(names)==1 else []
        else: c=[]
        if len(c)!=1:
            raise EnrichmentError(f'{kind}: expected one applicable CSV/TXT/DBF in ZIP; found {c[:8]} in {len(names)} files')
        return c[0]


def iter_source_records(path:Path,kind:str,member_hint=''):
    """Stream local assessor csv/DBF or selected ZIP member, preserving IDs as strings."""
    import contextlib
    if path.suffix.casefold()=='.zip':
        name=zip_member(path,kind,member_hint)
        with zipfile.ZipFile(path) as z:
            if Path(name).suffix.lower()=='.dbf':
                try:from dbfread import DBF
                except ImportError as e: raise EnrichmentError('DBF source requires python -m pip install dbfread') from e
                with tempfile.TemporaryDirectory() as td:
                    out=Path(td)/Path(name).name
                    with z.open(name) as fin, out.open('wb') as fout:
                        import shutil; shutil.copyfileobj(fin,fout)
                    for x in DBF(str(out),load=False,encoding='latin1',char_decode_errors='ignore'):
                        yield dict(x)
            else:
                with z.open(name) as f: yield from _iter_text_rows(f)
    elif path.suffix.casefold()=='.dbf':
        try:from dbfread import DBF
        except ImportError as e: raise EnrichmentError('DBF source requires python -m pip install dbfread') from e
        for x in DBF(str(path),load=False,encoding='latin1',char_decode_errors='ignore'):
            yield dict(x)
    elif path.suffix.casefold() in ('.csv','.txt'):
        with path.open('rb') as f: yield from _iter_text_rows(f)
    else:
        raise EnrichmentError(f'Unsupported supplemental format {path.suffix} (use CSV/TXT/DBF/ZIP)')


def _iter_text_rows(binary):
    # Older assessor exports may be Windows-1252, so replacement decode is
    # intentional; preserve identifiers as CSV strings and do not drop rows.
    import itertools
    with io.TextIOWrapper(binary,encoding='utf-8-sig',errors='replace',newline='') as stream:
        first=stream.readline()
        if not first:return
        sep=max([',','\t','|',';'],key=lambda c:first.count(c))
        if not first.count(sep):
            raise EnrichmentError('Could not detect assessor text field delimiter')
        reader=csv.DictReader(itertools.chain([first],stream),delimiter=sep)
        for row in reader:
            if row:yield row


def enrichable_value(value,field,as_text,positive):
    if field in ('acres',): return positive(value) is not None
    if field in ('market_value',):return positive(value) is not None
    if field in ('actual_value','appraised_value','assessed_value','taxable_value','land_value','improvement_value','total_value','sale_price'):
        try:return value is not None and str(value).strip()!='' and float(str(value).replace(',',''))>=0
        except (ValueError,TypeError):return False
    if field in ('property_zip','mailing_zip'):
        s=str(value or '').strip();return bool(re.fullmatch(r'\d{5}(?:-\d{4})?',s))
    return bool(as_text(value))


def _site_street(attrs):
    a=[str(attrs.get(n) or '').strip() for n in ['STREETNO','STREETALP','STREETDIR','STREETNAME','STREETSUF']]
    return ' '.join(x for x in a if x)


def build_index(db_path, path, kind, logger, *, parcel_field='', account_field='', member_hint=''):
    """Recreate a disk-backed source index, with no per-county memory explosion."""
    db_path.parent.mkdir(parents=True,exist_ok=True)
    if db_path.exists(): db_path.unlink()
    conn=sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode=OFF')
    conn.execute('PRAGMA synchronous=OFF')
    conn.execute('CREATE TABLE records (parcel TEXT, account TEXT, payload TEXT NOT NULL)')
    count=0
    try:
        for rec in iter_source_records(path,kind,member_hint=member_hint):
            lookup={str(k).upper(): v for k,v in rec.items() if k is not None}
            pk=(parcel_field or ('PARCEL' if kind=='el_paso' else 'PARCELNB')).upper()
            ak=(account_field or 'ACCOUNTNO').upper()
            parcel=key(lookup.get(pk))
            account=key(lookup.get(ak))
            if kind=='weld_current_inventory' and not account:continue
            if kind!='weld_current_inventory' and not parcel:continue
            if kind.startswith('weld'):
                rec['WELD_SITE_ADDR']=_site_street(lookup)
            conn.execute('INSERT INTO records VALUES (?,?,?)',(parcel,account,json.dumps(rec,default=str)))
            count+=1
            if count%25000==0:
                conn.commit();logger(f'[ENRICH] indexed {count:,} assessor rows ({kind})')
        conn.execute('CREATE INDEX idx_parcel ON records(parcel)')
        conn.execute('CREATE INDEX idx_account ON records(account)')
        conn.commit()
    except BaseException:
        conn.close();raise
    logger(f'[ENRICH] indexed {count:,} assessor rows ({kind})')
    return conn,count


def _fetch_rows(conn,record,kind):
    parcel=key(record.get('parcel_id'))
    account=key(record.get('account_id'))
    if kind=='weld_current_inventory':
        if not account:return []
        return [json.loads(x[0]) for x in conn.execute('SELECT payload FROM records WHERE account=? LIMIT 101',(account,))]
    if not parcel:return []
    rows=[(a,json.loads(v)) for a,v in conn.execute('SELECT account,payload FROM records WHERE parcel=? LIMIT 101',(parcel,))]
    if len(rows)>1 and account:
        exact=[v for a,v in rows if a==account]
        if exact:return exact
    return [v for a,v in rows]


def _select_candidate(records,kind,normalizer,src,oid_name):
    if not records:return None,'unmatched'
    normalized=[normalizer(rec,src,oid_name) for rec in records]
    signatures={tuple(str(row.get(f) or '').strip() for f in FILL_FIELDS) for row in normalized}
    if len(signatures)==1:return normalized[0],'matched'
    # Multiple assessor records may differ on owners or land uses while agreeing
    # on ZIP/acreage. Recover only fields with ONE uncontested nonblank value.
    # Keep ambiguous status, never guess a primary owner from multiple names.
    consensus={}
    for fld in FILL_FIELDS:
        candidates=[(str(row.get(fld)).strip().casefold(),row) for row in normalized
                    if row.get(fld) is not None and str(row.get(fld)).strip()]
        if len({k for k,_ in candidates})==1 and candidates:
            chosen=candidates[0][1]
            consensus[fld]=chosen[fld]
            if fld+'_source' in chosen:consensus[fld+'_source']=chosen[fld+'_source']
    return (consensus if consensus else None),'ambiguous'


def _add_flag(row,flag):
    flags=set(filter(None,str(row.get('enrichment_review_flags') or '').split('|')))
    flags.add(flag);row['enrichment_review_flags']='|'.join(sorted(flags))


def merge_rows(target,candidate,source_tag,as_text,positive,number):
    """Never replace a valid primary field except sourced geometry acres."""
    if not candidate:return [],[]
    filled=[]; conflicts=[]
    for field in FILL_FIELDS:
        incoming=candidate.get(field)
        if not enrichable_value(incoming,field,as_text,positive):continue
        current=target.get(field)
        has_current=enrichable_value(current,field,as_text,positive)
        upgrade_area=field=='acres' and str(target.get('acres_source') or '').startswith('GEOMETRY')
        if not has_current or upgrade_area:
            if has_current and upgrade_area:
                old=positive(current);new=positive(incoming)
                if old and new and abs(old-new)/max(old,new)>0.10:
                    _add_flag(target,'geometry_vs_assessor_acres_diff_gt_10pct')
            target[field]=incoming
            if field=='full_name':
                target['full_name_raw']=incoming
                target['full_name_quality']='source_spaced' if re.search(r'\s',str(incoming)) else 'source_compact_unreviewed'
            # Trace source of each accepted attribute.
            col=field+'_source'
            if col in target:
                evidence=str(candidate.get(col) or '')
                if field=='acres':target[col]=source_tag+':'+str(candidate.get('acres_source') or 'acres')
                else:target[col]=source_tag+':'+(evidence or field)
            filled.append(field)
        elif field in PRIORITY_FIELDS:
            old=str(current or '').strip().upper();new=str(incoming or '').strip().upper()
            if old!=new and field not in {'property_address','mailing_address'}:
                conflicts.append(field)
    # Field aliases in user-facing normalization.
    for dest,origin in [('legal_address','property_address'),('city','property_city'),('zip','property_zip')]:
        if origin in filled or (not as_text(target.get(dest)) and as_text(target.get(origin))):
            target[dest]=target.get(origin,'')
            if dest+'_source' in target:target[dest+'_source']=target.get(origin+'_source') or source_tag
    if conflicts:
        for fld in conflicts[:5]:_add_flag(target,'conflict_'+fld)
    if filled:
        existing=list(filter(None,str(target.get('enrichment_fields_filled') or '').split('|')))
        target['enrichment_fields_filled']='|'.join(dict.fromkeys(existing+filled))
    return filled,conflicts


def _source_normalize(attrs,kind,src,normalize,Source):
    assr=Source(state=src.state,county=src.county,
                publisher='Supplemental official assessor dataset',field_map=MAPPINGS[kind])
    return normalize(attrs,assr,'OBJECTID')


def apply_bulk(input_csv,output_csv,db,kind,src,normalize,Source,as_text,positive,number,fieldnames,logger,*,field_map=None):
    matches=Counter();recovered=Counter(); conflicts=Counter(); sample_ambiguous=[]
    tmp=output_csv.with_name(output_csv.name+'.tmp')
    with input_csv.open(encoding='utf-8-sig',newline='') as fin, tmp.open('w',encoding='utf-8-sig',newline='') as fout:
        reader=csv.DictReader(fin);writer=csv.DictWriter(fout,fieldnames=fieldnames,extrasaction='ignore');writer.writeheader()
        for n,row in enumerate(reader,1):
            candidates=_fetch_rows(db,row,kind)
            if len(candidates)>100:chosen,status=None,'ambiguous_many'
            else:
                chosen,status=_select_candidate(candidates,kind,normalize,
                    Source(state=src.state,county=src.county,field_map=field_map or MAPPINGS[kind]),'OBJECTID')
            matches[status]+=1
            if status=='matched':
                filled,flags=merge_rows(row,chosen,'county_assessor_'+kind,as_text,positive,number)
                recovered.update(filled);conflicts.update(flags)
                row['enrichment_matched']='yes'
                row['enrichment_method']=row.get('enrichment_method') or 'county_assessor_bulk'
            elif status.startswith('ambiguous'):
                if chosen:
                    shared,shared_conflicts=merge_rows(row,chosen,'county_assessor_'+kind+'_consensus',as_text,positive,number)
                    recovered.update(shared);conflicts.update(shared_conflicts)
                    if shared:row['enrichment_matched']='ambiguous_shared_fields_only'
                if len(sample_ambiguous)<50:sample_ambiguous.append({'parcel_id':row.get('parcel_id'), 'account_id':row.get('account_id'), 'candidate_count':len(candidates),'kind':kind})
                _add_flag(row,'ambiguous_assessor_join')
            writer.writerow({k:row.get(k,'') for k in fieldnames})
            if n%50000==0:logger(f'[ENRICH] matched {n:,} downloaded parcels against {kind}')
    tmp.replace(output_csv)
    return {'kind':kind,'join_counts':dict(matches),'fields_filled':dict(recovered),'conflicts_preserved':dict(conflicts),
            'sample_ambiguous':sample_ambiguous}


def fetch_secondary(s,url,ids,api_json,logger,delay,parcel_field='UPC'):
    """ID-filtered ArcGIS query, recursive split if provider imposes transfer limits."""
    if not ids:return []
    quotes=','.join("'"+str(x).replace("'","''")+"'" for x in ids)
    params={'where':f'{parcel_field} IN ({quotes})','outFields':'*','returnGeometry':'false'}
    try:
        result=api_json(s,url+'/query',params,delay=delay)
    except Exception:
        if len(ids)==1:raise
        split=len(ids)//2
        return fetch_secondary(s,url,ids[:split],api_json,logger,delay,parcel_field)+fetch_secondary(s,url,ids[split:],api_json,logger,delay,parcel_field)
    data=result.get('features')
    if not isinstance(data,list):raise EnrichmentError('Secondary ArcGIS layer returned no features array')
    if result.get('exceededTransferLimit'):
        if len(ids)==1:raise EnrichmentError('Secondary ArcGIS layer truncates results for a single UPC')
        mid=len(ids)//2
        return fetch_secondary(s,url,ids[:mid],api_json,logger,delay,parcel_field)+fetch_secondary(s,url,ids[mid:],api_json,logger,delay,parcel_field)
    return [f['attributes'] for f in data if isinstance(f,dict) and isinstance(f.get('attributes'),dict)]


def apply_bernalillo(input_csv,output_csv,src,session,api_json,normalize,Source,as_text,positive,number,fieldnames,delay,logger,
                     *,url=BERNALILLO_LAYER,parcel_field='UPC',field_map=None):
    field_map=field_map or MAPPINGS['bernalillo']
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',parcel_field):
        raise EnrichmentError('Unsafe secondary ArcGIS parcel key field')
    # Fetch only selected parcel UPCs, not the entire 250k+ county service.
    s=session();cache={};fetch_err=[];stats=Counter();recovered=Counter();conflicts=Counter()
    with input_csv.open(encoding='utf-8-sig',newline='') as fi:
        ids=sorted({key(r.get('parcel_id')) for r in csv.DictReader(fi) if key(r.get('parcel_id'))})
    started=time.monotonic()
    for off in range(0,len(ids),60):
        batch=ids[off:off+60]
        try:
            results=fetch_secondary(s,url,batch,api_json,logger,delay,parcel_field)
            for record in results:
                k=key(record.get(parcel_field))
                if k:cache.setdefault(k,[]).append(record)
        except Exception as exc:
            fetch_err.append(f'UPCs {off+1}-{off+len(batch)}: {str(exc)[:180]}')
        completed=min(off+60,len(ids))
        if completed%3000<60 or completed==len(ids):
            elapsed=max(time.monotonic()-started,0.001)
            rate=completed/elapsed
            remaining=(len(ids)-completed)/rate if rate>0 else 0
            logger(f'[ENRICH] secondary ArcGIS {completed:,}/{len(ids):,} IDs | '
                   f'{rate:.1f} IDs/s | ETA {remaining/60:.1f} min')
    temp=output_csv.with_name(output_csv.name+'.tmp')
    suppsrc=Source(state=src.state,county=src.county,field_map=field_map)
    with input_csv.open(encoding='utf-8-sig',newline='') as fi, temp.open('w',encoding='utf-8-sig',newline='') as fo:
        rd=csv.DictReader(fi);wr=csv.DictWriter(fo,fieldnames=fieldnames,extrasaction='ignore');wr.writeheader()
        for row in rd:
            found=cache.get(key(row.get('parcel_id')),[])
            chosen,status=_select_candidate(found,'bernalillo',normalize,suppsrc,'OBJECTID')
            stats[status]+=1
            if chosen:
                filled,cfs=merge_rows(row,chosen,'county_assessor_nm_bernalillo',as_text,positive,number)
                recovered.update(filled);conflicts.update(cfs)
                row['enrichment_matched']='yes'
                row['enrichment_method']=row.get('enrichment_method') or 'county_assessor_arcgis'
            elif status=='ambiguous':
                if chosen:
                    shared,cf=merge_rows(row,chosen,'county_assessor_arcgis_consensus',as_text,positive,number)
                    recovered.update(shared);conflicts.update(cf)
                    if shared:row['enrichment_matched']='ambiguous_shared_fields_only'
                _add_flag(row,'ambiguous_assessor_join')
            wr.writerow({k:row.get(k,'') for k in fieldnames})
    temp.replace(output_csv)
    return {'kind':'secondary_arcgis','source_url':url,'source_last_updated':'county-provided snapshot; verify year',
            'join_counts':dict(stats),'fields_filled':dict(recovered),'conflicts_preserved':dict(conflicts),
            'request_errors':fetch_err,'queried_upcs':len(ids)}


def _assessor_file_override(src,overrides):
    return overrides.get((src.state, re.sub(r'[^a-z0-9]','',src.county.lower())))


def enrich_county(input_csv,output_csv,src,args,county_dir,helpers,logger):
    """Run zero or more official county enrichment phases. Return report and output."""
    Source=helpers['Source'];normalizer=helpers['normalize'];as_text=helpers['as_text']
    pos=helpers['positive'];number=helpers['number'];session=helpers['session'];api_json=helpers['api_json']
    fieldnames=helpers['fieldnames']
    src_key=(src.state,re.sub(r'[^a-z0-9]','',src.county.lower()))
    config=args.assessor_rules.get(src_key) or COUNTY_SOURCES.get(src_key)
    enabled=args.assessor_enrich!='off'
    current=input_csv;phases=[];errors=[]
    if not enabled or not config:
        if enabled and not config and _assessor_file_override(src,args.assessor_files):
            message=('This county has an --assessor-file but no assessor mapping. '
                     'Add it to --assessor-rules-json to avoid guessing parcel joins.')
            if args.assessor_enrich=='require':raise EnrichmentError(message)
            return input_csv, {'enabled':True,'source_available':False,'phases':[], 'errors':[message]}
        return input_csv, {'enabled':enabled,'source_available':bool(config),'phases':[], 'errors':[]}
    if config['type']=='bulk':
        archive=_assessor_file_override(src,args.assessor_files)
        if archive is None:
            archive=config.get('local_file')
        if archive is None:
            try:
                s=requests.Session()
                link=(config.get('download_url') or locate_official_zip(s,config))
                if urlparse(link).scheme!='https':
                    raise EnrichmentError('Assessor URL must use HTTPS')
                archive=download_zip(s,link,county_dir/'_assessor_cache',logger)
            except Exception as exc:
                errors.append('Could not download official assessor bulk source: '+str(exc)[:250])
        if archive is not None:
            archive=Path(archive)
            if not archive.exists():errors.append('Assessor source not found: '+str(archive))
            else:
                if config.get('stages'):
                    stages=config['stages']
                elif src_key==('CO','elpaso'):
                    stages=[{'kind':'el_paso'}]
                elif src_key==('CO','weld'):
                    stages=[{'kind':'weld_ownership'},{'kind':'weld_current_inventory'}]
                else:
                    stages=[{'kind':'custom_bulk','parcel_field':config.get('parcel_field','parcel_id'),
                             'account_field':config.get('account_field','account'),
                             'member_hint':config.get('member_hint',''),
                             'field_map':config.get('field_map',{})}]
                for stage in stages:
                    kind=stage['kind']
                    fm=stage.get('field_map') or MAPPINGS.get(kind)
                    if not isinstance(fm,dict) or not fm:
                        errors.append(f'No supplemental field map defined for {kind}');continue
                    try:
                        with tempfile.TemporaryDirectory(prefix='assessor-index-',dir=county_dir) as tmp:
                            db,n=build_index(Path(tmp)/'source.sqlite',archive,kind,logger,
                                parcel_field=stage.get('parcel_field',''),account_field=stage.get('account_field',''),
                                member_hint=stage.get('member_hint',''))
                            nxt=county_dir / f'_{kind}_enriched.csv'
                            stats=apply_bulk(current,nxt,db,kind,src,normalizer,Source,as_text,pos,number,fieldnames,logger,field_map=fm)
                            db.close()
                            stats['source_file']=str(archive);stats['assessor_rows_indexed']=n
                            phases.append(stats);current=nxt
                    except Exception as exc:
                        errors.append(f'Assessor {kind} failed: {str(exc)[:250]}')
    elif config['type']=='arcgis':
        try:
            nxt=county_dir/'_secondary_arcgis_enriched.csv'
            stats=apply_bernalillo(current,nxt,src,session,api_json,normalizer,Source,as_text,pos,number,
                                  fieldnames,args.delay,logger,url=config.get('url',BERNALILLO_LAYER),
                                  parcel_field=config.get('parcel_field','UPC'),
                                  field_map=config.get('field_map') or MAPPINGS['bernalillo'])
            phases.append(stats);current=nxt
            errors.extend(stats.get('request_errors',[]))
        except Exception as exc:
            errors.append('Secondary GIS enrichment failed: '+str(exc)[:250])
    if args.assessor_enrich=='require' and (errors or not phases):
        raise EnrichmentError('Required assessor enrichment failed: '+'; '.join(errors or ['no successful phases']))
    return current, {'enabled':enabled,'source_available':True,'phases':phases,'errors':errors,
                     'source_reference':config.get('landing_page') or config.get('url'),
                     'note':'Only vacant fields filled; assessor acreage may replace GIS-estimated acreage. Conflicts preserved.'}
