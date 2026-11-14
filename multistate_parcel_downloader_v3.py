#!/usr/bin/env python3
"""Multi-State Parcel Downloader v3.1 (WY, CO, OK, NM).

Downloads publicly queryable ArcGIS FeatureServer parcel layers using *all* object
IDs, not an unreliable first 1,000/2,000 rows. Also reads local CSV / SHP / GDB
files (including ZIPs). Produces raw/normalized CSV, QA reports and resume chunks.

Coverage is NOT statewide in OK/NM. Built-ins do not circumvent view-only
sources or licensing restrictions. See README before use.

Examples:
  python multistate_parcel_downloader_v3.py --state WY --counties Natrona Laramie
  python multistate_parcel_downloader_v3.py --state CO --counties Weld "El Paso"
  python multistate_parcel_downloader_v3.py --state OK --counties Tulsa Canadian
  python multistate_parcel_downloader_v3.py --state NM --counties Bernalillo
  python multistate_parcel_downloader_v3.py --target WY:Natrona --target OK:Tulsa --max-parcels 100 --dry-run
  python multistate_parcel_downloader_v3.py --state CO --counties ALL --workers 2
  python multistate_parcel_downloader_v3.py --list-sources
"""
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import hashlib
import json
import math
import os
import random
import re
import shutil
import sys
import time
import traceback
import zipfile
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from owner_name_repair_v2_3 import repair_owner_name_scalar
except ImportError:
    repair_owner_name_scalar = None

VERSION = '3.1.0'
SQM_PER_ACRE = 4046.8564224
URL_CO = 'https://gis.colorado.gov/public/rest/services/Address_and_Parcel/Colorado_Public_Parcels/FeatureServer/0'
URL_WY = 'https://services3.arcgis.com/r0iJ85SKZ4zAzz3P/ArcGIS/rest/services/Wyoming_Parcels_for_2026/FeatureServer/0'

# County-level public datasets. These links are discoverable and subject to
# replacement by their publishers; runtime metadata and query permission checked.
BUILTIN: dict[str, dict[str, dict[str, str]]] = {
    'WY': {'*': {'url': URL_WY, 'county_field': 'jurisdicti', 'county_format': 'compact_upper',
                 'publisher': 'Wyoming DOR / county assessors, tax year 2026'}},
    'CO': {'*': {'url': URL_CO, 'county_field': 'countyName', 'county_format': 'title',
                 'publisher': 'Colorado Governor OIT; attribution required; resale prohibited'}},
    'OK': {
        'canadian': {'url': 'https://services2.arcgis.com/0NjdXxmJp53hZWPd/ArcGIS/rest/services/ParcelDataService_2_view/FeatureServer/3',
                     'publisher': 'Canadian County Assessor'},
        'cleveland': {'url': 'https://services1.arcgis.com/kxHJSF07PF5lbQ3z/arcgis/rest/services/ClevelandTaxParcelsJune2026/FeatureServer/0',
                      'publisher': 'Cleveland County Assessor (June 2026 snapshot)'},
        'oklahoma': {'url': 'https://services8.arcgis.com/euhkr1dAJeQBIjV0/arcgis/rest/services/TaxParcelsPublics_view/FeatureServer/0',
                     'publisher': 'Oklahoma County Assessor'},
        'tulsa': {'url': 'https://map11.incog.org/arcgis11wa/rest/services/Parcels_TulsaCo/FeatureServer/0',
                  'publisher': 'INCOG / Tulsa County Assessor'},
    },
    'NM': {
        'bernalillo': {'url': 'https://services.arcgis.com/CWv1abTnC3urn4bV/ArcGIS/rest/services/berncoparcels_forIDO/FeatureServer/0',
                       'publisher': 'Bernalillo County Assessor extract / City of Albuquerque'},
    },
}

FIELDS = [
    'id', 'full_name', 'legal_address', 'mailing_address', 'acres', 'city', 'zip', 'state', 'market_value',
    'state_abbr', 'county', 'parcel_id', 'parcel_id_raw', 'parcel_id_list', 'parcel_id_count',
    'account_id', 'source_objectid', 'id_source', 'id_quality',
    'full_name_raw', 'full_name_secondary', 'full_name_quality', 'full_name_repair_method',
    'full_name_repair_confidence', 'property_address', 'property_city', 'property_state',
    'property_zip', 'mailing_city', 'mailing_state', 'mailing_zip', 'legal_description',
    'acres_source', 'geometry_acres', 'market_value_source', 'actual_value', 'appraised_value',
    'assessed_value', 'taxable_value', 'land_value', 'improvement_value', 'total_value',
    'property_class', 'land_use', 'sale_date', 'sale_price', 'tax_year',
    'source_url', 'source_publisher', 'source_data_year', 'data_quality_flags',
    # Source provenance is distinct from values so an estimate cannot masquerade
    # as assessor-verified acreage or a mailing ZIP as a situs ZIP.
    'full_name_source', 'property_address_source', 'property_city_source',
    'property_zip_source', 'mailing_address_source', 'mailing_city_source',
    'mailing_state_source', 'mailing_zip_source', 'legal_address_source',
    'city_source', 'zip_source', 'actual_value_source', 'appraised_value_source',
    'assessed_value_source', 'taxable_value_source', 'total_value_source',
    'land_value_source', 'improvement_value_source', 'legal_description_source',
    'enrichment_method', 'enrichment_matched', 'enrichment_fields_filled',
    'enrichment_review_flags', 'property_city_raw', 'property_address_2_raw',
]
REQUIRED = ['id', 'full_name', 'legal_address', 'mailing_address', 'acres', 'city', 'zip', 'state', 'market_value']

# Exact-field priority matters. Never mistake mailing city for situs city;
# never treat a generic assessed/taxable/total value as a market value.
ALIAS: dict[str, list[str]] = {
    'parcel_id': ['parcel_id', 'PARCEL', 'parcelnb', 'ParcelNo', 'PARCELNB_1', 'pin', 'ParcelID',
                  'UPC', 'UserAccount', 'PROP_ID', 'APN', 'property_id', 'acct', 'accountno',
                  'account', 'ACCT_NUM', 'pid', 'ID'],
    'account_id': ['accountno', 'account', 'AccountNo', 'UserAccount', 'ACCT_NUM', 'account_id', 'a_number'],
    'full_name': ['ownername1', 'OWNER1', 'owner', 'owners_name', 'OwnerNameFormatted', 'name1',
                  'OWNER', 'OWNER_NAME', 'GIS_ParcelInfo_View_Owner', 'TaxPayerName', 'OwnerName'],
    'full_name_secondary': ['ownername2', 'owner2', 'Name2', 'OWNER2'],
    'property_address': ['situsAdd', 'locationad', 'situs', 'FormattedLocation', 'PropertyAddress',
                         'LOCATION', 'SITUSADD', 'situs_addr', 'SitusAddress', 'CompleteSiteAddress'],
    'property_address_2': ['SITUSADD2', 'site_address_2', 'site_addr2'],
    'property_state': ['SITUSSTATE', 'situs_state', 'property_state', 'SiteState'],
    'property_city': ['sitAddCty', 'situs_city', 'SITUSCITY', 'PropertyCity', 'LocationCity', 'locationcity',
                      'SiteCity', 'SITUS_CITY', 'situsCity'],
    'property_zip': ['sitAddZip', 'LOCATIONZIP', 'situs_zip', 'SITUSZIP', 'PropertyZIP', 'SITUS_ZIP', 'situsZip'],
    'mailing_full': ['CompleteOwnerAddress', 'MailingAddress', 'MAIL_ADDR', 'mailing_address'],
    'mailing_line1': ['ownerAdd', 'MAILADR', 'mailaddres', 'mail_address', 'MailingAddressLine1',
                      'OWNADD', 'Address1', 'mail_info', 'OwnerAddress', 'mail_line1'],
    'mailing_line2': ['OWNADD2', 'mail_info2', 'MailingAddressLine2', 'Address2', 'mail_line2'],
    'mailing_city': ['ownAddCty', 'MAILCITY', 'mailcity', 'mail_city', 'MailingCity', 'OWNCITY', 'City'],
    'mailing_state': ['ownAddStt', 'MAILSTATE', 'mailstate', 'mail_state', 'MailingState', 'State'],
    'mailing_zip': ['ownAddZip', 'MAILZIPCODE', 'mailzipcod', 'mail_zip', 'MailingZIP', 'ZIPCode', 'ZIP', 'OwnerZipCode'],
    'legal_description': ['legalDesc', 'PARTIALLEGAL', 'legal', 'Legal', 'LEGALDESC', 'LEGAL_DESC', 'LegalReference'],
    'acres': ['landAcres', 'landgrossa', 'GrossAcre', 'ACREAGE', 'Acreage', 'acres',
              'legalAcreage', 'land_acres', 'effectiveSizeAcres', 'GIS_AREA'],
    'sqft': ['landSqft', 'landgrosss', 'land_sqft', 'lot_square_feet'],
    'parcel_size': ['parcel_size', 'ParcelSize', 'lot_size'],
    'parcel_size_units': ['parcel_size_units', 'ParcelSizeUnits', 'lot_size_units'],
    'market_value': ['MKT_VALUE', 'market_value', 'MarketValue', 'marketvalue', 'Market_Value'],
    'actual_value': ['actualvalu', 'actual_value', 'ActualValue'],
    'appraised_value': ['apprValTot', 'appraised_value', 'AppraisedValue', 'ownerAppraisedValue'],
    'assessed_value': ['assessedva', 'ASSESSEDVALUE', 'total_assess', 'assessed_value', 'TotalAssessedValue'],
    'taxable_value': ['asedValTot', 'NETTAXABLE', 'taxable_val', 'TaxableValue', 'taxable_value'],
    'total_value': ['TotalAcctValue', 'TOTVALUE', 'total_val', 'total_value', 'TCV', 'TotalValue'],
    'land_value': ['TotalLandValue', 'land_val', 'land_value', 'LANDVALUE', 'LAND_VALUE'],
    'improvement_value': ['TotalImpValue', 'bldg_val', 'improvement_value', 'IMPTVALUE', 'IMP_VALUE'],
    'property_class': ['PROPCLASS', 'prop_class', 'PropertyType', 'PAR_TYPE', 'AcctType', 'propType'],
    'land_use': ['landUseDsc', 'landUseCde', 'UseCode', 'LOC_LAND_USE', 'STAT_LAND_USE', 'useCd'],
    'sale_date': ['saleDate', 'SaleDate', 'DocumentDate', 'LastSaleDate1'],
    'sale_price': ['salePrice', 'SalePrice', 'LastSalePrice'],
    'tax_year': ['taxyear', 'TAXYR', 'tax_year', 'TaxYear'],
    'source_data_year': ['dateReceived', 'LastUpdate', 'LoadDate', 'DATE_ACQ'],
}

@dataclass
class Source:
    state: str
    county: str
    url: str | None = None
    local_file: str | None = None
    county_field: str | None = None
    county_format: str = 'title'
    where: str = '1=1'
    publisher: str = 'Source provided by user'
    field_map: dict[str, list[str]] = field(default_factory=dict)


def timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def slug(v: str) -> str:
    return re.sub(r'[^a-z0-9]+', '_', str(v).casefold()).strip('_')


def county_key(v: str) -> str:
    return re.sub(r'[^a-z0-9]+', '', re.sub(r'\s+county$', '', v.strip(), flags=re.I).casefold())


def display_county(v: str) -> str:
    return re.sub(r'\s+county$', '', v.strip(), flags=re.I).strip()


def log(prefix: str, msg: str) -> None:
    print(f'[{prefix}] {msg}', flush=True)


def dur(sec: float | None) -> str:
    if sec is None or not math.isfinite(sec) or sec < 0:
        return '?'
    h, rem = divmod(int(sec), 3600)
    m, s = divmod(rem, 60)
    return f'{h:d}:{m:02d}:{s:02d}' if h else f'{m:d}:{s:02d}'


def as_text(v: Any) -> str:
    if v is None:
        return ''
    if isinstance(v, float) and math.isnan(v):
        return ''
    s = str(v).strip().replace('\u00a0', ' ')
    return '' if s.casefold() in {'none', 'null', '<na>', 'nan', 'n/a', 'unknown', 'unassigned',
                                'no address', 'not available', 'not assigned', 'none provided'} else s


def number(v: Any) -> float | None:
    s = as_text(v)
    if not s:
        return None
    s = re.sub(r'[$,\s]', '', s)
    s = re.sub(r'^(\(.*\))$', r'-\1', s)
    m = re.fullmatch(r'-?\(?\d+(?:\.\d+)?\)?', s)
    if not m:
        return None
    try:
        n = float(s.strip('()'))
        return n if math.isfinite(n) else None
    except ValueError:
        return None


def positive(v: Any) -> float | None:
    n = number(v)
    return n if n is not None and n > 0 else None


def clean_zip(v: Any) -> str:
    s = as_text(v)
    if not s:
        return ''
    s = re.sub(r'\.0+$', '', s)
    digits = re.sub(r'\D', '', s)
    return digits if len(digits) == 5 else digits[:5] + '-' + digits[5:] if len(digits) == 9 else ''


VALID_STATES = set(('AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN '
                    'MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA '
                    'WA WV WI WY DC').split())


def city_is_valid(value: Any) -> bool:
    s = as_text(value).strip().casefold()
    return bool(s and not s.startswith('unincorporated') and s not in {'outside city limits',
                         'rural','no city','n/a','unknown','other','county'})


def parse_city_state_zip(line: Any, assumed_state: str = '') -> tuple[str,str,str]:
    """Parse only an isolated address *second line* such as ALBUQUERQUE NM 87122.

    Do not infer a city from a whole street address or a mailing address.
    """
    s = as_text(line)
    m = re.fullmatch(r'\s*([^,]+?)\s*,?\s+([A-Za-z]{2})\s+(\d{5}(?:[- ]?\d{4})?)\s*',s)
    if not m:
        # A few county source exports omit the state *only* on the property
        # address second line (example: Bernalillo ALBUQUERQUE 87122).
        # Never use this inference for MAILING addresses, which may be out-of-state.
        if assumed_state in VALID_STATES:
            loose=re.fullmatch(r'\s*([A-Za-z][A-Za-z .\'-]*?)\s+(\d{5}(?:[- ]?\d{4})?)\s*',s)
            if loose and city_is_valid(loose.group(1)):
                return loose.group(1).strip(), assumed_state, clean_zip(loose.group(2))
        return '', '', ''
    city,st,z = m.groups();st=st.upper()
    if st not in VALID_STATES or not city_is_valid(city) or re.search(r'\d',city):return '', '', ''
    return city.strip(),st,clean_zip(z)


def simple_field(attrs: dict[str, Any], name: str, overrides: dict[str, list[str]] | None = None) -> tuple[Any, str]:
    lookup = {k.casefold(): k for k in attrs}
    cand = (overrides or {}).get(name, []) + ALIAS.get(name, [])
    for c in cand:
        actual = lookup.get(str(c).casefold())
        if actual is not None and as_text(attrs.get(actual)):
            return attrs[actual], actual
    return None, ''


def full_address(parts: Iterable[Any], separator: str = ', ') -> str:
    return separator.join(x for x in (as_text(p) for p in parts) if x)


def acres_from_attributes(attrs: dict[str, Any], overrides: dict[str, list[str]]) -> tuple[float | None, str]:
    v, k = simple_field(attrs, 'acres', overrides)
    n = positive(v)
    if n is not None:
        return n, k
    v, k = simple_field(attrs, 'parcel_size', overrides)
    units, _ = simple_field(attrs, 'parcel_size_units', overrides)
    x = positive(v)
    u = as_text(units).lower()
    if x is not None and u:
        if 'acre' in u or u == 'ac':
            return x, k + ':acres'
        if any(tag in u for tag in ['sqft', 'sq ft', 'square feet', 'ft2']):
            return x / 43560.0, k + ':sqft'
        if any(tag in u for tag in ['sq m', 'sqm', 'square meter', 'm2']):
            return x / SQM_PER_ACRE, k + ':sqm'
    v, k = simple_field(attrs, 'sqft', overrides)
    x = positive(v)
    if x is not None:
        return x / 43560.0, k + ':sqft'
    return None, ''


def clean_id(v: Any) -> str:
    t = as_text(v)
    if re.fullmatch(r'[0-9]+\.0+', t):
        t = t.split('.')[0]
    # Never cast to float: parcel IDs often include leading zeros/letters.
    if t in {'0', '0000000'}:
        return ''
    return t


def parcel_id_parts(value: Any) -> tuple[str, str, int]:
    """Preserve multiple IDs without merging them into one false parcel identifier."""
    raw = as_text(value)
    if not raw:
        return '', '', 0
    valid = []
    for part in raw.split(','):
        text = clean_id(part)
        if not text or not re.search(r'\d', text):
            continue
        if re.search(r'\d\s*(?:AC|ACRE|ACRES)\b', text, flags=re.I):
            continue
        if not re.fullmatch(r'[A-Za-z0-9]+(?:[.\/_-][A-Za-z0-9]+)*', text):
            continue
        if text not in valid:
            valid.append(text)
    return (valid[0], ' | '.join(valid), len(valid)) if valid else ('', '', 0)


def normalize(attrs: dict[str, Any], source: Source, oid_field: str, *, geometry_acres: float | None = None) -> dict[str, Any]:
    def field(k: str) -> tuple[Any,str]:
        return simple_field(attrs,k,source.field_map)
    def get(k: str) -> Any:
        return field(k)[0]
    parcel, parcel_col = field('parcel_id')
    account, account_col = field('account_id')
    owner, owner_col = field('full_name')
    owner2 = as_text(get('full_name_secondary'))
    pid, pid_list, pid_count = parcel_id_parts(parcel)
    aid = clean_id(account)
    oid = clean_id(attrs.get(oid_field))
    if pid:
        ident, id_col, quality = pid, parcel_col, 'PARCEL_ID_MULTI_PRIMARY_FIRST' if pid_count > 1 else 'PARCEL_ID'
    elif aid:
        ident, id_col, quality = aid, account_col, 'ACCOUNT_ID_FALLBACK'
    elif oid:
        ident, id_col, quality = 'GIS_OID:' + oid, oid_field, 'GIS_OID_FALLBACK_NOT_CAD_ID'
    else:
        ident, id_col, quality = '', '', 'MISSING'

    # Keep situs and mailing contexts separate to prevent an owner mailing ZIP
    # being guessed as a parcel's physical ZIP.
    site_raw,site_col = field('property_address')
    site = as_text(site_raw)
    site2,site2_col = field('property_address_2')
    site2 = as_text(site2)
    site_city_raw,site_city_col = field('property_city')
    site_city=as_text(site_city_raw) if city_is_valid(site_city_raw) else ''
    site_zip_raw,site_zip_col = field('property_zip')
    site_zip=clean_zip(site_zip_raw)
    site_state_raw,site_state_col = field('property_state')
    site_state = as_text(site_state_raw).upper() if as_text(site_state_raw).upper() in VALID_STATES else source.state
    inferred_city,parsed_state,inferred_zip=parse_city_state_zip(site2,assumed_state=source.state)
    if not site_city and inferred_city and parsed_state == source.state:
        site_city=inferred_city;site_city_col=site2_col+':parsed_city'
    if not site_zip and inferred_zip and parsed_state == source.state:
        site_zip=inferred_zip;site_zip_col=site2_col+':parsed_zip'
    # The composite site address sometimes contains exactly the same name+ZIP
    # as structured site2. Prefer the street-only SITUSADD when present.
    if site2 and site and site.upper().endswith(site2.upper()):
        site=site[:-len(site2)].strip(' ,') or site
    site=as_text(site)

    mail_full,mail_full_col=field('mailing_full')
    mail_full=as_text(mail_full)
    mail1,mail1_col=field('mailing_line1');mail1=as_text(mail1)
    mail2,mail2_col=field('mailing_line2');mail2=as_text(mail2)
    mail_city,mail_city_col=field('mailing_city');mail_city=as_text(mail_city)
    mail_state,mail_state_col=field('mailing_state');mail_state=as_text(mail_state).upper()
    mail_zip,mail_zip_col=field('mailing_zip');mail_zip=clean_zip(mail_zip)
    parsed_mail_city,parsed_mail_state,parsed_mail_zip=parse_city_state_zip(mail2)
    if not city_is_valid(mail_city) and parsed_mail_city:
        mail_city=parsed_mail_city;mail_city_col=mail2_col+':parsed_city'
    if (not mail_state or mail_state not in VALID_STATES) and parsed_mail_state:
        mail_state=parsed_mail_state;mail_state_col=mail2_col+':parsed_state'
    if not mail_zip and parsed_mail_zip:
        mail_zip=parsed_mail_zip;mail_zip_col=mail2_col+':parsed_zip'
    # When mail2 is a city/state/ZIP line, do not treat it as a street line.
    if mail1:
        line1=mail1 if parsed_mail_city else full_address([mail1,mail2],' ')
        mail=full_address([line1,mail_city,full_address([mail_state,mail_zip],' ')])
    else:
        mail=mail_full
    # Make sure an existing complete mailing address is not lost when the source
    # supplies structured metadata but no street line.
    if not mail and mail_full:mail=mail_full
    if mail and mail_full and not mail1:mail_full_col=mail_full_col or 'source_full_address'
    mail_source=(mail1_col or mail_full_col)

    acres,acres_col=acres_from_attributes(attrs,source.field_map)
    if acres is None and geometry_acres is not None and geometry_acres>0:
        acres,acres_col=geometry_acres,'GEOMETRY_EPSG5070_ESTIMATE'
    market_raw,market_col=field('market_value')
    market_value=number(market_raw)
    actual_value=number(get('actual_value'))
    appraised_value=number(get('appraised_value'))
    assessed_value=number(get('assessed_value'))
    taxable_value=number(get('taxable_value'))
    if market_value is None and source.state=='WY' and actual_value is not None:
        market_value=actual_value
        market_col='actualvalu [ASSESSOR_ACTUAL_VALUE]'
    name=as_text(owner)
    result={k:'' for k in FIELDS}
    values={
        'id':ident, 'full_name':name,'full_name_raw':name,'full_name_secondary':owner2,
        'full_name_quality':'source_spaced' if re.search(r'\s',name) else 'source_compact_unreviewed' if name else 'missing',
        'full_name_repair_method':'none','full_name_repair_confidence':'',
        'legal_address':site,'property_address':site,'mailing_address':mail,
        'acres':acres or '', 'city':site_city,'zip':site_zip,'state':source.state,
        'market_value': market_value if market_value is not None else '',
        'state_abbr':source.state,'county':source.county,
        'parcel_id':pid,'parcel_id_raw':as_text(parcel),'parcel_id_list':pid_list,'parcel_id_count':pid_count,
        'account_id':aid,'source_objectid':oid,'id_source':id_col,'id_quality':quality,
        'property_city':site_city,'property_state':site_state,'property_zip':site_zip,
        'mailing_city':mail_city,'mailing_state':mail_state,'mailing_zip':mail_zip,
        'legal_description':as_text(get('legal_description')),
        'acres_source':acres_col,'geometry_acres':geometry_acres if geometry_acres is not None else '',
        'market_value_source':market_col,'actual_value':actual_value if actual_value is not None else '',
        'appraised_value':appraised_value if appraised_value is not None else '',
        'assessed_value':assessed_value if assessed_value is not None else '',
        'taxable_value':taxable_value if taxable_value is not None else '',
        'land_value':number(get('land_value')),'improvement_value':number(get('improvement_value')),
        'total_value':number(get('total_value')),'property_class':as_text(get('property_class')),
        'land_use':as_text(get('land_use')),'sale_date':as_text(get('sale_date')),
        'sale_price':number(get('sale_price')),'tax_year':as_text(get('tax_year')),
        'source_url':source.url or source.local_file or '', 'source_publisher':source.publisher,
        'source_data_year':as_text(get('source_data_year')),
        'full_name_source':owner_col,'property_address_source':site_col,
        'property_city_source':site_city_col,'property_zip_source':site_zip_col,
        'legal_address_source':site_col,'city_source':site_city_col,'zip_source':site_zip_col,
        'mailing_address_source':mail_source,'mailing_city_source':mail_city_col,
        'mailing_state_source':mail_state_col,'mailing_zip_source':mail_zip_col,
        'property_city_raw':as_text(site_city_raw),'property_address_2_raw':site2,
    }
    for k in ('actual_value','appraised_value','assessed_value','taxable_value','total_value',
              'land_value','improvement_value','legal_description'):
        if k+'_source' in result and values.get(k) not in (None,''):
            values[k+'_source']=field(k)[1]
    result.update(values)
    return set_quality_flags({k:'' if v is None else v for k,v in result.items()})


def set_quality_flags(row:dict[str,Any])->dict[str,Any]:
    issues=[]
    if not as_text(row.get('full_name')):issues.append('missing_owner')
    if not as_text(row.get('property_address')):issues.append('missing_property_address')
    if not as_text(row.get('mailing_address')):issues.append('missing_mailing_address')
    if not city_is_valid(row.get('property_city')):issues.append('missing_property_city')
    if not clean_zip(row.get('property_zip')):issues.append('missing_property_zip')
    if positive(row.get('acres')) is None:issues.append('missing_acres')
    if positive(row.get('market_value')) is None:issues.append('missing_or_zero_market_value')
    if row.get('id_quality') not in ('PARCEL_ID','PARCEL_ID_MULTI_PRIMARY_FIRST'):
        issues.append('id_fallback')
    if str(row.get('acres_source') or '').startswith('GEOMETRY'):
        issues.append('geometry_acres_unverified')
    if as_text(row.get('property_city_raw')).upper()=='UNINCORPORATED':
        issues.append('unincorporated_not_city')
    row['data_quality_flags']='|'.join(issues)
    return row


def equal_area_acres(geom: dict[str, Any]) -> float | None:
    """Compute EPSG:5070 planar polygon acreage, respecting Esri ring orientation.

    Caller MUST query ArcGIS with outSR=5070. Signed area cancels holes.
    The resulting area is a GIS approximation, not legal/deeded acreage.
    """
    rings = geom.get('rings') if isinstance(geom, dict) else None
    if not isinstance(rings, list):
        return None
    signed = 0.0
    for ring in rings:
        if not isinstance(ring, list) or len(ring) < 3:
            continue
        vals = []
        for i, xy in enumerate(ring):
            nxt = ring[(i + 1) % len(ring)]
            if len(xy) < 2 or len(nxt) < 2:
                continue
            vals.append(float(xy[0]) * float(nxt[1]) - float(nxt[0]) * float(xy[1]))
        signed += math.fsum(vals) / 2.0
    result = abs(signed) / SQM_PER_ACRE
    return result if math.isfinite(result) and result >= 0.0001 else None


def session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=6, connect=6, read=6, status=6, backoff_factor=0.8,
                  respect_retry_after_header=True, status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=['GET'])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    s.mount('https://', adapter)
    s.headers.update({'User-Agent': 'MultiStateParcelDownloader/3.0 (public GIS data research)',
                      'Accept': 'application/json, */*'})
    return s


class GISFailure(Exception):
    pass


def api_json(s: requests.Session, url: str, params: dict[str, Any], *, timeout: int = 90, delay: float = 0) -> dict[str, Any]:
    parsed = urlparse(url)
    if parsed.scheme != 'https' or not parsed.hostname:
        raise GISFailure('ArcGIS sources must have an HTTPS URL.')
    r = s.get(url, params={'f': 'json', **params}, timeout=(20, timeout))
    if delay:
        time.sleep(delay)
    if r.status_code >= 400:
        raise GISFailure(f'HTTP {r.status_code} from {parsed.hostname}: {r.text[:250]}')
    try:
        data = r.json()
    except ValueError as exc:
        raise GISFailure(f'Non-JSON ArcGIS response from {parsed.hostname}') from exc
    if not isinstance(data, dict):
        raise GISFailure('Expected an ArcGIS JSON object')
    if data.get('error'):
        error = data['error']
        raise GISFailure(f'ArcGIS error: {error.get("code")} {error.get("message")} {str(error.get("details"))[:250]}')
    return data


def metadata(s: requests.Session, src: Source, delay: float) -> tuple[dict[str, Any], str, list[str]]:
    if not src.url:
        raise GISFailure('Source missing ArcGIS URL')
    data = api_json(s, src.url, {}, delay=delay)
    oid = data.get('objectIdField') or data.get('objectIdFieldName')
    fields = data.get('fields', [])
    if not oid:
        ids = [f.get('name') for f in fields if f.get('type') == 'esriFieldTypeOID']
        oid = ids[0] if ids else ''
    if not oid:
        raise GISFailure('ArcGIS layer has no object ID field; cannot guarantee complete extraction')
    if data.get('type') == 'Table' or ('Polygon' not in str(data.get('geometryType') or '')):
        raise GISFailure('Expected a parcel polygon Feature Layer, not a table or non-polygon layer')
    return data, str(oid), [str(x.get('name')) for x in fields if x.get('name')]


def where_for(src: Source) -> str:
    where = src.where.strip() or '1=1'
    if src.county_field:
        v = re.sub(r'[^A-Za-z0-9 ]', '', src.county)
        if src.county_format == 'compact_upper':
            v = re.sub(r'\s+', '', v).upper()
        elif src.county_format == 'upper':
            v = v.upper()
        return f'({where}) AND {src.county_field} = \'{v}\''
    return where


def layer_ids(s: requests.Session, src: Source, *, delay: float = 0) -> list[int]:
    where = where_for(src)
    result = api_json(s, src.url + '/query', {'where': where, 'returnIdsOnly': 'true',
                                               'returnGeometry': 'false'}, timeout=180, delay=delay)
    ids = result.get('objectIds')
    if ids is None:
        raise GISFailure('ArcGIS returnIdsOnly did not provide objectIds. No first-page fallback: refusing incomplete data.')
    ids = sorted(set(int(x) for x in ids))
    # A separate count check avoids silently skipping records when a provider misbehaves.
    count_result = api_json(s, src.url + '/query', {'where': where, 'returnCountOnly': 'true'}, delay=delay)
    count = count_result.get('count')
    if count is not None and len(ids) != int(count):
        raise GISFailure(f'Inconsistent ArcGIS count: {count} records vs {len(ids)} unique IDs; refusing incomplete export.')
    return ids


def fetch_features(s: requests.Session, src: Source, oids: list[int], oid_field: str,
                   *, geometry: bool, delay: float) -> tuple[list[dict[str, Any]], list[int]]:
    """Retrieve every requested OID; bisect on transfer limits/missing rows."""
    if not oids:
        return [], []
    params: dict[str, Any] = {'objectIds': ','.join(map(str, oids)), 'outFields': oid_field if geometry else '*',
                              'returnGeometry': 'true' if geometry else 'false', 'outSR': '5070'}
    try:
        reply = api_json(s, src.url + '/query', params, delay=delay)
    except GISFailure:
        if len(oids) <= 1:
            raise
        mid = len(oids) // 2
        left, miss1 = fetch_features(s, src, oids[:mid], oid_field, geometry=geometry, delay=delay)
        right, miss2 = fetch_features(s, src, oids[mid:], oid_field, geometry=geometry, delay=delay)
        return left + right, miss1 + miss2
    feats = reply.get('features')
    if not isinstance(feats, list):
        raise GISFailure('ArcGIS query returned no features array')
    got: dict[int, dict[str, Any]] = {}
    for f in feats:
        if not isinstance(f, dict) or not isinstance(f.get('attributes'), dict):
            continue
        raw_oid = f['attributes'].get(oid_field)
        if raw_oid is not None:
            got[int(raw_oid)] = f
    missing = [oid for oid in oids if oid not in got]
    if missing and len(oids) > 1:
        # Try just the unreturned IDs; may be a hidden result-length limit.
        if len(missing) == len(oids):
            mid = len(oids) // 2
            left, miss1 = fetch_features(s, src, oids[:mid], oid_field, geometry=geometry, delay=delay)
            right, miss2 = fetch_features(s, src, oids[mid:], oid_field, geometry=geometry, delay=delay)
            return left + right, miss1 + miss2
        more, still_missing = fetch_features(s, src, missing, oid_field, geometry=geometry, delay=delay)
        for f in more:
            got[int(f['attributes'][oid_field])] = f
        missing = still_missing
    # True missing IDs can arise when a parcel changes during a long export.
    return [got[i] for i in oids if i in got], missing


def geometry_fallback(s: requests.Session, src: Source, feats: list[dict[str, Any]],
                      oid_field: str, mode: str, delay: float) -> tuple[dict[int, float], list[str]]:
    if mode == 'never':
        return {}, []
    needed = []
    for f in feats:
        attrs = f['attributes']
        ac, _ = acres_from_attributes(attrs, src.field_map)
        if mode == 'always' or ac is None:
            needed.append(int(attrs[oid_field]))
    if not needed:
        return {}, []
    try:
        polys, missing = fetch_features(s, src, needed, oid_field, geometry=True, delay=delay)
        areas = {}
        for x in polys:
            a = equal_area_acres(x.get('geometry') or {})
            if a is not None:
                areas[int(x['attributes'][oid_field])] = a
        notices = [f'Geometry not returned for {len(missing)} parcel(s).'] if missing else []
        return areas, notices
    except GISFailure as exc:
        return {}, [f'Geometry-acreage fallback unavailable: {exc}']


def csv_output(path: Path, names: list[str], rows: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.tmp')
    total = 0
    with tmp.open('w', encoding='utf-8-sig', newline='') as fout:
        writer = csv.DictWriter(fout, fieldnames=names, extrasaction='ignore', lineterminator='\n')
        writer.writeheader()
        for row in rows:
            writer.writerow({k: '' if row.get(k) is None else row.get(k) for k in names})
            total += 1
    tmp.replace(path)
    return total


def combine_chunks(chunks: list[Path], destination: Path, fieldnames: list[str]) -> int:
    def rows():
        for p in chunks:
            with p.open(encoding='utf-8-sig', newline='') as fin:
                reader = csv.DictReader(fin)
                for x in reader:
                    yield x
    return csv_output(destination, fieldnames, rows())


def fix_names(src: Path, dest: Path, audit_path: Path, mode: str, min_compact_percent: float) -> dict[str, Any]:
    if mode not in ('auto', 'always', 'never'):
        raise ValueError(mode)
    seen = compact = alpha = 0
    with src.open(encoding='utf-8-sig', newline='') as fin:
        for r in csv.DictReader(fin):
            name = as_text(r.get('full_name_raw'))
            if name:
                seen += 1
                if not re.search(r'\s', name):
                    compact += 1
                    if re.fullmatch('[A-Za-z]+', name):
                        alpha += 1
    pct = 100 * compact / seen if seen else 0
    alpha_pct = 100 * alpha / seen if seen else 0
    detected = bool(seen >= 50 and pct >= min_compact_percent and alpha_pct >= 75)
    apply = bool(mode == 'always' or mode == 'auto' and detected)
    if mode == 'never' or repair_owner_name_scalar is None:
        apply = False
    applied = unresolved = 0
    cache = {}
    def records():
        nonlocal applied, unresolved
        with src.open(encoding='utf-8-sig', newline='') as fin:
            for row in csv.DictReader(fin):
                name = as_text(row.get('full_name_raw'))
                if apply and name and not re.search(r'\s', name):
                    if name not in cache:
                        cache[name] = repair_owner_name_scalar(name)
                    candidate, method, confidence, changed = cache[name]
                    row['full_name_repair_method'] = method
                    row['full_name_repair_confidence'] = confidence
                    if changed:
                        row['full_name'] = candidate
                        row['full_name_quality'] = 'repaired_high_confidence'
                        applied += 1
                    else:
                        row['full_name_quality'] = method
                        unresolved += 1
                yield row
    count = csv_output(dest, FIELDS, records())
    if apply:
        names = ['state', 'county', 'id', 'source_objectid', 'full_name_raw', 'full_name',
                 'full_name_repair_method', 'full_name_repair_confidence', 'full_name_quality']
        def audit_records():
            with dest.open(encoding='utf-8-sig', newline='') as fin:
                for row in csv.DictReader(fin):
                    raw = as_text(row.get('full_name_raw'))
                    if raw and not re.search(r'\s', raw):
                        yield {key: row.get(key, '') for key in names}
        csv_output(audit_path, names, audit_records())
    return {'rows': count, 'nonblank_names': seen, 'compact_names': compact,
            'compact_percent': round(pct, 3), 'compact_alpha_percent': round(alpha_pct, 3),
            'compression_detected': detected, 'repair_ran': apply, 'repair_applied': applied,
            'compressed_unresolved': unresolved,
            'audit_file': str(audit_path) if apply else None,
            'note': 'Original owner string retained in full_name_raw; uncertain cases unchanged.'}


def filter_and_audit(src: Path, dest: Path, min_acres: float, keep_missing: bool) -> dict[str, Any]:
    total = kept = below = missing_acres = 0
    coverage = Counter()
    id_quality = Counter()
    def records():
        nonlocal total, kept, below, missing_acres
        with src.open(encoding='utf-8-sig', newline='') as fin:
            for row in csv.DictReader(fin):
                total += 1
                for col in REQUIRED:
                    if col in ('acres','market_value'):
                        has_value=positive(row.get(col)) is not None
                    elif col=='city':
                        has_value=city_is_valid(row.get(col))
                    elif col=='zip':
                        has_value=bool(clean_zip(row.get(col)))
                    else:
                        has_value=bool(as_text(row.get(col)))
                    if has_value:coverage[col]+=1
                id_quality[row.get('id_quality') or 'MISSING'] += 1
                a = positive(row.get('acres'))
                if min_acres > 0 and (a is None and not keep_missing or a is not None and a < min_acres):
                    below += 1
                    continue
                if a is None:
                    missing_acres += 1
                kept += 1
                yield row
    csv_output(dest, FIELDS, records())
    return {'all_rows': total, 'exported_rows': kept, 'filtered_out_rows': below,
            'missing_acres_in_export': missing_acres, 'id_quality_counts': dict(id_quality),
            'all_row_field_coverage': {x: {'populated': coverage[x], 'pct': round(100 * coverage[x] / total, 2) if total else 0}
                                       for x in REQUIRED}}


def coverage_report(csv_path:Path)->dict[str,Any]:
    counter=Counter();n=0
    tracked=['id','full_name','property_address','mailing_address','acres','property_city',
             'property_zip','mailing_city','mailing_state','mailing_zip','market_value',
             'appraised_value','assessed_value','taxable_value','land_value','total_value']
    with csv_path.open(encoding='utf-8-sig',newline='') as f:
        for row in csv.DictReader(f):
            n+=1
            for col in tracked:
                if col in ('acres','market_value'):
                    ok=positive(row.get(col)) is not None
                elif col=='property_city':ok=city_is_valid(row.get(col))
                elif 'zip' in col:ok=bool(clean_zip(row.get(col)))
                else:ok=bool(as_text(row.get(col)))
                if ok:counter[col]+=1
    return {'rows':n,'fields':{k:{'valid':counter[k], 'pct':round(100*counter[k]/n,2) if n else 0}
                            for k in tracked}}


def quality_recommendations(coverage:dict[str,Any],queue:dict[str,Any],is_preview:bool)->dict[str,Any]:
    vals={k:v['pct'] for k,v in coverage.get('fields',{}).items()}
    issues=[]
    thresholds={'full_name':90,'mailing_address':90,'acres':95,'property_address':50,
                'property_zip':50,'property_city':50}
    for field,minimum in thresholds.items():
        if vals.get(field,0)<minimum:
            issues.append({'severity':'HIGH' if field in {'full_name','mailing_address','acres'} else 'REVIEW',
                           'type':'low_field_coverage','field':field,
                           'valid_percent':vals.get(field,0),'minimum_percent':minimum,
                           'action':'Check raw aliases first, then official assessor source; never fabricate unavailable values'})
    if vals.get('market_value',0)<50:
        issues.append({'severity':'REVIEW','type':'low_positive_market_value',
            'valid_percent':vals.get('market_value',0),
            'action':'Check official explicit market-value fields; do not relabel assessed/total/taxable'})
    flags=queue.get('missing_flag_counts',{})
    if flags.get('geometry_acres_unverified'):
        issues.append({'severity':'REVIEW','type':'geometry_acre_estimates',
             'rows':flags['geometry_acres_unverified'],
             'action':'Use county legal/assessor acreage when available and retain geometry estimate for audit'})
    if flags.get('id_fallback'):
        issues.append({'severity':'HIGH','type':'non_authoritative_parcel_ids',
             'rows':flags['id_fallback'],'action':'Identify CAD parcel/account ID before joining owner data'})
    if is_preview:
        issues.append({'severity':'INFO','type':'sample_only','action':
            'A preview cannot establish whole-county coverage. Use --sample-mode random or run the full county.'})
    return {'issues':issues,'risk_count':len(issues)}


def missing_data_queue(input_csv:Path,output_csv:Path)->dict[str,Any]:
    cols=['state','county','id','parcel_id','account_id','full_name','acres','missing_fields',
          'priority','suggested_recovery','id_quality','data_quality_flags','source_url']
    totals=Counter()
    def records():
        with input_csv.open(encoding='utf-8-sig',newline='') as f:
            for row in csv.DictReader(f):
                flags=set(str(row.get('data_quality_flags') or '').split('|'))
                needs=[x for x in ('missing_owner','missing_mailing_address',
                    'missing_property_address','missing_property_city','missing_property_zip',
                    'missing_acres','missing_or_zero_market_value','id_fallback',
                    'geometry_acres_unverified') if x in flags]
                if not needs:continue
                totals.update(needs)
                if any(x in needs for x in ('missing_owner','missing_mailing_address','id_fallback')):
                    priority='HIGH';action='Official assessor owner/account lookup; verify join key'
                elif 'missing_acres' in needs or 'geometry_acres_unverified' in needs:
                    priority='MEDIUM';action='Obtain legal acreage from county assessor; geometry is an estimate'
                else:
                    priority='LOW';action='Check situs address and valuation via official county data; rural sites may not have assigned addresses'
                yield {**{k:row.get(k,'') for k in cols},'missing_fields':'|'.join(needs),
                       'priority':priority,'suggested_recovery':action}
    n=csv_output(output_csv,cols,records())
    return {'queued_records':n,'missing_flag_counts':dict(totals),'file':str(output_csv)}


def refresh_quality_file(input_csv:Path,output_csv:Path)->None:
    def rows():
        with input_csv.open(encoding='utf-8-sig',newline='') as f:
            for row in csv.DictReader(f):yield set_quality_flags(row)
    csv_output(output_csv,FIELDS,rows())


def read_assessor_rules(path:Path|None)->dict[tuple[str,str],dict[str,Any]]:
    if not path:return {}
    data=json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(data,dict):raise GISFailure('Assessor rules JSON must be state -> county -> config')
    rules={}
    for state,counties in data.items():
        if state.upper() not in BUILTIN or not isinstance(counties,dict):
            raise GISFailure('Invalid state or county list in assessor rules JSON')
        for county,config in counties.items():
            if not isinstance(config,dict) or config.get('type') not in {'bulk','arcgis'}:
                raise GISFailure(f'{state}:{county}: assessor rule must have type bulk or arcgis')
            if config['type']=='arcgis':
                if not str(config.get('url','')).startswith('https://'):
                    raise GISFailure('Secondary ArcGIS enrichment URL must be HTTPS')
                if not config.get('field_map'):
                    raise GISFailure('Custom ArcGIS assessor rule needs a field_map')
                field=config.get('parcel_field','UPC')
                if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',str(field)):
                    raise GISFailure('Invalid ArcGIS parcel key field in assessor rules')
            else:
                if not config.get('local_file') and not config.get('download_url'):
                    raise GISFailure('Bulk assessor rule requires local_file or download_url')
                for stage in config.get('stages',[]):
                    if stage.get('kind')!='custom_bulk' or not stage.get('field_map'):
                        raise GISFailure('Custom bulk stages require kind=custom_bulk and field_map')
            rules[(state.upper(),county_key(county))]=config
    return rules


def assessor_overrides(items:list[str])->dict[tuple[str,str],str]:
    data={}
    for item in items:
        if '=' not in item or ':' not in item.split('=',1)[0]:
            raise GISFailure('Use --assessor-file "CO:El Paso=/path/file.zip"')
        lhs,path=item.split('=',1);state,county=lhs.split(':',1)
        if not path or not county.strip():raise GISFailure('Assessor file override requires county and file')
        data[(state.upper(),county_key(county))]=path
    return data


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.tmp')
    temp.write_text(json.dumps(value, indent=2, ensure_ascii=False, default=str), encoding='utf-8')
    temp.replace(path)


def plan_hash(src: Source, oid_list: list[int], fields: list[str], mode: str) -> str:
    data = json.dumps({'mode': mode, 'source': src.__dict__, 'ids': oid_list,
                       'fields': fields}, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(data.encode('utf-8')).hexdigest()


def validate_plan(job_dir: Path, new_plan: dict[str, Any], force: bool) -> None:
    plan_path = job_dir / 'plan.json'
    if force and job_dir.exists():
        shutil.rmtree(job_dir)
    if plan_path.exists():
        old = json.loads(plan_path.read_text(encoding='utf-8'))
        if old.get('hash') != new_plan.get('hash'):
            raise GISFailure('Source/IDs/schema changed since previous run. '
                             'Refusing mixed snapshot. Use --force to restart this county.')
    elif job_dir.exists() and any(job_dir.iterdir()):
        raise GISFailure('Existing job chunks have no valid plan.json. Use --force to restart.')
    job_dir.mkdir(parents=True, exist_ok=True)
    write_json(plan_path, new_plan)


def renormalize_cached_chunks(job_dir:Path,chunks:list[list[int]],src:Source,oid_field:str)->None:
    """Recalculate v3.1 normalized data from *raw* CSV without redownloading.

    Preserve geometry acreage measured in a previous run; do not reuse the old
    normalization itself (it may contain incorrect field mappings).
    """
    for i,_ in enumerate(chunks,start=1):
        raw_path=job_dir/f'{i:06d}.raw.csv'
        normalized_path=job_dir/f'{i:06d}.normalized.csv'
        if not raw_path.exists():continue
        geom={}
        if normalized_path.exists():
            with normalized_path.open(encoding='utf-8-sig',newline='') as f:
                for row in csv.DictReader(f):
                    oid=clean_id(row.get('source_objectid'))
                    acres=positive(row.get('geometry_acres'))
                    if oid and acres:geom[oid]=acres
        def rows():
            with raw_path.open(encoding='utf-8-sig',newline='') as f:
                for row in csv.DictReader(f):
                    yield normalize(row,src,oid_field,geometry_acres=geom.get(clean_id(row.get(oid_field))))
        csv_output(normalized_path,FIELDS,rows())


def apply_enrichment_pipeline(input_csv:Path,src:Source,args:argparse.Namespace,county_dir:Path):
    from county_enrichment_v31 import enrich_county
    enriched,stats=enrich_county(input_csv,county_dir/'_unused_enrichment.csv',src,args,county_dir,
            {'Source':Source,'normalize':normalize,'as_text':as_text,'positive':positive,
             'number':number,'session':session,'api_json':api_json,'fieldnames':FIELDS},
            lambda message:log(f'{src.state}:{src.county}',message))
    final=county_dir/'_post_enrichment_quality.csv'
    refresh_quality_file(enriched,final)
    return final,stats


def run_arcgis(src: Source, args: argparse.Namespace, county_dir: Path) -> dict[str, Any]:
    label = f'{src.state}:{src.county}'
    start = time.perf_counter()
    s = session()
    md, oid_field, raw_fields = metadata(s, src, args.delay)
    source_cols = set(x.casefold() for x in raw_fields)
    if src.county_field and src.county_field.casefold() not in source_cols:
        raise GISFailure(f'County filter field {src.county_field!r} absent in source. '
                         'County selection cannot be verified; stopping.')
    ids = layer_ids(s, src, delay=args.delay)
    source_total = len(ids)
    if args.max_parcels:
        if args.sample_mode=='random' and len(ids)>args.max_parcels:
            ids=sorted(random.Random(args.seed).sample(ids,args.max_parcels))
        else:
            ids=ids[:args.max_parcels]
    if not ids:
        raise GISFailure(f'No parcel IDs matched the source filter ({where_for(src)}).')
    log(label, f'ArcGIS layer: {md.get("name")} | source IDs: {source_total:,} | '
               f'job IDs: {len(ids):,} | fields: {len(raw_fields)}')
    max_rec = max(1, int(md.get('maxRecordCount') or 1000))
    batch_size = min(args.batch_size, max_rec)
    chunks = [ids[i:i + batch_size] for i in range(0, len(ids), batch_size)]
    if args.dry_run:
        return {'state': src.state, 'county': src.county, 'status': 'dry_run',
                'source_total': source_total, 'selected_parcels': len(ids),
                'source_url': src.url, 'source_fields': raw_fields,
                'expected_batches': len(chunks), 'where': where_for(src),
                'warning': 'No parcels downloaded in dry-run mode.'}
    job_dir = county_dir / '_resume_batches'
    plan = {'hash': plan_hash(src, ids, raw_fields, args.geometry_acres), 'source_url': src.url,
            'where': where_for(src), 'source_total': source_total, 'selected_count': len(ids),
            'batch_size': batch_size, 'batches': len(chunks), 'object_id_field': oid_field,
            'created_at': timestamp(), 'geometry_mode': args.geometry_acres}
    validate_plan(job_dir, plan, args.force)
    issue_list: list[str] = []
    missed: list[int] = []
    t = time.perf_counter()
    for i, oid_batch in enumerate(chunks, start=1):
        raw_chunk = job_dir / f'{i:06d}.raw.csv'
        norm_chunk = job_dir / f'{i:06d}.normalized.csv'
        missing_chunk = job_dir / f'{i:06d}.missing.json'
        if raw_chunk.exists() and norm_chunk.exists() and missing_chunk.exists():
            old_missing = json.loads(missing_chunk.read_text(encoding='utf-8'))
            if not old_missing:
                if i == 1 or i == len(chunks) or i % 20 == 0:
                    log(label, f'[RESUME] Batch {i:,}/{len(chunks):,} already completed')
                continue
            log(label, f'[RESUME] Retrying incomplete batch {i:,}: '
                       f'{len(old_missing):,} object IDs were missing last time')
        features, missing = fetch_features(s, src, oid_batch, oid_field, geometry=False,
                                           delay=args.delay)
        if missing:
            issue_list.append(f'Batch {i}: {len(missing)} IDs not returned, even on retry.')
        geom_areas, geom_warnings = geometry_fallback(s, src, features, oid_field,
                                                      args.geometry_acres, args.delay)
        issue_list.extend(geom_warnings)
        rows_raw = [f['attributes'] for f in features]
        rows_norm = [normalize(a, src, oid_field, geometry_acres=geom_areas.get(int(a[oid_field])))
                     for a in rows_raw]
        csv_output(raw_chunk, raw_fields, rows_raw)
        csv_output(norm_chunk, FIELDS, rows_norm)
        write_json(missing_chunk, missing)
        done = i * batch_size if i != len(chunks) else len(ids)
        elapsed = max(time.perf_counter() - t, 0.001)
        rate = done / elapsed
        eta = max(0, len(ids) - done) / rate if rate > 0 else None
        log(label, f'[BATCH {i:,}/{len(chunks):,}] {done:,}/{len(ids):,} parcel IDs '
                   f'| got {len(rows_norm):,} | {rate:,.1f} IDs/s '
                   f'| remaining {len(ids)-done:,} | ETA {dur(eta)}')
    # Need every chunk to avoid declaring incomplete output successful.
    raw_parts, norm_parts = [], []
    for i in range(1, len(chunks) + 1):
        rp, np = job_dir / f'{i:06d}.raw.csv', job_dir / f'{i:06d}.normalized.csv'
        mp = job_dir / f'{i:06d}.missing.json'
        if not rp.exists() or not np.exists() or not mp.exists():
            raise GISFailure(f'Chunk {i} incomplete; rerun without --force to resume')
        raw_parts.append(rp)
        norm_parts.append(np)
        missed.extend(json.loads(mp.read_text(encoding='utf-8')))
    output = county_dir / 'output'
    output.mkdir(parents=True, exist_ok=True)
    prefix = slug(src.county)
    raw_csv = output / f'{prefix}_raw_all_fields.csv'
    before_repair = output / f'{prefix}_normalized_pre_repair.csv'
    after_repair = output / f'{prefix}_all_normalized.csv'
    final_csv = output / f'{prefix}_production.csv'
    owner_audit = output / f'{prefix}_owner_name_audit.csv'
    combine_chunks(raw_parts, raw_csv, raw_fields)
    # IMPORTANT: cached v3.0 normalized chunks are not reliable after mapping
    # changes. Recreate from source raw rows and preserve prior geometry estimates.
    renormalize_cached_chunks(job_dir,chunks,src,oid_field)
    processed = combine_chunks(norm_parts, before_repair, FIELDS)
    initial_coverage=coverage_report(before_repair)
    enriched_source,enrichment_report=apply_enrichment_pipeline(before_repair,src,args,county_dir)
    names_stats = fix_names(enriched_source, after_repair, owner_audit, args.owner_name_repair,
                            args.owner_name_compression_threshold)
    audit = filter_and_audit(after_repair, final_csv, args.min_acres, args.keep_missing_acres)
    updated_coverage=coverage_report(after_repair)
    queue=missing_data_queue(after_repair,output/f'{prefix}_missing_data_queue.csv')
    recommendations=quality_recommendations(updated_coverage,queue,bool(args.max_parcels))
    missing_file = output / f'{prefix}_missing_arcgis_ids.csv'
    csv_output(missing_file, ['source_objectid'], ({'source_objectid': x} for x in missed))
    warnings = list(dict.fromkeys(issue_list))
    warnings.extend(enrichment_report.get('errors',[]))
    if missed:
        warnings.append(f'{len(missed):,} object IDs were missing from downloaded records. '
                        'Check missing_arcgis_ids.csv and rerun on a stable snapshot.')
    if args.max_parcels and len(ids) < source_total:
        warnings.append(f'PREVIEW ONLY: --max-parcels capped {source_total:,} source IDs to {len(ids):,}.')
    if any(audit['all_row_field_coverage'][f]['pct'] < 40 for f in ['full_name', 'mailing_address']):
        warnings.append('Source has poor owner or mailing address coverage; assessor enrichment may be needed.')
    report = {'version': VERSION, 'state': src.state, 'county': src.county, 'status':
              'partial_missing_ids' if missed else 'completed',
              'finished_at_utc': timestamp(), 'source_url': src.url, 'publisher': src.publisher,
              'where': where_for(src), 'layer_name': md.get('name'),
              'data_source_record_count': source_total, 'planned_object_ids': len(ids),
              'downloaded_rows': processed, 'missing_object_ids': len(missed),
              'raw_fields': raw_fields, 'field_coverage': audit, 'owner_name_report': names_stats,
              'coverage_before_assessor':initial_coverage,'coverage_after_assessor':updated_coverage,
              'assessor_enrichment':enrichment_report,'missing_data_queue':queue,
              'quality_recommendations':recommendations,
              'minimum_acres': args.min_acres, 'geometry_acres_mode': args.geometry_acres,
              'is_preview': bool(args.max_parcels and len(ids) < source_total),
              'output_files': {'raw_csv': str(raw_csv), 'all_normalized_csv': str(after_repair),
                               'production_csv': str(final_csv), 'owner_audit': names_stats['audit_file'],
                               'missing_object_ids': str(missing_file),
                               'missing_data_queue':queue['file']},
              'warnings': warnings, 'elapsed_seconds': round(time.perf_counter() - start, 2),
              'value_note': 'market_value is only explicit market value, or WY actualvalu flagged as assessor actual; '
                            'appraised/assessed/taxable/total are independent fields.',
              'legal_address_note': 'legal_address denotes physical/situs address, NOT deed legal description.',
              'terms_note': 'Respect publisher terms. CO OIT attribution is required; resale of dataset prohibited.' if src.state == 'CO' else ''}
    write_json(output / f'{prefix}_report.json', report)
    log(label, f'COMPLETE: {processed:,} of {len(ids):,} selected IDs | '
               f'{audit["exported_rows"]:,} production rows | {dur(time.perf_counter() - start)}')
    return report


def safe_unzip(path: Path, out: Path) -> Path:
    marker = out / '.extract.complete'
    if marker.exists() and marker.read_text(encoding='utf-8') == f'{path.stat().st_size}:{path.stat().st_mtime_ns}':
        return out
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    root = out.resolve()
    with zipfile.ZipFile(path) as z:
        for member in z.infolist():
            target = (out / member.filename).resolve()
            try:
                target.relative_to(root)
            except ValueError as exc:
                raise GISFailure(f'Unsafe archive path {member.filename}') from exc
        z.extractall(out)
    marker.write_text(f'{path.stat().st_size}:{path.stat().st_mtime_ns}', encoding='utf-8')
    return out


def vector_path(path: Path, county_dir: Path) -> Path:
    if path.suffix.lower() == '.zip':
        folder = safe_unzip(path, county_dir / '_local_extracted')
        gdbs = [p for p in folder.rglob('*.gdb') if p.is_dir()]
        if gdbs:
            if len(gdbs) == 1:
                return gdbs[0]
            likely = [p for p in gdbs if 'parcel' in p.name.casefold()]
            if len(likely) == 1:
                return likely[0]
            raise GISFailure('ZIP includes multiple geodatabases. Extract and specify the correct .gdb directly.')
        for pat in ('*.gpkg', '*.shp'):
            files = list(folder.rglob(pat))
            if len(files) == 1:
                return files[0]
            if files:
                likely = [p for p in files if 'parcel' in p.stem.casefold()]
                if len(likely) == 1:
                    return likely[0]
                raise GISFailure('ZIP includes multiple vector layers. Extract and pass the parcel file directly.')
        raise GISFailure('ZIP has no shapefile, GeoPackage, or file geodatabase.')
    return path


def run_local(src: Source, args: argparse.Namespace, county_dir: Path) -> dict[str, Any]:
    """Process a user-provided county CSV or vector dataset without web calls."""
    label = f'{src.state}:{src.county}'
    t0 = time.perf_counter()
    p = Path(src.local_file or '')
    if not p.exists():
        raise GISFailure(f'Local input not found: {p}')
    if p.is_dir() and p.suffix.lower() != '.gdb':
        raise GISFailure('Supply a dataset file/ZIP or .gdb directory, not an arbitrary folder.')
    input_path = vector_path(p, county_dir)
    csv_mode = input_path.suffix.lower() == '.csv'
    if csv_mode:
        with input_path.open('r', encoding='utf-8-sig', newline='') as fin:
            first = csv.DictReader(fin)
            field_names = first.fieldnames or []
        total = None
        vector_info = None
    else:
        try:
            import pyogrio
        except ImportError as exc:
            raise GISFailure('Local SHP/GDB/GPKG needs pyogrio: pip install pyogrio geopandas') from exc
        layers = pyogrio.list_layers(input_path)
        polygon_layers = [str(a[0]) for a in layers if 'polygon' in str(a[1]).lower()]
        if not polygon_layers:
            raise GISFailure('No polygon layer in supplied vector dataset.')
        if len(polygon_layers) > 1:
            likely = [x for x in polygon_layers if 'parcel' in x.casefold() or 'taxpar' in x.casefold()]
            if len(likely) != 1:
                raise GISFailure(f'Multiple polygon layers ({polygon_layers[:12]}). '
                                 'Supply a separate parcel-only dataset or GeoPackage.')
            layer = likely[0]
        else:
            layer = polygon_layers[0]
        vector_info = pyogrio.read_info(input_path, layer=layer)
        field_names = [str(f) for f in vector_info['fields']]
        total = int(vector_info['features'])
    if not field_names:
        raise GISFailure('Dataset has no attribute fields.')
    oid_field = next((f for f in field_names if f.casefold() in ('objectid', 'fid', 'oid')), '_local_row')
    if args.dry_run:
        return {'state': src.state, 'county': src.county, 'status': 'dry_run',
                'source_total': total, 'source_url': str(p), 'source_fields': field_names}
    out = county_dir / 'output'
    out.mkdir(parents=True, exist_ok=True)
    prefix = slug(src.county)
    raw_csv = out / f'{prefix}_raw_all_fields.csv'
    pre_csv = out / f'{prefix}_normalized_pre_repair.csv'
    all_csv = out / f'{prefix}_all_normalized.csv'
    final_csv = out / f'{prefix}_production.csv'
    audit_csv = out / f'{prefix}_owner_name_audit.csv'
    raw_tmp = raw_csv.with_suffix('.csv.tmp')
    norm_tmp = pre_csv.with_suffix('.csv.tmp')
    done = 0
    with raw_tmp.open('w', encoding='utf-8-sig', newline='') as rf, norm_tmp.open('w', encoding='utf-8-sig', newline='') as nf:
        rw = csv.DictWriter(rf, fieldnames=field_names, extrasaction='ignore')
        nw = csv.DictWriter(nf, fieldnames=FIELDS, extrasaction='ignore')
        rw.writeheader()
        nw.writeheader()
        if csv_mode:
            def records():
                with input_path.open('r', encoding='utf-8-sig', newline='') as inf:
                    yield from csv.DictReader(inf)
            reader = records()
            for attrs in reader:
                if args.max_parcels and done >= args.max_parcels:
                    break
                if oid_field == '_local_row':
                    attrs['_local_row'] = done + 1
                rw.writerow(attrs)
                nw.writerow(normalize(attrs, src, oid_field))
                done += 1
                if done % args.chunk_size == 0:
                    log(label, f'[LOCAL] {done:,} rows processed')
        else:
            import pyogrio
            import geopandas as gpd
            limit = min(total, args.max_parcels) if args.max_parcels else total
            for offset in range(0, limit, args.chunk_size):
                frame = pyogrio.read_dataframe(input_path, layer=layer, skip_features=offset,
                                               max_features=min(args.chunk_size, limit-offset),
                                               read_geometry=args.geometry_acres != 'never')
                geom_acres = {}
                if args.geometry_acres != 'never' and frame.geometry is not None and frame.crs is not None:
                    # Local conversion, geometry only where source acreage missing unless always.
                    outgeom = frame.to_crs('EPSG:5070')
                    areas = outgeom.geometry.area / SQM_PER_ACRE
                    geom_acres = {idx: a for idx, a in enumerate(areas) if math.isfinite(float(a)) and a > 0}
                for idx, (_, row) in enumerate(frame.iterrows()):
                    attrs = {c: row[c] for c in field_names}
                    if oid_field == '_local_row':
                        attrs['_local_row'] = done + 1
                    a, _ = acres_from_attributes(attrs, src.field_map)
                    geom_value = geom_acres.get(idx) if args.geometry_acres == 'always' or a is None else None
                    rw.writerow(attrs)
                    nw.writerow(normalize(attrs, src, oid_field, geometry_acres=geom_value))
                    done += 1
                log(label, f'[LOCAL] {done:,}/{limit:,} rows processed')
    raw_tmp.replace(raw_csv)
    norm_tmp.replace(pre_csv)
    initial_coverage=coverage_report(pre_csv)
    enriched_source,enrichment_report=apply_enrichment_pipeline(pre_csv,src,args,county_dir)
    names = fix_names(enriched_source, all_csv, audit_csv, args.owner_name_repair,
                      args.owner_name_compression_threshold)
    audited = filter_and_audit(all_csv, final_csv, args.min_acres, args.keep_missing_acres)
    updated_coverage=coverage_report(all_csv)
    queue=missing_data_queue(all_csv,out/f'{prefix}_missing_data_queue.csv')
    recommendations=quality_recommendations(updated_coverage,queue,bool(args.max_parcels))
    report = {'version': VERSION, 'state': src.state, 'county': src.county, 'status': 'completed',
              'source': str(p), 'publisher': src.publisher, 'downloaded_rows': done,
              'is_preview': bool(args.max_parcels), 'field_coverage': audited,
              'coverage_before_assessor':initial_coverage,'coverage_after_assessor':updated_coverage,
              'assessor_enrichment':enrichment_report,'missing_data_queue':queue,
              'quality_recommendations':recommendations,
              'owner_name_report': names, 'raw_fields': field_names,
              'output_files': {'raw_csv': str(raw_csv), 'all_normalized_csv': str(all_csv),
                               'production_csv': str(final_csv),'missing_data_queue':queue['file']},
              'warnings': ['Local file assumed to contain only requested county; verify provenance.']+enrichment_report.get('errors',[]),
              'elapsed_seconds': round(time.perf_counter() - t0, 2)}
    write_json(out / f'{prefix}_report.json', report)
    log(label, f'COMPLETE local: {done:,} source / {audited["exported_rows"]:,} production')
    return report


def read_registry(path: Path | None) -> dict[str, dict[str, dict[str, Any]]]:
    reg: dict[str, dict[str, dict[str, Any]]] = {
        state: {k: dict(v) for k, v in counties.items()} for state, counties in BUILTIN.items()
    }
    if not path:
        return reg
    data = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(data, dict):
        raise GISFailure('sources JSON must be an object of state -> county -> metadata')
    for state, counties in data.items():
        state = state.upper()
        if state not in BUILTIN or not isinstance(counties, dict):
            raise GISFailure(f'Invalid state or county registry in {state}: WY/CO/OK/NM only')
        for county, info in counties.items():
            if not isinstance(info, dict):
                raise GISFailure('Each county registry entry must be an object')
            reg[state][county_key(county)] = info
    return reg


def lookup_source(registry: dict[str, Any], state: str, county: str) -> Source:
    state = state.upper()
    k = county_key(county)
    info = registry[state].get(k) or registry[state].get('*')
    if not info:
        configured = ', '.join(sorted(n.title() for n in registry[state] if n != '*'))
        raise GISFailure(f'No vetted source for {state}:{county}. Available built-ins: {configured}. '
                         'Add a county-specific public ArcGIS source to --sources-json, or use --local-file.')
    if not info.get('url') and not info.get('local_file'):
        raise GISFailure(f'Source for {state}:{county} lacks url or local_file')
    fm = info.get('field_map') or {}
    if not isinstance(fm, dict):
        raise GISFailure('field_map must be a dictionary of output field to source aliases')
    fmaps = {key: ([value] if isinstance(value, str) else list(value)) for key, value in fm.items()}
    return Source(state=state, county=display_county(county), url=info.get('url'),
                  local_file=info.get('local_file'), county_field=info.get('county_field'),
                  county_format=info.get('county_format', 'title'), where=info.get('where', '1=1'),
                  publisher=info.get('publisher', 'Source provided by user'), field_map=fmaps)


def discover_counties(s: requests.Session, src: Source, delay: float) -> list[str]:
    if not src.county_field:
        raise GISFailure('Cannot discover statewide counties without county_field')
    data = api_json(s, src.url + '/query', {'where': src.where, 'returnDistinctValues': 'true',
                     'returnGeometry': 'false', 'outFields': src.county_field,
                     'orderByFields': src.county_field}, delay=delay)
    feats = data.get('features')
    if not isinstance(feats, list):
        raise GISFailure('Distinct county query returned unexpected format')
    vals = []
    for item in feats:
        val = as_text(item.get('attributes', {}).get(src.county_field))
        if val:
            vals.append(val.title() if src.state == 'WY' else val)
    vals = sorted(set(vals))
    if data.get('exceededTransferLimit'):
        raise GISFailure('County distinct query truncated; specify named counties instead')
    return vals


def make_targets(args: argparse.Namespace, registry: dict[str, Any]) -> list[Source]:
    requested: list[tuple[str, str]] = []
    if args.target:
        for t in args.target:
            if ':' not in t:
                raise GISFailure(f'Use --target STATE:County, not {t}')
            st, co = t.split(':', 1)
            requested.append((st.upper(), co))
    if args.state:
        if not args.counties:
            raise GISFailure('With --state, specify --counties (or --counties ALL)')
        if len(args.counties) == 1 and args.counties[0].upper() == 'ALL':
            if args.state in ('WY', 'CO'):
                prototype = lookup_source(registry, args.state, 'Example')
                log(args.state, 'Discovering all counties *represented in the public GIS layer*. '
                                'This is not proof of all-state coverage.')
                counties = discover_counties(session(), prototype, args.delay)
            else:
                counties = [c.title() for c in registry[args.state] if c != '*']
                log(args.state, f'ALL means {len(counties)} BUILT-IN county sources only; '
                                'NOT every county in this state.')
            requested.extend((args.state, c) for c in counties)
        else:
            requested.extend((args.state, county) for county in args.counties)
    if not requested:
        raise GISFailure('Supply --state WY --counties Natrona or --target WY:Natrona')
    sources = []
    seen = set()
    for state, county in requested:
        if state not in BUILTIN:
            raise GISFailure(f'Unsupported state {state}; use WY, CO, OK or NM.')
        if (state, county_key(county)) in seen:
            continue
        seen.add((state, county_key(county)))
        # Explicit single-county override does not require a pre-registered county.
        if len(requested) == 1 and (args.local_file or args.service_url):
            try:
                src = lookup_source(registry, state, county)
            except GISFailure:
                src = Source(state=state, county=display_county(county))
        else:
            src = lookup_source(registry, state, county)
        if args.local_file:
            if len(requested) != 1:
                raise GISFailure('--local-file only works with one selected county; for many, use sources JSON')
            src.local_file = str(args.local_file)
            src.url = None
            src.publisher = 'Local user-supplied county dataset'
        if args.service_url:
            if len(requested) != 1:
                raise GISFailure('--service-url only works with one county; for many, use sources JSON')
            src.url = args.service_url
            src.local_file = None
            src.publisher = 'User-specified ArcGIS layer (verify publisher terms)'
            src.county_field = None  # explicit single-county URL, not statewide unless mapped via JSON
        if args.where:
            if len(requested) != 1:
                raise GISFailure('--where applies to one county only; configure per-county where in sources JSON')
            src.where = args.where
        sources.append(src)
    return sources


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('--state', choices=['WY', 'OK', 'CO', 'NM'], help='State abbreviation for --counties')
    p.add_argument('--counties', nargs='+', default=[], help='County names, or ALL for counties covered by selected source')
    p.add_argument('--target', action='append', help='Can repeat: --target WY:Natrona --target OK:Tulsa')
    p.add_argument('--list-sources', action='store_true', help='List built-in state and county services; do not download')
    p.add_argument('--sources-json', type=Path, help='Extra county endpoints / local files and custom field maps')
    p.add_argument('--local-file', type=Path, help='Single-county CSV/SHP/GDB/GPKG/ZIP source; offline supported')
    p.add_argument('--service-url', help='Single-county ArcGIS FeatureServer layer URL (not a web map URL)')
    p.add_argument('--where', help='Optional extra SQL expression; single-source only')
    p.add_argument('--output', type=Path, default=Path('multi_state_parcels_output_v3'))
    p.add_argument('--batch-size', type=int, default=200, help='ArcGIS object IDs per request; server cap also observed')
    p.add_argument('--chunk-size', type=int, default=2500, help='Rows per local vector/CSV progress chunk')
    p.add_argument('--workers', type=int, default=2, help='Counties downloaded in parallel')
    p.add_argument('--delay', type=float, default=0.2, help='Seconds between ArcGIS requests, polite throttling')
    p.add_argument('--max-parcels', type=int, default=0, help='Preview: first N IDs or seeded random sample')
    p.add_argument('--sample-mode', choices=['first','random'],default='first',
                   help='When max-parcels is set, first N IDs or reproducible random sample')
    p.add_argument('--seed',type=int,default=42,help='Random sample seed for repeatability')
    p.add_argument('--assessor-enrich', choices=['off','auto','require'],default='auto',
                   help='County assessor supplements for El Paso/Weld/Bernalillo; failures warned in auto')
    p.add_argument('--assessor-rules-json',type=Path,help='Optional future-county assessor supplement rules (bulk or ArcGIS)')
    p.add_argument('--assessor-file',action='append',default=[],metavar='STATE:COUNTY=FILE',
                   help='Use an official local assessor CSV/TXT/DBF/ZIP for CO:El Paso or CO:Weld; repeatable')
    p.add_argument('--min-acres', type=float, default=0.0, help='Filter PRODUCTION CSV (raw CSV always includes all)')
    p.add_argument('--keep-missing-acres', action='store_true', help='Keep unknown-acreage parcels even when min acres > 0')
    p.add_argument('--geometry-acres', choices=['auto', 'always', 'never'], default='auto',
                   help='Calculate equal-area polygon acres when source fields are missing')
    p.add_argument('--owner-name-repair', choices=['auto', 'always', 'never'], default='auto',
                   help='Conservative compressed-name detection and repair from TxGIO v2.3')
    p.add_argument('--owner-name-compression-threshold', type=float, default=95,
                   help='Percent compact owner names needed for auto repair')
    p.add_argument('--dry-run', action='store_true', help='Inspect metadata and counts only')
    p.add_argument('--force', action='store_true', help='Restart downloaded ArcGIS chunks for counties')
    return p


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.list_sources:
        print('VERIFIED/IDENTIFIED BUILT-INS (coverage is not universal in OK/NM):')
        for st, counties in BUILTIN.items():
            for name, data in counties.items():
                print(f'  {st} {name:14s} {data["url"]}')
        print('Extra counties: --sources-json registry.json or --local-file data.zip')
        return 0
    for v in ('batch_size', 'chunk_size', 'workers'):
        if getattr(args, v) <= 0:
            raise SystemExit(f'--{v.replace("_", "-")} must be > 0')
    if args.max_parcels < 0 or args.min_acres < 0 or args.delay < 0:
        raise SystemExit('--max-parcels, --min-acres and --delay must be >= 0')
    if not 0 <= args.owner_name_compression_threshold <= 100:
        raise SystemExit('Owner-name compression threshold must be 0..100')
    args.assessor_files=assessor_overrides(args.assessor_file)
    args.assessor_rules=read_assessor_rules(args.assessor_rules_json)
    registry = read_registry(args.sources_json)
    sources = make_targets(args, registry)
    args.output.mkdir(parents=True, exist_ok=True)
    print('=' * 84)
    print(f'MULTI-STATE PARCEL DOWNLOADER v{VERSION}')
    print(f'Targets: {", ".join(f"{x.state}:{x.county}" for x in sources)}')
    print(f'Output: {args.output.absolute()}')
    print(f'Workers: {min(args.workers, len(sources))} | batch IDs: {args.batch_size} | '
          f'geometry acres: {args.geometry_acres} | minimum acres: {args.min_acres}')
    print('Source coverage: WY/CO statewide layers (may omit counties); OK/NM only mapped counties')
    print('=' * 84)

    def run(src: Source) -> dict[str, Any]:
        county_dir = args.output / src.state / slug(src.county)
        if args.max_parcels:
            county_dir = county_dir / (f'preview_random_{args.max_parcels}_seed_{args.seed}' if args.sample_mode=='random' else f'preview_first_{args.max_parcels}')
        try:
            if src.local_file:
                return run_local(src, args, county_dir)
            return run_arcgis(src, args, county_dir)
        except Exception as exc:
            log(f'{src.state}:{src.county}', f'ERROR: {type(exc).__name__}: {exc}')
            if not args.dry_run:
                error_dir = county_dir / 'output'
                error_dir.mkdir(parents=True, exist_ok=True)
                (error_dir / 'error_traceback.txt').write_text(traceback.format_exc(), encoding='utf-8')
            return {'state': src.state, 'county': src.county, 'status': 'failed',
                    'error': f'{type(exc).__name__}: {exc}', 'source_url': src.url or src.local_file}

    if len(sources) == 1:
        results = [run(sources[0])]
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(args.workers, len(sources))) as pool:
            futures = [pool.submit(run, src) for src in sources]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
    results.sort(key=lambda x: (x['state'], x['county']))
    summary_path = args.output / 'multi_state_summary.json'
    write_json(summary_path, {'version': VERSION, 'created_utc': timestamp(), 'results': results})
    summary_csv = args.output / 'multi_state_summary.csv'
    summary_rows = []
    for result in results:
        audit = result.get('field_coverage', {})
        row = {k: result.get(k, '') for k in ['state', 'county', 'status', 'source_url', 'downloaded_rows', 'error']}
        row['production_rows'] = audit.get('exported_rows', '')
        row['missing_object_ids'] = result.get('missing_object_ids', '')
        for f in REQUIRED:
            row[f + '_coverage_pct'] = audit.get('all_row_field_coverage', {}).get(f, {}).get('pct', '')
        summary_rows.append(row)
    csv_output(summary_csv, list(summary_rows[0]) if summary_rows else ['state', 'county', 'status'], summary_rows)
    print('\n' + '=' * 84)
    for result in results:
        print(f'{result["state"]}:{result["county"]}: {result["status"]}'
              + (f' ({result["error"]})' if result.get('error') else ''))
    print(f'Summary: {summary_path} | {summary_csv}')
    failures = sum(r['status'] in ('failed', 'partial_missing_ids') for r in results)
    return 1 if failures else 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (GISFailure, FileNotFoundError, ValueError) as exc:
        print(f'FATAL: {exc}', file=sys.stderr)
        raise SystemExit(2)
