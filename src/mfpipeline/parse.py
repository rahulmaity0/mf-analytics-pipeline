"""Parser for AMFI's NAV history text format.

The file is semicolon-separated, but category and fund-house names are not
columns: they appear as standalone header lines, and every data row belongs
to the most recent header above it::

    Scheme Code;NAV Name;Plan;Option;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Date

    Open Ended Schemes ( Income )

    Axis Mutual Fund
    135762;Axis Children's Fund - Direct Plan - Growth;;;INF846K01WO1;;29.2304;07-Oct-2026

Because a row's meaning depends on lines above it, a file cannot be split
at arbitrary line boundaries. The Spark job therefore parallelises by file,
and this parser walks each file sequentially. Values are returned as raw
strings: typing and validation happen in Spark so that bad rows can be
quarantined with a reason instead of crashing the parse.
"""
import re
from collections.abc import Iterator
from typing import NamedTuple

HEADER_PREFIX = "Scheme Code;"
EXPECTED_FIELDS = 8
# "Open Ended Schemes ( Income )", "Interval Fund Schemes ( Income )",
# "Open Ended Schemes(Children's Fund - Childrens' Fund)"
_CATEGORY_LINE = re.compile(r"^(?P<scheme_type>.*?Schemes?)\s*\(\s*(?P<category>.*?)\s*\)$")
_WHITESPACE = re.compile(r"\s+")


class RawNavRow(NamedTuple):
    scheme_code: str
    scheme_name: str
    isin_growth: str | None
    isin_reinvest: str | None
    nav: str
    nav_date: str
    scheme_type: str | None
    category: str | None
    fund_house: str | None
    line_no: int
    parse_error: str | None


def _blank_to_none(value: str) -> str | None:
    value = value.strip()
    return value if value and value != "-" else None


def parse_amfi_text(text: str) -> Iterator[RawNavRow]:
    scheme_type = category = fund_house = None
    for line_no, line in enumerate(text.splitlines(), start=1):
        line = line.strip()
        if not line or line.startswith(HEADER_PREFIX):
            continue

        if ";" not in line:
            match = _CATEGORY_LINE.match(line)
            if match:
                scheme_type = match["scheme_type"].strip()
                category = match["category"].strip()
                fund_house = None
            else:
                fund_house = _WHITESPACE.sub(" ", line)
            continue

        fields = line.split(";")
        if len(fields) != EXPECTED_FIELDS:
            yield RawNavRow(
                scheme_code=fields[0].strip(), scheme_name=line, isin_growth=None,
                isin_reinvest=None, nav="", nav_date="", scheme_type=scheme_type,
                category=category, fund_house=fund_house, line_no=line_no,
                parse_error=f"expected {EXPECTED_FIELDS} fields, got {len(fields)}",
            )
            continue

        code, name, _plan, _option, isin_growth, isin_reinvest, nav, nav_date = fields
        yield RawNavRow(
            scheme_code=code.strip(),
            scheme_name=_WHITESPACE.sub(" ", name.strip()),
            isin_growth=_blank_to_none(isin_growth),
            isin_reinvest=_blank_to_none(isin_reinvest),
            nav=nav.strip(),
            nav_date=nav_date.strip(),
            scheme_type=scheme_type,
            category=category,
            fund_house=fund_house,
            line_no=line_no,
            parse_error=None,
        )
