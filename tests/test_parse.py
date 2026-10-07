from mfpipeline.parse import parse_amfi_text
from tests.conftest import FIXTURES


def rows():
    text = (FIXTURES / "amfi_nav_20260901_20260902.txt").read_text(encoding="utf-8")
    return list(parse_amfi_text(text))


def test_rows_inherit_category_and_fund_house_from_header_lines():
    first = rows()[0]
    assert first.scheme_code == "120465"
    assert first.scheme_type == "Open Ended Schemes"
    assert first.category == "Equity Scheme - Large Cap Fund"
    assert first.fund_house == "Axis Mutual Fund"  # double space collapsed


def test_new_category_header_resets_fund_house_context():
    children = [r for r in rows() if r.scheme_code == "135762"]
    assert children[0].category == "Children’s Fund - Childrens' Fund"
    assert children[0].fund_house == "Axis Mutual Fund"


def test_values_are_kept_as_raw_strings_for_spark_to_validate():
    na_row = next(r for r in rows() if r.scheme_code == "120466")
    assert na_row.nav == "N.A."
    assert na_row.isin_reinvest == "INF846K01DR4"
    assert na_row.parse_error is None


def test_dash_isin_becomes_none():
    child = next(r for r in rows() if r.scheme_code == "135762")
    assert child.isin_reinvest is None


def test_wrong_field_count_is_reported_not_raised():
    broken = next(r for r in rows() if r.scheme_code == "119019")
    assert broken.parse_error == "expected 8 fields, got 7"
    assert broken.line_no == 14


def test_header_and_blank_lines_produce_no_rows():
    assert len(rows()) == 10
    assert list(parse_amfi_text("Scheme Code;NAV Name\r\n\r\n")) == []
