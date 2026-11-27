"""The report templates are written to be read by everyone.

Each template renders to a tagged PDF/UA-1 document, which Typst refuses to
produce without a document title and alt text on every image, so a template
that renders is accessible in those respects. What the tests here hold,
without R or Quarto, is that every template asks for that standard and a
language, sets its title, and that the colours the design script offers for
series meet the contrast WCAG 2.2 asks of a graphic.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

PACKAGE = Path(__file__).resolve().parents[1] / "src" / "fincommons"
TEMPLATES = sorted(PACKAGE.rglob("templates/*.qmd"))
DESIGN = PACKAGE / "templates" / "r-corporate-design-functions-ggplot2.R"


def front_matter(path):
    text = path.read_text(encoding="utf-8")
    return text.split("---", 2)[1]


def test_there_are_templates():
    assert len(TEMPLATES) >= 15


@pytest.mark.parametrize("path", TEMPLATES, ids=lambda p: p.name)
def test_a_template_asks_for_pdf_ua_and_a_language(path):
    head = front_matter(path)
    assert re.search(r"^\s*pdf-standard:\s*ua-1\s*$", head, re.M), path.name
    assert re.search(r"^lang:\s*\S+", head, re.M), path.name


@pytest.mark.parametrize("path", TEMPLATES, ids=lambda p: p.name)
def test_a_template_sets_the_title_of_its_document(path):
    assert "typst_document_title(" in path.read_text(encoding="utf-8"), path.name


def luminance(hex_colour):
    channels = [int(hex_colour[i:i + 2], 16) / 255 for i in (1, 3, 5)]
    linear = [c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4 for c in channels]
    return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]


def contrast(a, b):
    la, lb = sorted((luminance(a), luminance(b)), reverse=True)
    return (la + 0.05) / (lb + 0.05)


def r_vector(name):
    found = re.search(rf"^{name} <- c\((.*?)\)", DESIGN.read_text(encoding="utf-8"), re.M | re.S)
    return re.findall(r'"([^"]+)"', found.group(1))


def test_the_series_colours_stand_out_from_the_page():
    palette = r_vector("palette_accessible")

    assert len(palette) == 6
    assert all(contrast(c, "#FFFFFF") >= 3 for c in palette), \
        {c: round(contrast(c, "#FFFFFF"), 2) for c in palette}


def test_the_parts_of_a_stack_stand_out_from_the_page():
    palette = r_vector("palette_accessible_stack")

    assert len(palette) == len(set(palette)) == 10
    assert all(contrast(c, "#FFFFFF") >= 3 for c in palette)


def test_no_two_series_share_a_line_type():
    linetypes = r_vector("linetypes_accessible")

    assert len(linetypes) == len(set(linetypes)) == 6


def test_the_contrast_is_computed_as_wcag_defines_it():
    assert contrast("#000000", "#FFFFFF") == pytest.approx(21)
    assert contrast("#767676", "#FFFFFF") == pytest.approx(4.54, abs=0.01)
