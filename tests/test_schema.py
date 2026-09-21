"""The 45-column schema contract. The assets column is a verbatim JSON
string of the upstream assets object; the live test proves its hrefs point
at real objects."""
import json
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
from s2_schema import COLUMNS, USER_AGENT


def test_schema_shape():
    assert len(COLUMNS) == 45
    assert COLUMNS[-1][0] == "geometry"
    assert ("_hilbert", "UINTEGER") == COLUMNS[-2][:2]
    assert ("_month", "TINYINT") == COLUMNS[-3][:2]
    assert COLUMNS[-4][0] == "assets"
    assert USER_AGENT.startswith("sentinel-2-catalog-tools/")


def test_live_assets_resolve():
    """A live Earth Search item's assets, serialized the way s2_fetch will
    store them, must parse back and point at real objects."""
    body = json.dumps({"collections": ["sentinel-2-l2a"], "limit": 1}).encode()
    req = urllib.request.Request(
        "https://earth-search.aws.element84.com/v1/search", data=body,
        headers={"Content-Type": "application/json"})
    f = json.load(urllib.request.urlopen(req, timeout=60))["features"][0]
    a = json.loads(json.dumps(f["assets"], separators=(",", ":")))
    assert len(a) >= 30                       # full object, nothing stripped
    assert a["red"]["eo:bands"][0]["name"] == "B04"
    for key in ("red", "visual", "thumbnail"):
        r = urllib.request.Request(a[key]["href"], method="HEAD")
        assert urllib.request.urlopen(r, timeout=30).status == 200
