"""
Snapshot inspector: compares Iceberg table states and generates GeoJSON
showing what changed between them.

Works in terms of OSC files, not raw Iceberg snapshots. Each ``apply_osc``
creates multiple snapshots (MERGE + DELETE per type + index maintenance);
the inspector treats each complete apply as one logical step.

For each step, changed features are classified as added / modified / deleted.
Modified features flag whether the geometry changed; attribute-only changes
include the tag diff. Geometry changes emit both old and new geometries.

The HTML viewer uses MapLibre GL JS with a timeline slider.
"""

import json
import logging
import os
from typing import List, Optional, Tuple

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Snapshot queries
# ---------------------------------------------------------------------------


def list_snapshots(spark: SparkSession, table_name: str) -> List[dict]:
    """Return snapshot metadata ordered by commit time."""
    rows = spark.sql(f"""
        SELECT snapshot_id, committed_at, operation, summary
        FROM {table_name}.snapshots
        ORDER BY committed_at
    """).collect()
    return [row.asDict() for row in rows]


def _find_osc_boundaries(snapshots: List[dict]) -> List[Tuple[int, int]]:
    """Identify pairs of snapshots that bracket complete OSC applies.

    The init phase produces ``append`` snapshots. After that, each
    ``apply_osc`` produces a series of ``overwrite`` snapshots. We pair
    the last snapshot before each overwrite-group with the last snapshot
    of the group.

    Returns a list of ``(before_id, after_id)`` tuples.
    """
    if len(snapshots) < 2:
        return []

    last_append_idx = None
    for i, s in enumerate(snapshots):
        if s["operation"] == "append":
            last_append_idx = i

    if last_append_idx is None or last_append_idx >= len(snapshots) - 1:
        return []

    init_end = snapshots[last_append_idx]["snapshot_id"]

    # Each apply_osc creates ~6 overwrite snapshots. We don't try to
    # identify per-apply groups — just compare init-end to the final state.
    # For per-OSC diffs, the user runs inspect after each apply_osc call.
    return [(init_end, snapshots[-1]["snapshot_id"])]


# ---------------------------------------------------------------------------
# Diff computation
# ---------------------------------------------------------------------------


def diff_snapshots(
    spark: SparkSession,
    table_name: str,
    osm_type: str,
    snap_before: int,
    snap_after: int,
) -> list:
    """Find features that changed between two snapshots of a per-type table.

    ``osm_type`` ('node' / 'way' / 'relation') is baked into the diff rows
    as a literal so downstream GeoJSON consumers can still see the type.
    """
    spark.sql(f"""
        SELECT * FROM {table_name} VERSION AS OF {snap_before}
    """).createOrReplaceTempView("_snap_before")

    spark.sql(f"""
        SELECT * FROM {table_name} VERSION AS OF {snap_after}
    """).createOrReplaceTempView("_snap_after")

    return spark.sql(f"""
        SELECT
            COALESCE(a.id, b.id)     AS id,
            CAST('{osm_type}' AS STRING) AS type,
            CASE
                WHEN b.id IS NULL THEN 'added'
                WHEN a.id IS NULL THEN 'deleted'
                ELSE 'modified'
            END AS change,
            CASE
                WHEN b.geometry IS NULL AND a.geometry IS NULL THEN false
                WHEN b.geometry IS NULL OR  a.geometry IS NULL THEN true
                WHEN b.geometry != a.geometry                  THEN true
                ELSE false
            END AS geometry_changed,
            ST_AsGeoJSON(ST_GeomFromWKB(a.geometry)) AS geojson_after,
            ST_AsGeoJSON(ST_GeomFromWKB(b.geometry)) AS geojson_before,
            a.tags       AS tags_after,
            b.tags       AS tags_before,
            a.version    AS version_after,
            b.version    AS version_before,
            a.timestamp  AS ts_after,
            b.timestamp  AS ts_before
        FROM      _snap_after  a
        FULL OUTER JOIN _snap_before b ON a.id = b.id
        WHERE b.id IS NULL
           OR a.id IS NULL
           OR a.version != b.version
    """).collect()


# ---------------------------------------------------------------------------
# GeoJSON conversion
# ---------------------------------------------------------------------------


def _diff_tags(before: Optional[dict], after: Optional[dict]) -> dict:
    """Return ``{key: [old_value, new_value]}`` for every tag that changed."""
    before = before or {}
    after = after or {}
    changes = {}
    for key in set(before) | set(after):
        old = before.get(key)
        new = after.get(key)
        if old != new:
            changes[key] = [old, new]
    return changes


def _row_to_features(row, step: int, committed_at: str, osc_file: str = "") -> list:
    """Convert a diff row into one or more GeoJSON Feature dicts."""
    features = []
    change = row["change"]

    geojson_str = row["geojson_before"] if change == "deleted" else row["geojson_after"]
    geometry = json.loads(geojson_str) if geojson_str else None

    tags = dict(row["tags_before"] or {}) if change == "deleted" else dict(row["tags_after"] or {})

    props = {
        "@id": row["id"],
        "@type": row["type"],
        "@change": change,
        "@step": step,
        "@committed_at": committed_at,
        "@tags": tags,
    }
    if osc_file:
        props["@osc_file"] = osc_file

    if change == "added":
        props["@version"] = row["version_after"]
        props["@valid_since"] = _ts(row["ts_after"])
        props["@valid_until"] = None
    elif change == "deleted":
        props["@version"] = row["version_before"]
        props["@valid_since"] = _ts(row["ts_before"])
        props["@valid_until"] = committed_at
    else:
        props["@geometry_changed"] = row["geometry_changed"]
        props["@version"] = [row["version_before"], row["version_after"]]
        props["@valid_since"] = _ts(row["ts_after"])
        props["@valid_until"] = None
        changed = _diff_tags(row["tags_before"], row["tags_after"])
        if changed:
            props["@changed_tags"] = changed

    features.append({"type": "Feature", "geometry": geometry, "properties": props})

    if change == "modified" and row["geometry_changed"] and row["geojson_before"]:
        features.append({
            "type": "Feature",
            "geometry": json.loads(row["geojson_before"]),
            "properties": {
                "@id": row["id"],
                "@type": row["type"],
                "@role": "previous",
                "@step": step,
                "@valid_since": _ts(row["ts_before"]),
                "@valid_until": _ts(row["ts_after"]),
            },
        })

    return features


def _ts(val) -> Optional[str]:
    return str(val) if val is not None else None


def diff_to_geojson(
    rows: list,
    snap_before: int,
    snap_after: int,
    step: int = 0,
    committed_at: str = "",
    osc_file: str = "",
) -> dict:
    """Build a GeoJSON FeatureCollection from collected diff rows."""
    features = []
    for row in rows:
        features.extend(_row_to_features(row, step, committed_at, osc_file))

    return {
        "type": "FeatureCollection",
        "features": features,
    }


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------


def write_geojson(geojson: dict, path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(geojson, f, indent=2, default=str)


_VIEWER_HTML = """\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<title>KryptOSM Inspector</title>
<link rel="stylesheet" href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css" />
<script src="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"></script>
<style>
*{box-sizing:border-box}
html,body{height:100%;margin:0;font-family:system-ui,sans-serif;font-size:13px}
#map{position:absolute;top:0;left:0;right:0;bottom:54px}

.timeline{
  position:absolute;bottom:0;left:0;right:0;height:54px;
  background:#1e293b;color:#e2e8f0;
  display:flex;align-items:center;gap:10px;padding:0 16px;
  z-index:10;
}
.timeline button{
  background:#334155;border:none;color:#e2e8f0;
  width:32px;height:32px;border-radius:6px;font-size:16px;cursor:pointer;
}
.timeline button:hover{background:#475569}
.timeline input[type=range]{flex:1;accent-color:#60a5fa}
.step-label{white-space:nowrap;min-width:280px;text-align:right;font-variant-numeric:tabular-nums}

.info{
  position:absolute;top:10px;right:10px;z-index:10;
  background:#fff;padding:14px 18px;border-radius:8px;
  max-width:340px;box-shadow:0 2px 12px rgba(0,0,0,.25);line-height:1.5;
}
.info h3{margin:0 0 6px;font-size:15px}
.legend-row{display:flex;align-items:center;gap:8px;margin:3px 0}
.swatch{width:14px;height:14px;border-radius:3px;flex-shrink:0}

.maplibregl-popup-content{font-size:12px;line-height:1.5;max-height:320px;overflow-y:auto}
</style>
</head>
<body>
<div id="map"></div>

<div class="info">
  <h3>Snapshot Inspector</h3>
  <div id="summary"></div>
  <hr style="margin:8px 0">
  <div class="legend-row"><span class="swatch" style="background:#22c55e"></span> Added</div>
  <div class="legend-row"><span class="swatch" style="background:#f59e0b"></span> Modified (attributes)</div>
  <div class="legend-row"><span class="swatch" style="background:#3b82f6"></span> Modified (geometry, new)</div>
  <div class="legend-row"><span class="swatch" style="background:#818cf8;opacity:.6"></span> Modified (geometry, previous)</div>
  <div class="legend-row"><span class="swatch" style="background:#ef4444"></span> Deleted</div>
</div>

<div class="timeline">
  <button id="prev" title="Previous step">&#9664;</button>
  <input type="range" id="slider" min="0" max="0" value="0" step="1">
  <button id="next" title="Next step">&#9654;</button>
  <div class="step-label" id="step-label"></div>
</div>

<script>
var steps = __STEPS_JSON__;
var data  = __DATA_JSON__;

var map = new maplibregl.Map({
  container: 'map',
  style: {
    version: 8,
    sources: { osm: {
      type: 'raster',
      tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
      tileSize: 256,
      attribution: '&copy; <a href="https://openstreetmap.org">OpenStreetMap</a> contributors'
    }},
    layers: [{ id: 'osm', type: 'raster', source: 'osm' }]
  },
  center: [0, 0],
  zoom: 2
});

var colorExpr = [
  'case',
  ['==', ['get', '@role'], 'previous'],                                    '#818cf8',
  ['==', ['get', '@change'], 'added'],                                     '#22c55e',
  ['==', ['get', '@change'], 'deleted'],                                   '#ef4444',
  ['all', ['==', ['get', '@change'], 'modified'],
          ['==', ['get', '@geometry_changed'], true]],                      '#3b82f6',
  '#f59e0b'
];

var cur = 0;

map.on('load', function() {
  map.addSource('changes', { type: 'geojson', data: data });

  map.addLayer({
    id: 'fill', type: 'fill', source: 'changes',
    filter: ['all', ['==', ['get', '@step'], steps[0].step],
                    ['any', ['==', ['geometry-type'], 'Polygon'],
                            ['==', ['geometry-type'], 'MultiPolygon']]],
    paint: { 'fill-color': colorExpr, 'fill-opacity': 0.15 }
  });

  map.addLayer({
    id: 'line', type: 'line', source: 'changes',
    filter: ['all', ['==', ['get', '@step'], steps[0].step],
                    ['any', ['==', ['geometry-type'], 'LineString'],
                            ['==', ['geometry-type'], 'MultiLineString'],
                            ['==', ['geometry-type'], 'Polygon'],
                            ['==', ['geometry-type'], 'MultiPolygon']]],
    paint: {
      'line-color': colorExpr,
      'line-width': ['case', ['==', ['get', '@role'], 'previous'], 2, 3],
      'line-opacity': ['case', ['==', ['get', '@role'], 'previous'], 0.5, 0.85],
      'line-dasharray': ['case', ['==', ['get', '@role'], 'previous'],
                         ['literal', [2, 2]], ['literal', [1, 0]]]
    }
  });

  map.addLayer({
    id: 'circle', type: 'circle', source: 'changes',
    filter: ['all', ['==', ['get', '@step'], steps[0].step],
                    ['==', ['geometry-type'], 'Point']],
    paint: {
      'circle-radius': 5,
      'circle-color': colorExpr,
      'circle-stroke-color': '#333',
      'circle-stroke-width': 1,
      'circle-opacity': 0.85
    }
  });

  setStep(0);

  ['fill', 'line', 'circle'].forEach(function(layerId) {
    map.on('click', layerId, function(e) {
      var p = e.features[0].properties;
      var lines = ['<b>' + p['@type'] + '/' + p['@id'] + '</b>'];
      if (p['@role'] === 'previous') {
        lines.push('<em>previous geometry</em>');
        if (p['@valid_since']) lines.push('Valid: ' + p['@valid_since'] + ' &rarr; ' + p['@valid_until']);
      } else {
        lines.push('<em>' + p['@change'] + '</em>');
        if (p['@osc_file']) lines.push('OSC: ' + esc(p['@osc_file']));
        if (p['@valid_since']) lines.push('Valid since: ' + p['@valid_since'] +
          (p['@valid_until'] ? ' until ' + p['@valid_until'] : ''));
        var ver = tryParse(p['@version']);
        if (Array.isArray(ver))
          lines.push('v' + ver[0] + ' &rarr; v' + ver[1]);
        else if (ver)
          lines.push('v' + ver);
        if (p['@geometry_changed'] === true || p['@geometry_changed'] === 'true')
          lines.push('&#x1f7e6; Geometry changed');
        var ct = tryParse(p['@changed_tags']);
        if (ct && typeof ct === 'object') {
          lines.push('<b>Changed tags:</b>');
          for (var k in ct) {
            var v = ct[k];
            lines.push('&bull; ' + esc(k) + ': ' + (v[0]||'&empty;') + ' &rarr; ' + (v[1]||'&empty;'));
          }
        }
        var tags = tryParse(p['@tags']);
        if (tags && typeof tags === 'object') {
          var tagLines = [];
          for (var k in tags) tagLines.push(esc(k) + ' = ' + esc(String(tags[k])));
          if (tagLines.length) {
            lines.push('<b>Tags:</b>');
            tagLines.forEach(function(t) { lines.push('&bull; ' + t); });
          }
        }
      }
      new maplibregl.Popup({maxWidth: '360px'})
        .setLngLat(e.lngLat).setHTML(lines.join('<br>')).addTo(map);
    });
    map.on('mouseenter', layerId, function() { map.getCanvas().style.cursor = 'pointer'; });
    map.on('mouseleave', layerId, function() { map.getCanvas().style.cursor = ''; });
  });
});

function tryParse(v) {
  if (typeof v === 'string' && (v[0]==='{' || v[0]==='[')) {
    try { return JSON.parse(v); } catch(e) {}
  }
  return v;
}
function esc(s) {
  var d = document.createElement('span'); d.textContent = s; return d.innerHTML;
}

var slider = document.getElementById('slider');
slider.max = steps.length - 1;

function setStep(i) {
  cur = i;
  slider.value = i;
  var s = steps[i];
  var stepFilter = ['==', ['get', '@step'], s.step];

  map.setFilter('fill',   ['all', stepFilter,
    ['any', ['==',['geometry-type'],'Polygon'], ['==',['geometry-type'],'MultiPolygon']]]);
  map.setFilter('line',   ['all', stepFilter,
    ['any', ['==',['geometry-type'],'LineString'], ['==',['geometry-type'],'MultiLineString'],
            ['==',['geometry-type'],'Polygon'], ['==',['geometry-type'],'MultiPolygon']]]);
  map.setFilter('circle', ['all', stepFilter,
    ['==', ['geometry-type'], 'Point']]);

  document.getElementById('step-label').innerHTML =
    'Step ' + (i+1) + '/' + steps.length +
    (s.osc_file ? ' &mdash; ' + s.osc_file : '') +
    ' &mdash; +' + s.added + ' / ~' + s.modified + ' / -' + s.deleted;

  var bounds = null;
  data.features.forEach(function(f) {
    if (f.properties['@step'] !== s.step) return;
    if (!f.geometry) return;
    eachCoord(f.geometry, function(c) {
      if (!bounds) bounds = new maplibregl.LngLatBounds(c, c);
      else bounds.extend(c);
    });
  });
  if (bounds) map.fitBounds(bounds, {padding: 60, maxZoom: 16, duration: 600});

  document.getElementById('summary').innerHTML =
    'Added: ' + s.added + '<br>Modified: ' + s.modified +
    ' (' + s.geometry + ' geometry)<br>Deleted: ' + s.deleted;
}

function eachCoord(geom, fn) {
  if (geom.type === 'Point') { fn(geom.coordinates); return; }
  var arrs = geom.coordinates;
  (function walk(a) {
    if (typeof a[0] === 'number') fn(a);
    else a.forEach(walk);
  })(arrs);
}

slider.addEventListener('input', function() { setStep(+this.value); });
document.getElementById('prev').addEventListener('click', function() { if(cur>0) setStep(cur-1); });
document.getElementById('next').addEventListener('click', function() { if(cur<steps.length-1) setStep(cur+1); });
</script>
</body>
</html>
"""


def write_html_viewer(combined_geojson: dict, steps: List[dict], path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        html = _VIEWER_HTML.replace("__STEPS_JSON__", json.dumps(steps, default=str))
        html = html.replace("__DATA_JSON__", json.dumps(combined_geojson, default=str))
        f.write(html)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _count_features(geojson: dict) -> dict:
    added = modified = deleted = geom = 0
    for f in geojson["features"]:
        p = f["properties"]
        if p.get("@role") == "previous":
            continue
        c = p.get("@change")
        if c == "added":
            added += 1
        elif c == "modified":
            modified += 1
            if p.get("@geometry_changed"):
                geom += 1
        elif c == "deleted":
            deleted += 1
    return {"added": added, "modified": modified, "deleted": deleted, "geometry": geom}


def inspect_snapshots(
    spark: SparkSession,
    table_name: str,
    osm_type: str,
    output_dir: str,
    snap_before: Optional[int] = None,
    snap_after: Optional[int] = None,
) -> List[str]:
    """Generate GeoJSON diffs and a combined HTML timeline viewer.

    With no snapshot args, compares init-end to the current state of the
    given per-type table (all OSC changes for that type in one diff). For
    per-OSC diffs, call after each ``apply_osc``.

    ``osm_type`` ('node' / 'way' / 'relation') is the type carried in this
    table; it's baked into the GeoJSON properties as ``@type``.

    Returns a list of generated file paths.
    """
    snapshots = list_snapshots(spark, table_name)
    snap_map = {s["snapshot_id"]: s for s in snapshots}

    if snap_before is not None and snap_after is not None:
        pairs: List[Tuple[int, int]] = [(snap_before, snap_after)]
    else:
        pairs = _find_osc_boundaries(snapshots)
        if not pairs:
            logger.info("No OSC updates applied yet \u2014 nothing to diff.")
            return []

    all_features: List[dict] = []
    steps: List[dict] = []
    paths: List[str] = []

    for step_idx, (before_id, after_id) in enumerate(pairs):
        after_snap = snap_map.get(after_id, {})
        committed_at = str(after_snap.get("committed_at", ""))

        logger.info("Diffing snapshot %s \u2192 %s ...", before_id, after_id)
        rows = diff_snapshots(spark, table_name, osm_type, before_id, after_id)
        if not rows:
            logger.info("  No changes.")
            continue

        geojson = diff_to_geojson(
            rows, before_id, after_id,
            step=step_idx, committed_at=committed_at,
        )
        counts = _count_features(geojson)

        stem = f"diff_{osm_type}_{before_id}_{after_id}"
        gj_path = os.path.join(output_dir, f"{stem}.geojson")
        write_geojson(geojson, gj_path)
        paths.append(gj_path)

        logger.info(
            "  %d+ %d~ %d- (%d geometry changes) \u2192 %s",
            counts['added'], counts['modified'], counts['deleted'],
            counts['geometry'], gj_path,
        )

        all_features.extend(geojson["features"])
        steps.append({
            "step": step_idx,
            "committed_at": committed_at,
            **counts,
        })

    if steps:
        combined = {"type": "FeatureCollection", "features": all_features}
        html_path = os.path.join(output_dir, f"inspector_{osm_type}.html")
        write_html_viewer(combined, steps, html_path)
        paths.append(html_path)
        logger.info("Timeline viewer (%d steps): %s", len(steps), html_path)

    return paths
