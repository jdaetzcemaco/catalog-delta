"""
Run the legacy Streamlit app (v1 app.py) headlessly and record everything it shows.

A stand-in `streamlit` module is injected before app.py is executed. It feeds the
given files to the uploaders and records every metric, table and the Excel download,
so the rebuilt core can be compared against the exact legacy behaviour.

Usage:
    python tools/legacy_capture.py --app legacy/app.py \
        --today data/today.xlsx --yesterday data/yesterday.xlsx \
        --diseno data/diseno.xlsx --edicion data/edicion.xlsx \
        --out data/legacy_capture.pkl
"""

from __future__ import annotations

import argparse
import io
import os
import pickle
import runpy
import sys
import time
import traceback
import types


class _Stop(Exception):
    pass


class _NamedBytesIO(io.BytesIO):
    def __init__(self, content: bytes, name: str):
        super().__init__(content)
        self.name = name


class _Recorder:
    def __init__(self, files: dict):
        self.files = files
        self.tab = ""
        self.heading = ""
        self.metrics: list[dict] = []
        self.tables: list[dict] = []
        self.messages: list[dict] = []
        self.download: bytes | None = None


def _make_streamlit(rec: _Recorder) -> types.ModuleType:
    st = types.ModuleType("streamlit")

    class _Ctx:
        def __init__(self, tab: str | None = None):
            self._tab = tab

        def __enter__(self):
            if self._tab is not None:
                self._prev = rec.tab
                rec.tab = self._tab
                rec.heading = ""
            return self

        def __exit__(self, *exc):
            if self._tab is not None:
                rec.tab = self._prev
            return False

        def __getattr__(self, name):
            return getattr(st, name)

    class _State(dict):
        __getattr__ = dict.get

        def __setattr__(self, k, v):
            self[k] = v

    class _Secrets(dict):
        pass

    st.session_state = _State(authenticated=True)
    st.secrets = _Secrets()
    st.sidebar = _Ctx()

    def cache_resource(*a, **k):
        if a and callable(a[0]):
            return a[0]
        return lambda f: f

    def _noop(*a, **k):
        return None

    def _heading(text, *a, **k):
        rec.heading = str(text)

    def _message(kind):
        def f(body="", *a, **k):
            rec.messages.append({"tab": rec.tab, "heading": rec.heading, "kind": kind, "text": str(body)})
        return f

    def metric(label, value, delta=None, *a, **k):
        rec.metrics.append({"tab": rec.tab, "heading": rec.heading, "label": str(label), "value": value, "delta": delta})

    def dataframe(df, *a, **k):
        rec.tables.append({"tab": rec.tab, "heading": rec.heading, "df": df.copy()})

    def columns(spec, *a, **k):
        n = spec if isinstance(spec, int) else len(spec)
        return [_Ctx() for _ in range(n)]

    def tabs(names, *a, **k):
        return [_Ctx(tab=str(n)) for n in names]

    def file_uploader(label, type=None, key=None, **k):
        path = rec.files.get(key)
        if not path:
            return None
        with open(path, "rb") as fh:
            return _NamedBytesIO(fh.read(), os.path.basename(path))

    def date_input(label, value=None, *a, **k):
        return value

    def download_button(label, data=None, *a, **k):
        rec.download = data

    def exception(e):
        traceback.print_exception(type(e), e, e.__traceback__)
        rec.messages.append({"tab": rec.tab, "heading": rec.heading, "kind": "exception", "text": repr(e)})

    def stop():
        raise _Stop()

    st.cache_resource = cache_resource
    st.cache_data = cache_resource
    st.set_page_config = _noop
    st.title = _heading
    st.header = _heading
    st.subheader = _heading
    st.markdown = _noop
    st.write = _noop
    st.caption = _message("caption")
    st.info = _message("info")
    st.success = _message("success")
    st.warning = _message("warning")
    st.error = _message("error")
    st.exception = exception
    st.divider = _noop
    st.bar_chart = _noop
    st.metric = metric
    st.dataframe = dataframe
    st.columns = columns
    st.tabs = tabs
    st.expander = lambda *a, **k: _Ctx()
    st.spinner = lambda *a, **k: _Ctx()
    st.file_uploader = file_uploader
    st.text_input = lambda *a, **k: ""
    st.button = lambda *a, **k: False
    st.date_input = date_input
    st.download_button = download_button
    st.stop = stop
    st.rerun = _noop
    return st


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--app", required=True, help="Path to legacy app.py")
    p.add_argument("--today", required=True)
    p.add_argument("--yesterday", required=True)
    p.add_argument("--diseno")
    p.add_argument("--edicion")
    p.add_argument("--out", required=True)
    args = p.parse_args()

    rec = _Recorder({
        "today": args.today,
        "yesterday": args.yesterday,
        "diseno": args.diseno,
        "edicion": args.edicion,
    })
    sys.modules["streamlit"] = _make_streamlit(rec)
    # Legacy app imports catalog_delta from its own folder
    sys.path.insert(0, os.path.dirname(os.path.abspath(args.app)))

    t0 = time.time()
    try:
        runpy.run_path(args.app, run_name="__main__")
    except _Stop:
        pass
    elapsed = time.time() - t0

    with open(args.out, "wb") as fh:
        pickle.dump({
            "metrics": rec.metrics,
            "tables": rec.tables,
            "messages": rec.messages,
            "download": rec.download,
            "elapsed_s": elapsed,
        }, fh)

    errors = [m for m in rec.messages if m["kind"] in ("error", "exception")]
    print(f"Captured {len(rec.metrics)} metrics, {len(rec.tables)} tables, "
          f"excel={'yes' if rec.download else 'no'}, errors={len(errors)} in {elapsed:.0f}s")
    for m in errors:
        print("  ERROR:", m["text"][:300])
    return 0


if __name__ == "__main__":
    sys.exit(main())
