"""The 'Preguntar' agent: tools, input validation, cost and the daily cap (no network)."""

from __future__ import annotations

import json
from types import SimpleNamespace as NS

import pandas as pd
import pytest

from app import agent


# ── a small in-memory data source with the app.data interface ──────────────
class FakeSrc:
    def __init__(self):
        self.info = pd.DataFrame({
            "SKU": ["100", "200", "300"], "NOMBRE DE PRODUCTO": ["Sartén Antiadherente", "Silla Plegable", "Taladro"],
            "MARCA": ["Tramontina", "Cemaco", "Bosch"], "NIVEL 1": ["Hogar", "Muebles", "Ferretería"],
            "VISIBLE": ["Si", "Si", "No"], "TIENE STOCK": ["Si", "No", "Si"], "STOCK REAL": [5, 0, 3],
            "URL IMAGEN": ["x", "y", "z"]})
        self.perf_df = pd.DataFrame({"SKU": ["100", "200", "300"], "vistas_30d": [500.0, 900.0, 10.0],
                                     "compras_30d": [20.0, 0.0, 1.0], "NIVEL 1": ["Hogar", "Muebles", "Ferretería"],
                                     "visible": [True, True, False], "con_stock": [True, False, True]})
        self.cro_df = pd.DataFrame({"SKU": ["200"], "grupo": ["trafico_sin_stock"], "motivo": ["m"], "impacto": [9.0]})

    def run_days(self): return ["2026-10-16", "2026-10-15"]
    def manifest(self, day):
        return {"previous_day": "2026-10-15" if day == "2026-10-16" else None,
                "summary": {"Total SKUs": 3 if day == "2026-10-16" else 2, "Visible %": 66.7},
                "inventory_kpis": {"tipo_c_stock": 1}, "quality": {"stock_corrected": True},
                "tables": {"changes": {"New SKUs": "x.parquet"}}, "row_counts": {"changes:New SKUs": 1}}
    def table(self, day, group, key): return pd.DataFrame({"SKU": ["300"], "NOMBRE DE PRODUCTO": ["Taladro"]})
    def sku_info(self, day): return self.info
    def history(self):
        return pd.DataFrame({"Fecha": pd.to_datetime(["2026-10-15", "2026-10-16"]), "Visible %": [60.0, 66.7]})
    def alert_days(self): return ["2026-10-16"]
    def alerts(self, day):
        return [{"id": "a1", "severity": "alta", "title": "T", "detail": "D", "count": 2, "skus": ["100", "200"]},
                {"id": "a2", "severity": "media", "title": "T2", "count": 0, "skus": []}]
    def acks(self): return {"a2": {"at": "x"}}
    def lists(self): return {"navidad": {"name": "Navidad 2026", "status": "active", "last_day": "2026-10-16",
                                         "sku_count": 2, "last_kpis": {"listos": 1, "total": 2}}}
    def list_meta(self, list_id): return {"skus": ["100", "200"]}
    def list_status(self, list_id, day):
        return pd.DataFrame({"SKU": ["100", "200"], "en_catalogo": [True, True], "visible": [True, True],
                             "habilitado": [True, True], "contenido": [True, False], "categoria_real": [True, True],
                             "stock": [True, False], "stock_confiable": [True, True],
                             "listo": [True, False]})
    def perf(self): return self.perf_df
    def perf_meta(self): return {"as_of": "2026-10-15"}
    def cro(self): return self.cro_df, {"as_of": "2026-10-15"}
    def visible_history(self): return pd.DataFrame({"SKU": ["100"], "last_visible": ["2026-10-16"]})
    def work_log(self):
        return pd.DataFrame({"SKU": ["100", "100", "200"], "dia_reporte": ["2026-10-02", "2026-10-03", "2026-10-03"],
                             "equipo": ["Diseño", "Edición", "Edición"],
                             "usuario": ["ACASTILLO - Ashley", "AD_EDICION - Diego Conedera", "ACASTILLO - Ashley"],
                             "promovio": ["ACASTILLO - Ashley", "ACASTILLO - Ashley", None],
                             "salida_flujo": pd.to_datetime(["2026-10-01"] * 3), "producto": ["Sartén", "Sartén", "Silla"]})
    def productivity_days(self): return ["2026-10-16"]
    def productivity(self, day):
        from catalog_core import build_productivity
        d = pd.DataFrame({"<ID>": ["100", "200"], "<Name>": ["Sartén", "Silla"], "Categoría": ["Hogar", "Muebles"],
                          "Usuario": ["ana", "ana"], "Usuario Promueve desde Catalogo": ["beto", None],
                          "Fecha de Salida del Flujo de trabajo": ["2026-10-15", "2026-10-15"], "Total Omnicanal": [3, 0]})
        e = d.rename(columns={"Usuario Promueve desde Catalogo": "Usuario Promueve desde Compras"}).iloc[:1]
        ing = pd.DataFrame({"<ID>": ["catgo-1", "100"], "<Name>": ["X", "Sartén"], "Categoría": [None, "Hogar"],
                            "Completitud Mercadeo (Calculado)": [10, 50], "Estado Imagen Primaria": ["Sin Imagen", "Con Imagen"],
                            "Fecha de Creación STEP": ["2026-10-15"] * 2})
        return build_productivity(d, e, ing)
    def queries_meta(self): return {"as_of": "2026-10-27", "desde": "2026-09-30", "dias": 28}
    def queries(self, name):
        if name == "sku":
            return pd.DataFrame({"SKU": ["100", "100", "200"], "query": ["sarten antiadherente", "(anónimas)", "silla plegable"],
                                 "impresiones": [120.0, 30.0, 80.0], "clics": [6.0, 0.0, 2.0], "posicion": [4.2, 9.0, 7.5],
                                 "dias": [20, 25, 10], "ctr": [0.05, 0.0, 0.025]})
        return pd.DataFrame({"query": ["sarten antiadherente", "silla plegable"], "impresiones": [120.0, 80.0],
                             "clics": [6.0, 2.0], "posicion": [4.2, 7.5], "skus": [1, 1], "ctr": [0.05, 0.025]})
    def seo_meta(self): return {"as_of": "2026-10-20", "window": 14}
    def seo(self, name):
        if name == "cohorts":
            return pd.DataFrame({"cohorte": ["2026-09-28", "2026-09-28"], "equipo": ["Todos", "Edición"], "skus": [2, 2],
                                 "completo": [True, True], "con_presencia": [1, 1], "nuevo": [0, 0], "crecio": [1, 1], "bajo": [0, 0],
                                 "estable": [0, 0], "sin_presencia": [1, 1], "no_visible": [0, 0], "delta_impr_pct": [50.0, 50.0],
                                 "ctrl_delta_impr_pct": [10.0, 10.0], "uplift_impr_pct": [40.0, 40.0], "muestra_pequena": [True, True]})
        if name == "skus":
            return pd.DataFrame({"SKU": ["100", "200"], "cohorte": ["2026-09-28"] * 2, "equipo": ["Edición"] * 2, "usuarios": ["ana", "ana"],
                                 "event_day": ["2026-10-01"] * 2, "estado": ["crecio", "sin_presencia"], "impr_antes": [100.0, 0.0],
                                 "impr_despues": [150.0, 0.0], "delta_impr_pct": [50.0, None], "uplift_impr_pct": [40.0, None],
                                 "clics_antes": [10.0, 0.0], "clics_despues": [15.0, 0.0], "pos_antes": [4.0, None], "pos_despues": [4.1, None],
                                 "cambios_ficha": ["+descripción", ""]})
        if name == "queries":
            return pd.DataFrame({"SKU": ["100", "100"], "query": ["sarten", "(anónimas)"], "estado": ["nueva", "estable"],
                                 "impr_antes": [0.0, 5.0], "impr_despues": [30.0, 6.0], "clics_antes": [0.0, 0.0], "clics_despues": [3.0, 0.0],
                                 "pos_antes": [None, 3.0], "pos_despues": [5.0, 3.0], "anonima": [False, True], "ml": [False, False], "ps": [True, False]})
        return None


@pytest.fixture
def tools():
    return agent.AgentTools(FakeSrc())


def call(tools, name, **args):
    text, is_error = tools.execute(name, args)
    return json.loads(text), is_error


def test_every_tool_answers(tools):
    out, err = call(tools, "resumen_dia")
    assert not err and out["dia"] == "2026-10-16" and out["cambio_vs_anterior"]["Total SKUs"] == 1
    out, err = call(tools, "cambios", grupo="changes", tabla="new skus")
    assert not err and out["tabla"] == "New SKUs" and out["total_filas"] == 1
    out, err = call(tools, "alertas", incluir_revisadas=False)
    assert not err and [a["id"] for a in out["alertas"]] == ["a1"]
    out, err = call(tools, "sku", sku="200")
    assert not err and out["catalogo"]["MARCA"] == "Cemaco" and out["cro"][0]["grupo"] == "trafico_sin_stock"
    assert "URL IMAGEN" not in out["catalogo"] and out["listas"][0]["preparacion"]["listo"] is False
    out, err = call(tools, "buscar_sku", texto="sarten")          # accent-insensitive
    assert not err and out["skus"][0]["SKU"] == "100"
    out, err = call(tools, "lista", nombre="navidad")
    assert not err and out["fallan_por_check"] == {"en_catalogo": 0, "visible": 0, "habilitado": 0, "contenido": 1,
                                                   "categoria_real": 0, "stock": 1}
    assert out["no_listos"][0]["falla"] == ["contenido", "stock"]
    assert out["pendientes_por_motivo"]["Pendiente en STEP"] == 1 and out["no_listos"][0]["motivo_pendiente"]
    out, err = call(tools, "ranking_skus", metrica="vistas_30d", con_stock=False)
    assert not err and [s["SKU"] for s in out["skus"]] == ["200"] and out["skus"][0]["NOMBRE DE PRODUCTO"]
    out, err = call(tools, "historial", metricas=["Visible %"], desde="2026-10-16")
    assert not err and out["serie"] == [{"Fecha": "2026-10-16", "Visible %": 66.7}]
    out, err = call(tools, "cro")
    assert not err and out["conteo_por_grupo"] == {"trafico_sin_stock": 1}
    out, err = call(tools, "impacto_seo")
    assert not err and out["cohorte"]["uplift_impr_pct"] == 40.0 and out["cohorte"]["equipo"] == "Todos"
    assert [s["SKU"] for s in out["skus_con_mayor_cambio"]] == ["100"] and out["visibles_sin_presencia"][0]["SKU"] == "200"
    out, err = call(tools, "impacto_seo", sku="100")
    assert not err and out["impacto"]["estado"] == "crecio" and out["consultas"]["nueva"][0]["query"] == "sarten"
    out, err = call(tools, "impacto_seo", semana="2026-01-05")
    assert err and "2026-09-28" in out["error"]
    out, err = call(tools, "sku", sku="100")
    assert not err and out["impacto_seo"]["estado"] == "crecio"


def test_tool_errors_go_back_to_the_model(tools):
    out, err = call(tools, "resumen_dia", dia="2020-01-01")
    assert err and "Días disponibles" in out["error"]
    out, err = call(tools, "ranking_skus", metrica="inventada")
    assert err and "vistas_30d" in out["error"]
    out, err = call(tools, "historial", metricas=["nope"])
    assert err and "Visible %" in out["error"]
    out, err = call(tools, "sku")                                   # missing required field
    assert err and out["error"].startswith("INVALID_INPUT")
    out, err = call(tools, "cro", grupo="otro")                     # not in enum
    assert err and "INVALID_INPUT" in out["error"]
    out, err = call(tools, "buscar_sku", texto="ab", limite=500)    # above maximum
    assert err


def test_results_are_trimmed_to_fit():
    big = {"filas": [{"x": "y" * 100}] * 2000}
    text = agent._dump(big)
    assert len(text) <= agent.MAX_RESULT_CHARS and "recortado" in json.loads(text)


def test_cost_uses_model_prices_and_cache_rates():
    usage = NS(input_tokens=1_000_000, output_tokens=100_000, cache_read_input_tokens=1_000_000,
               cache_creation_input_tokens=0)
    assert agent.cost_usd(usage, "claude-opus-5-5") == pytest.approx(4 + 2 + 0.2)
    # an unknown (e.g. fallback) model is priced at the highest row, never lower
    assert agent.cost_usd(usage, "some-other-model") >= agent.cost_usd(usage, "claude-opus-5-5")


# ── the loop, with a scripted fake client ──────────────────────────────────
class FakeStream:
    def __init__(self, message):
        self.message = message

    def __enter__(self): return self
    def __exit__(self, *a): return False

    def __iter__(self):
        for b in self.message.content:
            if b.type == "text":
                yield NS(type="text", text=b.text)

    def get_final_message(self): return self.message


class FakeClient:
    def __init__(self, messages):
        self.queue, self.requests = list(messages), []
        self.beta = NS(messages=NS(stream=self._stream))

    def _stream(self, **kwargs):
        self.requests.append(kwargs)
        return FakeStream(self.queue.pop(0))


def msg(stop, *blocks, out=100):
    return NS(stop_reason=stop, content=list(blocks), model="claude-opus-5-5",
              usage=NS(input_tokens=1000, output_tokens=out, cache_read_input_tokens=0, cache_creation_input_tokens=0))


class MemoryBudget(agent.DailyBudget):
    def __init__(self, limit):
        self.store = {}
        super().__init__(limit, self.store.get, self.store.__setitem__)


def test_loop_runs_tools_and_charges_each_call(tools):
    client = FakeClient([
        msg("tool_use", NS(type="text", text="Consulto. "),
            NS(type="tool_use", id="t1", name="sku", input={"sku": "100"}),
            NS(type="tool_use", id="t2", name="cro", input={})),
        msg("end_turn", NS(type="text", text="El SKU 100 tiene 500 vistas.")),
    ])
    budget, history = MemoryBudget(5), []
    res = agent.run_turn(client, history, "¿Cómo va el 100?", tools=tools, budget=budget)
    assert res.stopped == "end_turn" and "500 vistas" in res.text and len(res.tool_log) == 2
    # both tool results returned together in one user message, in order
    results = history[2]["content"]
    assert [r["tool_use_id"] for r in results] == ["t1", "t2"]
    req = client.requests[0]
    assert req["fallbacks"] == "default" and agent.FALLBACK_BETA in req["betas"]
    assert req["cache_control"] == {"type": "ephemeral"} and "tool_choice" not in req
    assert all(t["eager_input_streaming"] for t in req["tools"])
    usage = budget.usage()
    assert usage["calls"] == 2 and usage["questions"] == 1
    assert usage["cost_usd"] == pytest.approx(res.cost_usd) == pytest.approx(2 * (1000 * 4 + 100 * 20) / 1e6)


def test_invalid_tool_input_is_an_error_result(tools):
    client = FakeClient([
        msg("tool_use", NS(type="tool_use", id="t1", name="sku", input={"sku": 100})),   # int, not string
        msg("end_turn", NS(type="text", text="ok")),
    ])
    history = []
    agent.run_turn(client, history, "q", tools=tools, budget=MemoryBudget(5))
    result = history[2]["content"][0]
    assert result["is_error"] and "INVALID_INPUT" in result["content"]


def test_daily_cap_blocks_calls_before_they_happen(tools):
    budget = MemoryBudget(5)
    budget.charge(5 - agent.worst_call_cost(0) / 2)         # less than one full-output call left
    client = FakeClient([msg("end_turn", NS(type="text", text="no debería llamarse"))])
    res = agent.run_turn(client, [], "q", tools=tools, budget=budget)
    assert res.stopped == "budget" and client.requests == []


def test_round_cap_and_follow_up_question_merge(tools):
    loop = [msg("tool_use", NS(type="tool_use", id=f"t{i}", name="cro", input={})) for i in range(3)]
    client = FakeClient(loop)
    history = []
    res = agent.run_turn(client, history, "q", tools=tools, budget=MemoryBudget(5), max_rounds=3)
    assert res.stopped == "rounds" and history[-1]["role"] == "user"
    agent.add_user_text(history, "otra")                      # pending tool results + new question = one turn
    assert history[-1]["content"][-1] == {"type": "text", "text": "otra"}
    assert history[-1]["content"][0]["type"] == "tool_result"


def test_refusal_drops_the_question(tools):
    client = FakeClient([msg("refusal")])
    history = [{"role": "user", "content": "antes"}, {"role": "assistant", "content": []}]
    res = agent.run_turn(client, history, "q", tools=tools, budget=MemoryBudget(5))
    assert res.stopped == "refusal" and len(history) == 2


def test_max_tokens_with_open_tool_call_is_not_kept(tools):
    client = FakeClient([msg("max_tokens", NS(type="tool_use", id="t1", name="cro", input={}))])
    history = []
    res = agent.run_turn(client, history, "q", tools=tools, budget=MemoryBudget(5))
    assert res.stopped == "max_tokens" and history[-1]["role"] == "user"


def test_trabajo_by_sku_and_by_user(tools):
    out, err = call(tools, "trabajo", sku="100")
    assert not err and out["filas"] == 2 and out["cobertura"]["dias_con_reporte"] == 2
    who = {(r["usuario"], r["rol"]) for r in out["resumen_por_usuario"]}
    assert ("AD_EDICION - Diego Conedera", "trabajó") in who and ("ACASTILLO - Ashley", "promovió") in who
    out, err = call(tools, "trabajo", usuario="conedera")
    assert not err and out["skus"] == 1 and [r["usuario"] for r in out["resumen_por_usuario"]] == ["AD_EDICION - Diego Conedera"]
    out, err = call(tools, "trabajo", usuario="ashley", desde="2026-10-03")
    assert not err and out["skus"] == 2
    out, err = call(tools, "trabajo")
    assert err and "sku" in out["error"]
    out, _ = call(tools, "sku", sku="100")
    assert out["productividad"]["usuarios_que_lo_trabajaron"] == ["ACASTILLO - Ashley", "AD_EDICION - Diego Conedera"]


def test_productividad_day_and_period(tools):
    out, err = call(tools, "productividad")
    assert not err and out["modo"] == "dia" and out["skus_unicos"]["total"] == 2
    assert out["skus_unicos"]["ambos"] == 1 and out["salidas_del_flujo"]["fecha"] == "2026-10-15"
    assert out["salidas_del_flujo"]["con_inventario"] == 1
    assert out["ingresos_a_step"]["total"] == 2 and out["ingresos_a_step"]["con_sku"] == 1
    out, err = call(tools, "productividad", desde="2026-10-01", hasta="2026-10-31")
    assert not err and out["modo"] == "acumulado" and out["skus_unicos"] == 2
    assert out["skus_unicos_por_equipo"] == {"Diseño": 1, "Edición": 2} and out["skus_en_ambos_equipos"] == 1
    out, err = call(tools, "productividad", dia="2020-01-01")
    assert err


def test_totales_rendimiento(tools):
    out, err = call(tools, "totales_rendimiento")
    assert not err and out["total"]["vistas"] == 1410.0 and out["total"]["compras"] == 21.0
    assert out["total"]["skus_con_vistas"] == 3
    out, err = call(tools, "totales_rendimiento", agrupar="nivel1", con_stock=True)
    assert not err and {r["NIVEL 1"] for r in out["por_nivel1"]} == {"Hogar", "Ferretería"}
    out, err = call(tools, "totales_rendimiento", trabajado_por="Edición", trabajado_desde="2026-10-03")
    assert not err and out["skus_trabajados"] == 2 and out["total"]["vistas"] == 1400.0
    out, err = call(tools, "totales_rendimiento", trabajado_por="Diseño")
    assert not err and out["skus_considerados"] == 1 and out["total"]["compras"] == 20.0
    out, err = call(tools, "totales_rendimiento", agrupar="canal")   # fixture has no channel columns
    assert err


def test_consultas_google(tools):
    out, err = call(tools, "consultas_google", sku="100")
    assert not err and [q["query"] for q in out["consultas"]] == ["sarten antiadherente"]
    assert out["impresiones_anonimas"] == 30.0 and out["impresiones_totales"] == 150.0
    out, err = call(tools, "consultas_google", texto="Sartén")             # accent-insensitive
    assert not err and out["consultas_que_coinciden"] == 1 and out["skus_que_mas_aparecen"][0]["SKU"] == "100"
    out, err = call(tools, "consultas_google")
    assert not err and out["consultas_top"][0]["query"] == "sarten antiadherente"


def test_every_page_area_has_a_tool():
    names = set(agent.TOOLS_BY_NAME)
    assert {"resumen_dia", "cambios", "historial", "alertas", "lista", "ranking_skus", "totales_rendimiento",
            "consultas_google", "cro", "impacto_seo", "productividad", "trabajo", "sku", "buscar_sku"} <= names
