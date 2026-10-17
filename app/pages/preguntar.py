"""Preguntar: chat with Claude over the processed results (read-only tools, daily spend cap)."""

from __future__ import annotations

import json
import os

import streamlit as st

from app import agent, data, ui

st.title("Preguntar")
st.caption("Pregunta en español sobre el catálogo, las listas, las alertas, el rendimiento o el CRO. "
           "Claude solo **consulta** los datos del tablero; cada cifra sale de una consulta que puedes revisar "
           "en «Datos consultados» debajo de la respuesta.")

if not os.environ.get("ANTHROPIC_API_KEY"):
    st.info("El agente no está configurado: falta `ANTHROPIC_API_KEY` en el servicio web.", icon="🔑")
    st.stop()

budget = data.agent_budget()
used = budget.usage()
spent = float(used.get("cost_usd", 0.0))
c1, c2, c3 = st.columns(3)
ui.kpi(c1, "Gasto de hoy", f"US$ {spent:.2f}", help="Suma del costo de todas las preguntas de hoy (hora de Guatemala), "
                                                    "de todos los usuarios.")
ui.kpi(c2, "Tope diario", f"US$ {budget.limit:.2f}", help="Es un máximo, no un gasto fijo. Al alcanzarlo, el chat se "
                                                         "pausa hasta mañana. Si nadie pregunta, el gasto es cero.")
ui.kpi(c3, "Preguntas hoy", int(used.get("questions", 0)))
st.progress(min(1.0, spent / budget.limit) if budget.limit else 1.0)

state = st.session_state
state.setdefault("agent_history", [])     # API messages (thinking blocks passed back unchanged)
state.setdefault("agent_display", [])     # what the page shows: {role, text, tools, cost}

EXAMPLES = ["¿Cómo está el catálogo hoy comparado con ayer?",
            "¿Qué le falta a la lista de Navidad para estar al 100 %?",
            "Dame los 20 SKUs con más vistas en 30 días que no tienen stock",
            "¿Qué SKUs tienen fuga de conversión en Hogar?"]


def show_tools(tools: list[dict]) -> None:
    if not tools:
        return
    with st.expander(f"Datos consultados ({len(tools)})"):
        for t in tools:
            st.markdown(f"**{t['tool']}** `{json.dumps(t['input'], ensure_ascii=False)}`"
                        + (" · ⚠️ error" if t["error"] else ""))
            try:
                st.json(json.loads(t["result"]), expanded=False)
            except ValueError:
                st.code(t["result"][:5000])


for msg in state.agent_display:
    with st.chat_message(msg["role"]):
        st.markdown(msg["text"])
        if msg["role"] == "assistant":
            show_tools(msg.get("tools", []))
            if msg.get("cost"):
                st.caption(f"Costo de esta respuesta: US$ {msg['cost']:.3f}")

if state.agent_display:
    if st.button("🧹 Nueva conversación"):
        state.agent_history, state.agent_display = [], []
        st.rerun()
else:
    st.markdown("**Ejemplos**")
    cols = st.columns(2)
    for i, q in enumerate(EXAMPLES):
        if cols[i % 2].button(q, key=f"ex{i}", width="stretch"):
            state.agent_pending = q
            st.rerun()

asked = sum(1 for m in state.agent_display if m["role"] == "user")
blocked = None
if not budget.can_call():
    blocked = f"Se alcanzó el tope de US$ {budget.limit:.2f} de hoy. El chat vuelve mañana."
elif asked >= agent.MAX_QUESTIONS:
    blocked = f"Esta conversación llegó a {agent.MAX_QUESTIONS} preguntas. Empieza una nueva con 🧹."

question = st.chat_input("Escribe tu pregunta…", disabled=blocked is not None) or state.pop("agent_pending", None)
if blocked:
    st.warning(blocked, icon="⏸️")

if question and not blocked:
    state.agent_display.append({"role": "user", "text": question})
    with st.chat_message("user"):
        st.markdown(question)
    with st.chat_message("assistant"):
        box = st.empty()
        status = st.status("Pensando…", expanded=False)
        buf: list[str] = []

        def on_text(chunk: str) -> None:
            buf.append(chunk)
            box.markdown("".join(buf) + "▌")

        def on_tool(entry: dict) -> None:
            status.update(label=f"Consultando {entry['tool']}…")
            status.write(f"{entry['tool']} {json.dumps(entry['input'], ensure_ascii=False)}")

        try:
            import anthropic

            days = data.run_days()
            res = agent.run_turn(anthropic.Anthropic(), state.agent_history, question,
                                 tools=agent.AgentTools(data), budget=budget,
                                 latest_day=days[0] if days else None, on_text=on_text, on_tool=on_tool)
        except Exception as exc:  # noqa: BLE001 - show API/network problems in the chat
            res = agent.TurnResult(text="", stopped="error")
            st.error(f"No pude completar la respuesta: {type(exc).__name__}: {str(exc)[:300]}")
        status.update(label=f"Listo · {len(res.tool_log)} consultas", state="complete")

        notes = {
            "budget": f"\n\n⏸️ Se alcanzó el tope diario de US$ {budget.limit:.2f}; la respuesta quedó incompleta.",
            "rounds": "\n\n⚠️ La pregunta necesitó demasiadas consultas; intenta una más específica.",
            "max_tokens": "\n\n⚠️ La respuesta se cortó por largo; pide una versión más corta.",
            "refusal": "No puedo ayudar con esa solicitud. Prueba a reformular la pregunta.",
            "error": "\n\n⚠️ Hubo un problema al generar la respuesta. Intenta de nuevo.",
        }
        text = res.text.strip() or ""
        if res.stopped in notes:
            text = (notes[res.stopped] if res.stopped == "refusal" else text + notes[res.stopped]).strip()
        box.markdown(text or "_(sin respuesta)_")
        show_tools(res.tool_log)
        if res.cost_usd:
            st.caption(f"Costo de esta respuesta: US$ {res.cost_usd:.3f}")
    state.agent_display.append({"role": "assistant", "text": text or "_(sin respuesta)_",
                                "tools": res.tool_log, "cost": res.cost_usd})
    st.rerun()
