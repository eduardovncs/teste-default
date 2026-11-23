"""Testes do plan_parser. Rode com: pytest (ou python tests/test_plan_parser.py)

Os trechos imitam o formato real do Databricks/Photon, com nomes genéricos."""

from spark_lineage.plan_parser import (
    is_collapsed_to_empty,
    normalize_table,
    other_relation_classes,
    parse_merge_targets,
    parse_read_schema,
    parse_relations,
    parse_scans,
    parse_views,
    split_top_level,
    strip_ids,
)

SCAN_OK = (
    "   +- PhotonScan parquet cat.sch.clientes[id#1,saldo#2,tags#3] DataFilters: [isnotnull(id#1)], "
    "Format: parquet, Location: PreparedDeltaFileIndex(1 paths)[s3://x..., "
    "ReadSchema: struct<id:int,saldo:decimal(18,2),tags:array<string>>, "
    "RequiredDataFilters: [isnotnull(id#1)]"
)

# ReadSchema cortado em ~100 chars e um '>' (operador) depois, em outro filtro: não pode fechar o struct
SCAN_CUT = (
    "PhotonScan parquet cat.sch.mov[a#1,b#2,c#3,d#4] DataFilters: [isnotnull(a#1), (a#1 > 0), CAS..., "
    "Format: parquet, ReadSchema: struct<a:string,b:timestamp,c:int,d:decima..., "
    "RequiredDataFilters: [isnotnull(a#1), (a#1 > 0), CAS..."
)


def test_primitivas():
    assert strip_ids("cbco#14731L, x#1") == "cbco, x"
    assert normalize_table("`dev`.`s`.`t`") == "dev.s.t"
    assert split_top_level("a:int,b:decimal(18,2),c:map<string,int>", angle=True) == [
        "a:int",
        "b:decimal(18,2)",
        "c:map<string,int>",
    ]


def test_relations():
    plan = (
        "Project [id#1]\n"
        "+- SubqueryAlias cat.sch.clientes\n"
        "   +- Relation cat.sch.clientes[id#1,saldo#2L,nome#3] parquet\n"
        "   +- Relation [x#9,... 4 more fields] parquet\n"
    )
    rels = parse_relations(plan)
    assert rels[0]["table"] == "cat.sch.clientes" and rels[0]["format"] == "parquet"
    assert [c["name"] for c in rels[0]["columns"]] == ["id", "saldo", "nome"]
    assert rels[0]["columns"][1]["id"] == 2 and not rels[0]["columns_truncated"]
    assert rels[1]["table"] is None and rels[1]["columns_truncated"] is True


def test_read_schema_completo():
    schema, trunc = parse_read_schema(SCAN_OK)
    assert schema == {"id": "int", "saldo": "decimal(18,2)", "tags": "array<string>"} and not trunc


def test_read_schema_truncado():
    schema, trunc = parse_read_schema(SCAN_CUT)
    assert trunc is True
    assert schema == {"a": "string", "b": "timestamp", "c": "int"}  # 'd' estava cortado: descartado


def test_scans():
    ok = parse_scans(SCAN_OK)[0]
    assert ok["table"] == "cat.sch.clientes" and ok["read_schema"]["saldo"] == "decimal(18,2)"
    assert ok["filters"] == ["isnotnull(id)"] and ok["filters_truncated"] is False
    cut = parse_scans(SCAN_CUT)[0]
    assert [c["name"] for c in cut["columns"]] == ["a", "b", "c", "d"]
    assert cut["read_schema_truncated"] and cut["filters_truncated"]
    assert cut["filters"] == ["isnotnull(a)", "(a > 0)"]  # o filtro cortado ("CAS") foi descartado
    assert parse_scans("LocalTableScan <empty>, [a#1, b#2]") == []


def test_merge_views_e_outros():
    merge = (
        "+- MergeIntoCommandEdge SubqueryAlias s, SubqueryAlias t, `c`.`s`.`alvo`, Delta[version=3, x], "
        "(t.id = s.id), [Update [actions: [`id` = id#5, `valor` = valor#6]]], [Insert [actions: [`id` = id#5]]]"
    )
    assert parse_merge_targets(merge) == [{"table": "c.s.alvo", "columns": ["id", "valor"]}]
    assert parse_views("+- View (`vw_x`, [a#1, b#2])\n+- View (`c`.`s`.`vw_y`, [a#1])") == ["vw_x", "c.s.vw_y"]
    texto = "LocalRelation [a#1]\nCTERelationRef 3, true, [a#1]\nCTERelationDef 3, false\nRelation c.s.t[a#1] parquet"
    assert dict(other_relation_classes(texto)) == {"LocalRelation": 1}
    assert is_collapsed_to_empty("LocalTableScan <empty>, [a#1]")
    assert not is_collapsed_to_empty(SCAN_OK)


def test_doctor():
    from spark_lineage.doctor import render_doctor_text, run_doctor

    rep = run_doctor(connect=False, check_auth=False)
    assert rep["status"] in ("ok", "warn", "error")
    assert rep["python"]["status"] in ("ok", "warn", "error")
    pkg_imports = {p["import"] for p in rep["packages"]}
    assert {"databricks.connect", "databricks.sdk", "delta.tables", "pandas"} <= pkg_imports
    assert all(p["required"] is True for p in rep["packages"])
    txt = render_doctor_text(rep)
    assert "spark-lineage doctor" in txt


def test_branch_explorer_and_sql_target():
    import ast

    from spark_lineage.capture_plans import (
        BranchExplorer,
        _BranchTransformer,
        _should_instrument_if,
        extract_sql_target,
    )

    assert extract_sql_target("DELETE FROM dev.sch.tb_rejeitada WHERE dt_ref = '2026-11-20'") == "dev.sch.tb_rejeitada"
    assert extract_sql_target("INSERT INTO TABLE `dev`.`sch`.`tb` SELECT 1") == "dev.sch.tb"
    assert extract_sql_target("MERGE INTO dev.sch.alvo t USING src s ON t.id = s.id") == "dev.sch.alvo"

    # Guardas técnicos ignorados vs decisão de negócio instrumentada
    assert not _should_instrument_if(ast.parse("__name__ == '__main__'", mode="eval").body)
    assert not _should_instrument_if(ast.parse("x is None", mode="eval").body)
    assert not _should_instrument_if(ast.parse("p not in sys.path", mode="eval").body)
    assert _should_instrument_if(ast.parse("modo_execucao == 'REPROCESSAMENTO'", mode="eval").body)

    src = (
        "def job(modo='INCREMENTAL'):\n"
        "    if modo is None:\n"
        "        modo = 'INCREMENTAL'\n"
        "    if modo == 'REPROCESSAMENTO':\n"
        "        visited.append('REPROC')\n"
        "    else:\n"
        "        visited.append('INCR')\n"
    )
    tree = _BranchTransformer("job.py").visit(ast.parse(src))
    ast.fix_missing_locations(tree)
    code = compile(tree, filename="job.py", mode="exec")

    explorer = BranchExplorer(enabled=True)
    visited: list[str] = []
    ns: dict[str, object] = {"__spark_lineage_branch__": explorer.decide, "visited": visited}
    exec(code, ns)
    job_fn = ns["job"]
    assert callable(job_fn)

    # 1ª passada: fluxo padrão (INCREMENTAL)
    explorer.start_pass(0)
    job_fn()
    assert visited == ["INCR"]

    # Deve ter descoberto 1 ramo pendente (True para modo == 'REPROCESSAMENTO')
    nxt = explorer.next_unvisited()
    assert nxt is not None
    site_id, forced_val, _cond_str, forces = nxt
    assert site_id == "job.py:4" and forced_val is True

    # 2ª passada: força o ramo True
    explorer.start_pass(1, target_site=site_id, target_value=forced_val, planned_forces=forces)
    job_fn()
    assert visited == ["INCR", "REPROC"]
    assert explorer.next_unvisited() is None


def test_patcher_create_display():
    from typing import Any

    from spark_lineage.capture_plans import DF_ACTIONS, Patcher

    assert "display" in DF_ACTIONS

    class FakeDF:
        pass

    p = Patcher()
    called = []
    p.patch(FakeDF, "display", lambda _orig: lambda self, *a, **kw: called.append(self), create=True)
    df: Any = FakeDF()
    assert hasattr(df, "display")
    df.display()
    assert called == [df]
    p.restore()
    assert not hasattr(FakeDF, "display")


def test_formatted_scans_e_catalog_truncado():
    from spark_lineage.plan_parser import parse_formatted_scans
    from spark_lineage.sources import build_sources, render_markdown

    formatted = (
        "== Physical Plan ==\n"
        "PhotonProject (2)\n"
        "+- PhotonScan parquet dev.sch.pix (1)\n\n"
        "(1) PhotonScan parquet dev.sch.pix\n"
        "Output [3]: [id#1, cbco_orig#2, vl#3]\n"
        "PartitionFilters: [(dt#4 = 2026-11-20)]\n"
        "ReadSchema: struct<id:string,cbco_orig:int,vl:decimal(18,2)>\n"
        "RequiredDataFilters: [isnotnull(cbco_orig#2), (cbco_orig#2 > 0)]\n\n"
        "(2) PhotonProject\n"
        "Input [3]: [id#1, cbco_orig#2, vl#3]\n"
    )
    scans = parse_formatted_scans(formatted)
    assert len(scans) == 1
    assert scans[0]["table"] == "dev.sch.pix"
    assert [c["name"] for c in scans[0]["columns"]] == ["id", "cbco_orig", "vl"]
    assert scans[0]["read_schema"] == {"id": "string", "cbco_orig": "int", "vl": "decimal(18,2)"}
    assert not scans[0]["read_schema_truncated"] and not scans[0]["columns_truncated"]
    assert "(dt = 2026-11-20)" in scans[0]["filters"] and "(cbco_orig > 0)" in scans[0]["filters"]

    # Caso 1: plano antigo com scan truncado + --catalog -> colunas extras ficam com read=None e aviso explícito
    res_trunc = {
        "entrypoint": "job.py",
        "status": "ok",
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": "Relation dev.sch.grande[c1#1,c2#2,... 2 more fields] parquet",
                    "physical": "PhotonScan parquet dev.sch.grande[c1#1,... 1 more fields] ReadSchema: struct<c1:int>",
                },
                "occurrences": [],
            }
        ],
    }
    catalog = {
        "dev.sch.grande": {
            "c1": {"type": "int", "comment": None},
            "c2": {"type": "string", "comment": "col 2"},
            "c3": {"type": "double", "comment": None},
            "c4": {"type": "date", "comment": None},
        }
    }
    doc = build_sources(res_trunc, catalog=catalog)
    cols_by_name = {c["name"]: c for c in doc["sources"][0]["columns"]}
    assert set(cols_by_name.keys()) == {"c1", "c2", "c3", "c4"}
    assert cols_by_name["c1"]["read"] is True
    assert cols_by_name["c2"]["read"] is None and cols_by_name["c3"]["read"] is None
    assert doc["sources"][0]["scan_columns_truncated"] is True
    assert any("scan foi truncada" in w for w in doc["warnings"])
    assert "leitura indeterminada" in render_markdown(doc)

    # Caso 2: mesmo plano COM `formatted` -> parse_scans usa formatted (sem truncamento), c2/c3/c4 ficam read=False
    res_fmt = {
        "entrypoint": "job.py",
        "status": "ok",
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": "Relation dev.sch.grande[c1#1,c2#2,... 2 more fields] parquet",
                    "physical": "PhotonScan parquet dev.sch.grande[c1#1,... 1 more fields] ReadSchema: struct<c1:int>",
                    "formatted": (
                        "(1) PhotonScan parquet dev.sch.grande\n"
                        "Output [2]: [c1#1, c2#2]\n"
                        "ReadSchema: struct<c1:int,c2:string>\n"
                    ),
                },
                "occurrences": [],
            }
        ],
    }
    doc2 = build_sources(res_fmt, catalog=catalog)
    cols2 = {c["name"]: c for c in doc2["sources"][0]["columns"]}
    assert cols2["c1"]["read"] is True and cols2["c2"]["read"] is True
    assert cols2["c3"]["read"] is False and cols2["c4"]["read"] is False
    assert doc2["sources"][0]["scan_columns_truncated"] is False


def test_extract_tag_e_discover_tables():
    from spark_lineage.capture_plans import extract_sql_sources
    from spark_lineage.sources import build_sources, discover_plan_tables, extract_tag, render_markdown

    assert extract_sql_sources("SELECT * FROM dev.sch.a JOIN `dev`.`sch`.`b` ON a.id = b.id") == [
        "dev.sch.a",
        "dev.sch.b",
    ]

    plan_doc = {
        "entrypoint": "job.py",
        "status": "ok",
        "catalog": {
            "dev.sch.origem": {"id": {"type": "int", "comment": "chave"}, "nome": {"type": "string", "comment": None}},
        },
        "read_sites": {
            "sch.origem": ["job.py:3"],
        },
        "plans": [
            {
                "id": "P001",
                "output_columns": [{"name": "id", "type": "int"}],
                "sections": {
                    "analyzed": "Relation dev.sch.origem[id#1,nome#2] parquet",
                    "physical": "PhotonScan parquet dev.sch.origem[id#1] ReadSchema: struct<id:int>",
                },
                "occurrences": [
                    {
                        "action": "write.saveAsTable",
                        "target": "dev.sch.destino",
                        "call_site": {"file": "job.py", "line": 10},
                    }
                ],
            }
        ],
        "branches": [
            {
                "site": "job.py:5",
                "condition": "x > 0",
                "natural_value": True,
                "explored_values": [True, False],
                "status": {"True": "ok", "False": "ok"},
            }
        ],
        "events": [],
        "capture_warnings": [],
    }
    assert discover_plan_tables(plan_doc["plans"]) == ["dev.sch.origem", "dev.sch.destino"]
    # build_sources usa automaticamente o catalog e os read_sites já embutidos no plan_doc
    doc = build_sources(plan_doc)
    assert doc["sources"][0]["types_from"] == "catalog"
    assert doc["sources"][0]["columns"][0]["comment"] == "chave"
    assert doc["sources"][0]["call_sites"] == ["job.py:3"]

    # Consulta por tag e filtro de tabela
    only_dest = extract_tag(plan_doc, doc, tag="destinations", table="destino")
    assert len(only_dest) == 1 and only_dest[0]["table"] == "dev.sch.destino"
    only_br = extract_tag(plan_doc, doc, tag="branches")
    assert len(only_br) == 1 and only_br[0]["site"] == "job.py:5"
    md_dest = render_markdown(doc, tag="destinations")
    assert "## Destinos (1)" in md_dest and "## Fontes lidas" not in md_dest


def test_impact_multi_hop():
    from spark_lineage.impact import render_impact_markdown, trace_column_impact

    plan_doc = {
        "entrypoint": "job.py",
        "status": "ok",
        "catalog": {
            "dev.sch.pix": {"cbco_orig": {"type": "int", "comment": "Banco origem"}},
        },
        "read_sites": {"dev.sch.pix": ["job.py:5"]},
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": (
                        "cbco_orig: int, flag: boolean\n"
                        "Project [cbco_orig#1, (cbco_orig#1 = 341) AS flag#2]\n"
                        "+- Filter (cbco_orig#1 > 0)\n"
                        "   +- Relation dev.sch.pix[cbco_orig#1,vl#9] parquet\n"
                    ),
                    "physical": "",
                },
                "occurrences": [
                    {
                        "action": "write.saveAsTable",
                        "target": "dev.sch.stg",
                        "call_site": {"file": "job.py", "line": 12},
                    }
                ],
            },
            {
                "id": "P002",
                "sections": {
                    "analyzed": (
                        "cod_fmt: string\n"
                        "Project [lpad(cast(cbco_orig#10 as string), 3, 0) AS cod_fmt#11]\n"
                        "+- Relation dev.sch.stg[cbco_orig#10,flag#12] parquet\n"
                    ),
                    "physical": "",
                },
                "occurrences": [
                    {
                        "action": "write.saveAsTable",
                        "target": "dev.sch.final",
                        "call_site": {"file": "job.py", "line": 20},
                    }
                ],
            },
        ],
    }
    rep = trace_column_impact(plan_doc, table="pix", column="cbco_orig")
    assert rep["table"] == "dev.sch.pix" and rep["comment"] == "Banco origem"
    assert rep["summary"]["found"] is True and rep["summary"]["plans_affected"] == 2
    dest_tables = [d["table"] for d in rep["destinations_affected"]]
    assert dest_tables == ["dev.sch.stg", "dev.sch.final"]
    final_cols = [c["name"] for c in rep["destinations_affected"][1]["columns"]]
    assert final_cols == ["cod_fmt"]
    assert "Impacto de `dev.sch.pix.cbco_orig`" in render_impact_markdown(rep)


def test_sql_e_notebooks_preprocessing():
    import ast
    from spark_lineage.capture_plans import (
        DBUtilsStub,
        preprocess_notebook_source,
        substitute_sql_params,
    )
    from spark_lineage.impact import trace_column_impact
    from spark_lineage.plan_parser import parse_insert_target_columns

    # 1. Default values em dbutils.widgets
    db = DBUtilsStub({"env": "prod"})
    db.widgets.text("dt_ref", "2026-11-20")
    db.widgets.dropdown("env", "dev", ["dev", "prod"])
    assert db.widgets.get("dt_ref") == "2026-11-20"
    assert db.widgets.get("env") == "prod"  # --param prevalece sobre o default
    assert substitute_sql_params("SELECT * FROM ${env}.sch.tb WHERE dt = :dt_ref", db.widgets.getAll()) == (
        "SELECT * FROM prod.sch.tb WHERE dt = '2026-11-20'"
    )

    # 2. Arquivo .sql com múltiplos comandos e -- COMMAND ----------
    sql_src = (
        "-- Cabeçalho\n"
        "CREATE OR REPLACE TEMP VIEW vw_x AS\n"
        "SELECT id, valor FROM dev.sch.orig WHERE txt = 'a;b';\n"
        "-- COMMAND ----------\n"
        "INSERT INTO dev.sch.dest\n"
        "SELECT id, valor FROM vw_x\n"
    )
    py_from_sql = preprocess_notebook_source(sql_src, "job.sql")
    # Preserva exatamente o número de linhas e compila como AST Python válido
    assert len(py_from_sql.splitlines()) == len(sql_src.splitlines())
    ast.parse(py_from_sql)
    assert "__spark_lineage_sql__" in py_from_sql.splitlines()[1]
    assert "__spark_lineage_sql__" in py_from_sql.splitlines()[4]

    # 3. Notebook .py do Databricks com # MAGIC %run, %sql e %pip mantendo linhas
    nb_py = (
        "# Databricks notebook source\n"
        "# MAGIC %run ./commons/helpers $dt_ref='2026-11-20'\n"
        "# COMMAND ----------\n"
        "# MAGIC %sql\n"
        "# MAGIC SELECT * FROM dev.sch.orig\n"
        "# COMMAND ----------\n"
        "if modo == 'FULL':\n"
        "    pass\n"
    )
    py_from_nb = preprocess_notebook_source(nb_py, "nb.py")
    assert len(py_from_nb.splitlines()) == len(nb_py.splitlines())
    tree = ast.parse(py_from_nb)
    if_nodes = [n for n in ast.walk(tree) if isinstance(n, ast.If)]
    assert len(if_nodes) == 1 and if_nodes[0].lineno == 7

    # 4. parse_insert_target_columns e impact em AppendData RelationV2 (sql:INSERT)
    analyzed_insert = (
        "AppendData RelationV2[id_dest#10, vl_dest#11] dev.sch.dest\n"
        "+- Project [id#1, (vl#2 * 2) AS dobro#3]\n"
        "   +- Relation dev.sch.orig[id#1,vl#2] parquet\n"
    )
    assert parse_insert_target_columns(analyzed_insert) == [
        {"name": "id_dest", "type": None},
        {"name": "vl_dest", "type": None},
    ]
    rep = trace_column_impact(
        {
            "entrypoint": "job.sql",
            "status": "ok",
            "plans": [
                {
                    "id": "P001",
                    "sections": {"analyzed": analyzed_insert, "physical": ""},
                    "occurrences": [
                        {
                            "action": "sql:INSERT",
                            "target": "dev.sch.dest",
                            "call_site": {"file": "job.sql", "line": 5},
                        }
                    ],
                }
            ],
        },
        table="orig",
        column="vl",
    )
    assert rep["destinations_affected"][0]["table"] == "dev.sch.dest"
    assert [c["name"] for c in rep["destinations_affected"][0]["columns"]] == ["vl_dest"]



if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("OK ", name)
