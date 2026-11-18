import math

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession

st.set_page_config(page_title="Retards aériens", page_icon="✈️", layout="wide")
st.markdown(
    """
    <style>
    .stApp {
        background:
            radial-gradient(ellipse at 85% 0%, rgba(35, 91, 145, .20), transparent 38%),
            #0b1220;
        color: #f1f5f9;
    }
    .block-container { max-width: 1480px; padding-top: 2rem; padding-bottom: 3rem; }
    header[data-testid="stHeader"] { background: transparent; }
    h1, h2, h3, h4 { color: #f8fafc !important; }
    [data-testid="stMarkdownContainer"] p,
    [data-testid="stCaptionContainer"],
    [data-testid="stWidgetLabel"],
    [data-testid="stExpander"] summary,
    div[data-testid="stTabs"] button p {
        color: #e2eaf5 !important;
    }
    [data-testid="stSidebar"] { background: #101c2c; }
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"],
    [data-testid="stSidebar"] [data-testid="stWidgetLabel"] {
        color: #edf4fc !important;
    }
    .hero {
        padding: 1.8rem 2rem;
        margin-bottom: 1.3rem;
        border: 1px solid rgba(148, 163, 184, .18);
        border-radius: 22px;
        background: linear-gradient(115deg, rgba(20, 40, 66, .96), rgba(13, 25, 43, .82));
        box-shadow: 0 16px 40px rgba(0, 0, 0, .18);
    }
    .hero-kicker {
        color: #8be4d1;
        font-size: .76rem;
        font-weight: 700;
        letter-spacing: .16em;
        text-transform: uppercase;
    }
    .hero h1 { margin: .45rem 0 .4rem; color: #f4f8ff; font-size: 2.25rem; }
    .hero p { margin: 0; color: #d3deeb; font-size: 1rem; }
    [data-testid="stMetric"] {
        padding: 1rem 1.15rem;
        border: 1px solid rgba(148, 163, 184, .18);
        border-radius: 16px;
        background: linear-gradient(145deg, rgba(22, 38, 60, .94), rgba(15, 27, 45, .92));
    }
    [data-testid="stMetricLabel"] p { color: #d3deeb !important; font-size: .84rem; }
    [data-testid="stMetricValue"] { color: #f4f8ff; }
    div[data-testid="stTabs"] button { font-weight: 650; }
    div[data-testid="stExpander"] {
        border-color: rgba(148, 163, 184, .18);
        border-radius: 12px;
        background: rgba(15, 27, 45, .55);
    }
    div[data-testid="stExpander"] summary p { color: #edf4fc !important; }
    [data-testid="stPlotlyChart"] { border-radius: 14px; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hero">
      <div class="hero-kicker">Lakehouse aérien · données 2015</div>
      <h1>Retards et ponctualité des vols</h1>
      <p>Comparer les compagnies, suivre les tendances et explorer les profils des aéroports.</p>
    </div>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def get_spark():
    return (
        SparkSession.builder.appName("StreamlitGoldReader")
        .config("spark.driver.memory", "512m")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@st.cache_data(ttl=60)
def load_gold_table(hdfs_path: str):
    return get_spark().read.format("delta").load(hdfs_path).toPandas()


def style_chart(fig, height=420):
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"family": "Inter, sans-serif", "color": "#f1f5f9"},
        colorway=["#68cdb8", "#f0a36f", "#83aef5", "#c49af4"],
        margin={"l": 18, "r": 18, "t": 42, "b": 18},
        height=height,
        hoverlabel={"bgcolor": "#14243a", "bordercolor": "#64809f", "font": {"color": "#f8fafc"}},
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "x": 0,
            "font": {"color": "#f1f5f9"},
            "title": {"font": {"color": "#f1f5f9"}},
        },
    )
    fig.update_xaxes(
        showgrid=False,
        zerolinecolor="rgba(255,255,255,.24)",
        tickfont_color="#dbe7f3",
        title_font_color="#f1f5f9",
    )
    fig.update_yaxes(
        gridcolor="rgba(148,163,184,.20)",
        zerolinecolor="rgba(255,255,255,.24)",
        tickfont_color="#dbe7f3",
        title_font_color="#f1f5f9",
    )
    fig.update_annotations(font_color="#f1f5f9")
    return fig


try:
    airlines = load_gold_table("/lakehouse/gold/flights_airline_pareto")
    monthly = load_gold_table("/lakehouse/gold/flights_monthly")
    airports = load_gold_table("/lakehouse/gold/flights_airport_segments")
except AnalysisException as error:
    st.error("Tables Gold indisponibles : exécutez J3 et J4 avant d'ouvrir le tableau de bord.")
    st.exception(error)
    st.stop()

if airlines.empty or monthly.empty or airports.empty:
    st.error("Une table Gold est vide : vérifiez les résultats de J3 et J4.")
    st.stop()

total = int(airlines["n_vols"].sum())
positive_minutes = int(airlines["minutes_retard"].sum())
late_rate = float((airlines["retard_pct"] * airlines["n_vols"]).sum() / total)

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric("Vols analysés", f"{total:,}".replace(",", " "))
kpi2.metric("Retards ≥ 15 min", f"{late_rate:.1f} %")
kpi3.metric("Minutes de retard positives", f"{positive_minutes:,}".replace(",", " "))
kpi4.metric("Compagnies", f"{airlines['airline'].nunique()}")

with st.sidebar:
    st.markdown("### Filtres")
    options = sorted(airlines["airline"].tolist())
    selected = st.multiselect("Compagnies à comparer", options, default=options)
    st.caption("Le filtre s'applique aux graphiques des compagnies.")
    st.divider()
    st.caption("Les vues mensuelles, Pareto et aéroports restent globales.")

overview_tab, airline_tab, monthly_tab, airport_tab = st.tabs(
    ["Vue d'ensemble", "Compagnies", "Évolution mensuelle", "Aéroports"]
)

PARETO_SQL = """WITH grouped AS (
  SELECT airline, SUM(GREATEST(arrival_delay, 0)) AS minutes_retard
  FROM delta.`/lakehouse/silver/flights_enriched`
  WHERE cancelled = 0 AND arrival_delay IS NOT NULL GROUP BY airline
)
SELECT airline, minutes_retard,
       DENSE_RANK() OVER (ORDER BY minutes_retard DESC) AS rang,
       SUM(minutes_retard) OVER (
         ORDER BY minutes_retard DESC, airline
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) / SUM(minutes_retard) OVER () * 100 AS pareto_pct
FROM grouped"""

MONTHLY_SQL = """WITH monthly AS (
  SELECT DATE_TRUNC('MONTH', flight_date) AS mois, COUNT(*) AS n_vols,
         100 * AVG(CASE WHEN arrival_delay >= 15 THEN 1.0 ELSE 0.0 END) AS retard_pct,
         PERCENTILE_APPROX(arrival_delay, 0.5, 10000) AS mediane
  FROM delta.`/lakehouse/silver/flights_enriched`
  WHERE cancelled = 0 AND arrival_delay IS NOT NULL
  GROUP BY DATE_TRUNC('MONTH', flight_date)
)
SELECT *, LAG(n_vols) OVER (ORDER BY mois) AS vols_mois_precedent FROM monthly"""

with overview_tab:
    st.subheader("Vue d'ensemble")
    st.caption(
        "Les indicateurs de retard portent sur les vols non annulés dont le retard à l'arrivée est connu."
    )
    pareto = airlines.sort_values(["rang", "airline"])
    col1, col2 = st.columns([1.2, 1])
    with col1:
        st.markdown("#### Qui cumule le plus de minutes de retard ?")
        if pareto["pareto_pct"].isna().all():
            st.info("Le Pareto est indéfini car aucune minute de retard positive n'a été observée.")
        else:
            fig = px.area(
                pareto,
                x="airline",
                y="pareto_pct",
                markers=True,
                hover_data=["minutes_retard", "n_vols", "rang"],
                labels={
                    "airline": "Compagnie classée par minutes de retard",
                    "pareto_pct": "Part cumulée (%)",
                },
            )
            fig.add_hline(y=80, line_dash="dash", line_color="#f0a36f", annotation_text="80 %")
            st.plotly_chart(style_chart(fig, 400), width="stretch")
        with st.expander("Calcul du Pareto"):
            st.code(PARETO_SQL, language="sql")
            st.write(
                "Les avances contribuent zéro : le cumul porte sur les minutes de retard positives, "
                "pas sur un solde net."
            )

    with col2:
        st.markdown("#### Le taux de retard évolue-t-il selon le mois ?")
        monthly = monthly.sort_values("mois")
        fig = px.area(
            monthly,
            x="mois",
            y="retard_pct",
            markers=True,
            labels={"mois": "Mois", "retard_pct": "Vols avec retard ≥ 15 min (%)"},
        )
        fig.add_hline(y=late_rate, line_dash="dot", line_color="#83aef5", annotation_text="Moyenne annuelle")
        st.plotly_chart(style_chart(fig, 400), width="stretch")
        with st.expander("Calcul mensuel"):
            st.code(MONTHLY_SQL, language="sql")
            st.write("Le taux est calculé sur les vols non annulés avec retard connu.")

with airline_tab:
    st.subheader("Comparer les compagnies")
    filtered = airlines[airlines["airline"].isin(selected)].copy()
    if filtered.empty:
        st.info("Choisissez au moins une compagnie dans les filtres.")
    else:
        median_tab, rate_tab, spread_tab = st.tabs(
            ["Retard médian", "Part des vols en retard", "Dispersion (IQR)"]
        )
        with median_tab:
            st.caption("La médiane résiste mieux que la moyenne aux retards exceptionnellement élevés.")
            median_data = filtered.sort_values("mediane")
            fig = px.bar(
                median_data,
                x="mediane",
                y="airline",
                orientation="h",
                color="mediane",
                color_continuous_scale=["#68cdb8", "#83aef5", "#f0a36f"],
                hover_data=["airline_name", "n_vols", "p25", "p75", "p90"],
                labels={
                    "airline": "Compagnie",
                    "mediane": "Retard médian (min ; négatif = avance)",
                },
            )
            fig.add_vline(x=0, line_dash="dot", line_color="#a9bad0")
            st.plotly_chart(style_chart(fig, 480), width="stretch")
            with st.expander("Requête SQL & justification statistique"):
                st.code(
                    """SELECT airline, COUNT(*) AS n_vols,
       PERCENTILE_APPROX(arrival_delay, 0.25, 10000) AS p25,
       PERCENTILE_APPROX(arrival_delay, 0.50, 10000) AS mediane,
       PERCENTILE_APPROX(arrival_delay, 0.75, 10000) AS p75,
       PERCENTILE_APPROX(arrival_delay, 0.90, 10000) AS p90
FROM delta.`/lakehouse/silver/flights_enriched`
WHERE cancelled = 0 AND arrival_delay IS NOT NULL
GROUP BY airline""",
                    language="sql",
                )
                st.write("P25–P75 donne l'intervalle interquartile ; P90 décrit les retards les plus élevés.")

        with rate_tab:
            st.caption("Une arrivée est classée en retard à partir de 15 minutes.")
            composition = filtered.sort_values("retard_pct")
            fig = go.Figure()
            fig.add_bar(
                name="Retard ≥ 15 min",
                x=composition["retard_pct"],
                y=composition["airline"],
                orientation="h",
                marker_color="#f08c76",
                customdata=composition[["airline_name", "n_vols"]].to_numpy(),
                hovertemplate="%{customdata[0]}<br>Retard ≥ 15 min : %{x:.1f} %<br>"
                "Vols : %{customdata[1]:,}<extra></extra>",
            )
            fig.add_bar(
                name="Retard < 15 min",
                x=100 - composition["retard_pct"],
                y=composition["airline"],
                orientation="h",
                marker_color="#55c6a5",
                hovertemplate="Retard < 15 min : %{x:.1f} %<extra></extra>",
            )
            fig.update_layout(barmode="stack")
            fig.update_xaxes(range=[0, 100], title="Part des vols (%)")
            fig.update_yaxes(title="Compagnie")
            st.plotly_chart(style_chart(fig, 480), width="stretch")
            with st.expander("Calcul du taux & justification"):
                st.code(
                    """SELECT airline,
       100 * AVG(CASE WHEN arrival_delay >= 15 THEN 1.0 ELSE 0.0 END) AS retard_pct
FROM delta.`/lakehouse/silver/flights_enriched`
WHERE cancelled = 0 AND arrival_delay IS NOT NULL
GROUP BY airline""",
                    language="sql",
                )
                st.write(
                    "Les deux parts totalisent 100 % des vols observés pour chaque compagnie ; "
                    "les annulations et retards inconnus sont exclus."
                )

        with spread_tab:
            st.caption("Le point représente la médiane; la barre horizontale couvre le milieu de la distribution (P25–P75).")
            spread_data = filtered.assign(
                iqr_plus=filtered["p75"] - filtered["mediane"],
                iqr_minus=filtered["mediane"] - filtered["p25"],
            ).sort_values("mediane")
            fig = px.scatter(
                spread_data,
                x="mediane",
                y="airline",
                error_x="iqr_plus",
                error_x_minus="iqr_minus",
                color="p90",
                color_continuous_scale=["#68cdb8", "#83aef5", "#f0a36f"],
                hover_data=["airline_name", "n_vols", "p25", "p75", "p90"],
                labels={
                    "airline": "Compagnie",
                    "mediane": "Retard (minutes)",
                    "p90": "P90 (minutes)",
                },
            )
            fig.update_traces(marker_size=12)
            fig.add_vline(x=0, line_dash="dot", line_color="#dbe7f3")
            st.plotly_chart(style_chart(fig, 480), width="stretch")
            with st.expander("Requête SQL & lecture de la dispersion"):
                st.code(
                    """SELECT airline,
       PERCENTILE_APPROX(arrival_delay, 0.25, 10000) AS p25,
       PERCENTILE_APPROX(arrival_delay, 0.50, 10000) AS mediane,
       PERCENTILE_APPROX(arrival_delay, 0.75, 10000) AS p75,
       PERCENTILE_APPROX(arrival_delay, 0.90, 10000) AS p90
FROM delta.`/lakehouse/silver/flights_enriched`
WHERE cancelled = 0 AND arrival_delay IS NOT NULL
GROUP BY airline""",
                    language="sql",
                )
                st.write(
                    "La barre P25–P75 montre la dispersion centrale; le P90 en couleur situe la queue des retards."
                )

with monthly_tab:
    st.subheader("Évolution au fil de l'année")
    monthly = monthly.sort_values("mois")
    volume_col, delay_col = st.columns(2)
    with volume_col:
        st.markdown("#### Nombre de vols observés par mois")
        fig = px.bar(
            monthly,
            x="mois",
            y="n_vols",
            color="n_vols",
            color_continuous_scale=["#29496d", "#68cdb8"],
            hover_data=["vols_mois_precedent", "evolution_vols_pct"],
            labels={"mois": "Mois", "n_vols": "Vols"},
        )
        st.plotly_chart(style_chart(fig, 390), width="stretch")
        with st.expander("Requête SQL & comparaison"):
            st.code(MONTHLY_SQL, language="sql")
            st.write("LAG compare le volume du mois avec celui du mois précédent.")
    with delay_col:
        st.markdown("#### Part des arrivées avec ≥ 15 min de retard")
        fig = px.line(
            monthly,
            x="mois",
            y="retard_pct",
            markers=True,
            hover_data=["n_vols", "mediane", "evolution_vols_pct"],
            labels={"mois": "Mois", "retard_pct": "Vols en retard (%)"},
        )
        fig.add_hline(y=late_rate, line_dash="dot", line_color="#f0a36f", annotation_text="Moyenne annuelle")
        st.plotly_chart(style_chart(fig, 390), width="stretch")
        with st.expander("Requête SQL & choix de l'indicateur"):
            st.code(MONTHLY_SQL, language="sql")
            st.write("Le taux mensuel utilise le même dénominateur que le taux global.")

with airport_tab:
    st.subheader("Profils des aéroports")
    st.caption(
        "Chaque point représente un aéroport. Les couleurs indiquent les trois segments K-Means; "
        "la droite en pointillés montre une tendance descriptive."
    )
    fig = px.scatter(
        airports,
        x="n_vols",
        y="taux_retard",
        color="segment",
        size="n_vols",
        color_discrete_map={
            "Retard relatif faible": "#55c6a5",
            "Retard relatif intermédiaire": "#83aef5",
            "Retard relatif élevé": "#f08c76",
        },
        hover_name="origin_airport",
        hover_data=["airport_name", "distance_moyenne", "retard_positif_moyen", "taux_annulation"],
        log_x=True,
        labels={
            "n_vols": "Nombre de vols (échelle logarithmique)",
            "taux_retard": "Retards ≥ 15 min (%)",
            "segment": "Segment",
        },
    )
    log_volumes = [math.log10(float(value)) for value in airports["n_vols"]]
    late_rates = [float(value) for value in airports["taux_retard"]]
    mean_log_volume = sum(log_volumes) / len(log_volumes)
    mean_late_rate = sum(late_rates) / len(late_rates)
    variance_log_volume = sum((value - mean_log_volume) ** 2 for value in log_volumes)
    if variance_log_volume > 0:
        slope = sum(
            (volume - mean_log_volume) * (rate - mean_late_rate)
            for volume, rate in zip(log_volumes, late_rates)
        ) / variance_log_volume
        intercept = mean_late_rate - slope * mean_log_volume
        min_log_volume, max_log_volume = min(log_volumes), max(log_volumes)
        trend_log_volumes = [
            min_log_volume + (max_log_volume - min_log_volume) * i / 49 for i in range(50)
        ]
        fig.add_scatter(
            x=[10**value for value in trend_log_volumes],
            y=[intercept + slope * value for value in trend_log_volumes],
            mode="lines",
            name="Tendance linéaire (échelle log)",
            line={"color": "#f4f7fb", "dash": "dash", "width": 3},
            hoverinfo="skip",
        )
    st.plotly_chart(style_chart(fig, 560), width="stretch")
    with st.expander("Requête SQL & méthode de segmentation"):
        st.code(
            """SELECT origin_airport, COUNT(*) AS n_vols, AVG(distance) AS distance_moyenne,
       AVG(CASE WHEN cancelled = 0 AND arrival_delay IS NOT NULL
                THEN GREATEST(arrival_delay, 0) END) AS retard_positif_moyen,
       100 * AVG(CASE WHEN cancelled = 0 AND arrival_delay IS NOT NULL
                      THEN CASE WHEN arrival_delay >= 15 THEN 1.0 ELSE 0.0 END END) AS taux_retard,
       100 * AVG(cancelled) AS taux_annulation
FROM delta.`/lakehouse/silver/flights_enriched`
GROUP BY origin_airport HAVING COUNT(*) >= 100""",
            language="sql",
        )
        st.write(
            "K-Means utilise cinq indicateurs standardisés, après log1p pour les variables positives asymétriques. "
            "Les noms des segments sont relatifs aux médianes de retard des clusters. "
            "La tendance est ajustée au log du volume, avec un poids égal par aéroport; elle ne prouve pas une causalité."
        )
    st.markdown("#### Les annulations varient-elles selon le segment ?")
    fig = px.box(
        airports,
        x="segment",
        y="taux_annulation",
        color="segment",
        color_discrete_map={
            "Retard relatif faible": "#55c6a5",
            "Retard relatif intermédiaire": "#83aef5",
            "Retard relatif élevé": "#f08c76",
        },
        points="outliers",
        hover_data=["origin_airport", "airport_name", "n_vols"],
        labels={"segment": "Segment d'aéroport", "taux_annulation": "Annulations (%)"},
    )
    st.plotly_chart(style_chart(fig, 420), width="stretch")
    with st.expander("Méthode & interprétation"):
        st.code(
            """SELECT origin_airport, AVG(cancelled) * 100 AS taux_annulation
FROM delta.`/lakehouse/silver/flights_enriched`
GROUP BY origin_airport
HAVING COUNT(*) >= 100""",
            language="sql",
        )
        st.write(
            "Chaque boîte résume la distribution des taux d'annulation des aéroports du segment; "
            "les aéroports, et non les vols, sont les observations."
        )

st.caption("Source : tables Delta Gold de J3 et J4 · Projet d'analyse des retards aériens")
