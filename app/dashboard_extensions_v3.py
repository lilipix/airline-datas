"""Exploration quotidienne : un onglet unique, une date partagée et trois analyses.
Ne modifie aucune table Gold ni aucun des quatre onglets historiques.
"""
from datetime import date
from html import escape
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

GOLD_DAILY = '/lakehouse/gold/flights_daily_stats'
SILVER_FLIGHTS = '/lakehouse/silver/flights_enriched'
GOLD_AIRPORT = '/lakehouse/gold/flights_airport_daily_map'
GOLD_ROUTES = '/lakehouse/gold/flights_routes_daily_map'
GOLD_TRAFFIC = '/lakehouse/gold/flights_airport_traffic_delays'
EVENTS = {
    date(2015, 1, 1): 'Nouvel An',
    date(2015, 7, 4): 'Fête nationale',
    date(2015, 11, 25): 'Veille de Thanksgiving',
    date(2015, 11, 26): 'Thanksgiving',
    date(2015, 11, 27): 'Lendemain de Thanksgiving',
    date(2015, 12, 25): 'Noël',
}
SPECIAL_EVENTS = {
    date(2015, 2, 1): 'Super Bowl XLIX · départs PHX',
}

@st.cache_data(ttl=600, show_spinner=False)
def _daily(_spark):
    return _spark.read.format('delta').load(GOLD_DAILY).orderBy('flight_date').toPandas()

@st.cache_data(ttl=600, show_spinner=False)
def _geo(_spark, path, day):
    from pyspark.sql import functions as F
    return (_spark.read.format('delta').load(path)
            .filter(F.col('flight_date') == F.lit(day)).toPandas())

@st.cache_data(ttl=600, show_spinner=False)
def _hours(_spark, day, airport):
    from pyspark.sql import functions as F
    df = (_spark.read.format('delta').load(GOLD_TRAFFIC)
          .filter(F.col('flight_date') == F.lit(day)))
    if airport != 'Tous les aéroports':
        df = df.filter(F.col('origin_airport') == airport)
    return (df.groupBy('scheduled_hour')
            .agg(F.sum('traffic_volume').alias('scheduled'),
                 F.sum('observed_flights').alias('observed'),
                 F.sum('delayed_flights').alias('delayed'))
            .orderBy('scheduled_hour').toPandas())


@st.cache_data(ttl=600, show_spinner=False)
def _airport_names(_spark):
    """Charge les noms complets des aéroports depuis la couche Bronze."""
    try:
        df = (_spark.read.format('delta').load('/lakehouse/bronze/airports')
              .select('iata_code', 'airport')
              .dropna(subset=['iata_code', 'airport'])
              .dropDuplicates(['iata_code'])
              .toPandas())
        return dict(zip(df['iata_code'], df['airport']))
    except Exception:
        return {}


@st.cache_data(ttl=600, show_spinner=False)
def _weather(_spark, day):
    from pyspark.sql import functions as F
    columns = [
        'flight_date', 'weather_observation_time_utc', 'air_temperature_c',
        'wind_speed_mps', 'precipitation_1h_mm',
    ]
    df = _spark.read.format('delta').load(SILVER_FLIGHTS)
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f'Colonnes NOAA absentes de Silver : {missing}')
    return (
        df.filter(F.col('flight_date') == F.lit(day))
        .select(*columns)
        .dropDuplicates(['flight_date', 'weather_observation_time_utc'])
        .agg(
            F.avg('air_temperature_c').alias('temperature'),
            F.avg('wind_speed_mps').alias('wind_speed'),
            F.max('wind_speed_mps').alias('max_wind_speed'),
            F.sum(F.when(F.col('precipitation_1h_mm') > 0, 1).otherwise(0)).alias('rain_hours'),
            F.sum(F.coalesce('precipitation_1h_mm', F.lit(0.0))).alias('precipitation'),
            F.count('weather_observation_time_utc').alias('observed_hours'),
        )
        .toPandas()
    )


@st.cache_data(ttl=600, show_spinner=False)
def _weather_association(_spark):
    from pyspark.sql import functions as F
    columns = [
        'origin_airport', 'flight_date', 'scheduled_departure_utc_hour',
        'air_temperature_c', 'wind_speed_mps', 'precipitation_1h_mm',
        'cancelled', 'arrival_delay',
    ]
    df = _spark.read.format('delta').load(SILVER_FLIGHTS)
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f'Colonnes nécessaires à l’association NOAA absentes : {missing}')
    return (
        df.select(*columns)
        .groupBy(
            'origin_airport', 'flight_date', 'scheduled_departure_utc_hour',
            'air_temperature_c', 'wind_speed_mps', 'precipitation_1h_mm',
        )
        .agg(
            F.count('*').alias('scheduled_flights'),
            F.sum(F.when((F.col('cancelled') == 0) & F.col('arrival_delay').isNotNull(), 1)
                 .otherwise(0)).alias('observed_flights'),
            F.sum(F.when((F.col('cancelled') == 0) & (F.col('arrival_delay') >= 15), 1)
                 .otherwise(0)).alias('delayed_flights'),
        )
        .filter(
            (F.col('observed_flights') >= 10)
            & F.col('air_temperature_c').isNotNull()
            & F.col('wind_speed_mps').isNotNull()
            & F.col('precipitation_1h_mm').isNotNull()
        )
        .withColumn('delay_rate', 100 * F.col('delayed_flights') / F.col('observed_flights'))
        .toPandas()
    )


def _render_weather_association(get_spark):
    try:
        groups = _weather_association(get_spark())
    except Exception as exc:
        st.warning('Association NOAA-retards indisponible : exécuter J1 puis J2.')
        st.caption(str(exc))
        return
    if groups.empty:
        st.info('Pas assez de groupes aéroport/heure avec des observations NOAA complètes.')
        return

    rain = groups['precipitation_1h_mm'] > 0
    rain_rate = groups.loc[rain, 'delay_rate'].mean()
    dry_rate = groups.loc[~rain, 'delay_rate'].mean()
    wind_corr = groups['wind_speed_mps'].corr(groups['delay_rate'], method='spearman')
    temperature_corr = groups['air_temperature_c'].corr(groups['delay_rate'], method='spearman')
    st.markdown('**☔ Association météo-retards — année 2015 complète**')
    c1, c2, c3 = st.columns(3)
    c1.metric('Avec précipitations', '—' if pd.isna(rain_rate) else f'{rain_rate:.1f} %')
    c2.metric('Sans précipitations', '—' if pd.isna(dry_rate) else f'{dry_rate:.1f} %')
    c3.metric(
        'Écart pluie − sec',
        '—' if pd.isna(rain_rate) or pd.isna(dry_rate) else f'{rain_rate - dry_rate:+.1f} pt',
    )
    st.caption(
        f'Spearman vent / retard : {wind_corr:+.2f} · '
        f'température / retard : {temperature_corr:+.2f} · '
        f'{len(groups):,} groupes aéroport/heure analysés sur l’année 2015 complète. '
        'Cette association ne décrit donc pas uniquement la journée sélectionnée. '
        'Association descriptive, pas preuve de causalité.'
    )


def _event_label(day):
    return EVENTS.get(day) or SPECIAL_EVENTS.get(day)


def _event_choices():
    return ['Choisir librement une journée'] + [
        f'{name} — {day:%d/%m/%Y}'
        for day, name in {**EVENTS, **SPECIAL_EVENTS}.items()
    ]


def _date_state():
    if 'ext_selected_date' not in st.session_state:
        st.session_state.ext_selected_date = date(2015, 11, 26)
    return st.session_state.ext_selected_date

def _event_changed():
    """Callback appelé AVANT la reconstruction des widgets Streamlit."""
    label = st.session_state.get('ext_event_choice', '')
    for day, name in {**EVENTS, **SPECIAL_EVENTS}.items():
        if label == f'{name} — {day:%d/%m/%Y}':
            st.session_state.ext_selected_date = day
            st.session_state.ext_calendar = day
            return


def _calendar_changed():
    day = st.session_state.ext_calendar
    st.session_state.ext_selected_date = day
    st.session_state.ext_event_choice = (
        f'{_event_label(day)} — {day:%d/%m/%Y}'
        if _event_label(day) else 'Choisir librement une journée'
    )


def _daily_summary(get_spark):
    daily = _daily(get_spark()).copy()
    if daily.empty:
        return daily, None
    daily['flight_date'] = pd.to_datetime(daily['flight_date'])
    day = _date_state()
    chosen = daily.loc[daily.flight_date.dt.date == day]
    return daily, chosen.iloc[0] if not chosen.empty else None


def _render_weather_context(get_spark, day):
    try:
        weather = _weather(get_spark(), day).iloc[0]
    except Exception as exc:
        st.warning('Analyse NOAA indisponible : exécuter J1 puis J2.')
        st.caption(str(exc))
        return
    if int(weather.observed_hours or 0) == 0:
        st.info('Aucune observation NOAA associée aux départs de cette journée.')
        return

    st.markdown('**🌦️ Conditions observées par NOAA**')
    c1, c2, c3 = st.columns(3)
    c1.metric(
        'Température moyenne',
        '—' if pd.isna(weather.temperature) else f'{weather.temperature:.1f} °C',
    )
    c2.metric(
        'Vent moyen',
        '—' if pd.isna(weather.wind_speed) else f'{weather.wind_speed:.1f} m/s',
    )
    c3.metric(
        'Heures avec précipitations',
        f'{int(weather.rain_hours or 0)} h',
    )
    st.caption(
        f'{int(weather.observed_hours)} heures NOAA observées · '
        f'cumul de précipitations : {weather.precipitation or 0:.1f} mm · '
        'mesures météo réelles, distinctes de la cause de retard déclarée.'
    )


def _render_super_bowl_context(get_spark, day):
    if day != date(2015, 2, 1):
        return
    try:
        airport = _geo(get_spark(), GOLD_AIRPORT, day)
    except Exception as exc:
        st.warning('Comparaison Super Bowl indisponible : vérifier les tables Gold.')
        st.caption(str(exc))
        return
    phx = airport[airport.origin_airport == 'PHX']
    if phx.empty:
        st.info('Aucune donnée de départ PHX pour le Super Bowl XLIX.')
        return
    row = phx.iloc[0]
    st.markdown(
        '<div style="background:#151f30;border:1px solid #2a3547;'
        'border-left:5px solid #4cc9f0;border-radius:10px;'
        'padding:10px 16px;margin:12px 0;color:#f1f5f9;">'
        '<strong>🏈 Super Bowl XLIX · Phoenix (PHX)</strong><br>'
        f'Vols programmés : {int(row.scheduled_flights):,} · '
        f'Retards à l’arrivée : {row.delay_rate:.1f} % · '
        'périmètre local aux départs PHX.'
        '</div>',
        unsafe_allow_html=True,
    )


def _render_daily_summary(get_spark):
    try:
        daily, row = _daily_summary(get_spark)
    except Exception as exc:
        st.warning('Synthèse quotidienne indisponible : exécuter le notebook J3.')
        st.caption(str(exc))
        return
    if row is None:
        st.info('Aucune donnée disponible pour la journée sélectionnée.')
        return

    day = _date_state()
    annual_rate = float(daily['delay_rate'].mean())
    day_rate = float(row.delay_rate)
    delta = day_rate - annual_rate
    if delta <= -1:
        status_color, status_icon, status_label = '#3ddc97', '🟢', 'Journée plutôt ponctuelle'
    elif delta >= 1:
        status_color, status_icon, status_label = '#ff6b6b', '🔴', 'Journée plus perturbée que la moyenne'
    else:
        status_color, status_icon, status_label = '#ffc933', '🟡', 'Journée proche de la moyenne'

    c1, c2, c3, c4 = st.columns(4)
    c1.metric('Vols programmés', f'{int(row.scheduled_flights):,}'.replace(',', ' '))
    c2.metric('Retards ≥ 15 min', f'{day_rate:.1f} %', f'{delta:+.1f} pt vs moyenne')
    c3.metric('Annulations', f'{row.cancellation_rate:.2f} %')
    c4.metric('Retard médian', f'{int(row.median_arrival_delay)} min')
    st.markdown(
        f'<div style="background:#151f30;border:1px solid #2a3547;'
        f'border-left:5px solid {status_color};border-radius:10px;'
        'padding:10px 16px;margin:2px 0 8px;color:#f1f5f9;">'
        f'<strong>{status_icon} {status_label}</strong> · '
        f'{day:%d/%m/%Y} · Taux de retard : {day_rate:.1f} % · '
        f'Moyenne annuelle : {annual_rate:.1f} %'
        '</div>',
        unsafe_allow_html=True,
    )


def render_exploration_tab(get_spark, style_chart):
    """Point d'entrée unique : les trois analyses partagent le même calendrier."""
    st.subheader('📅 Exploration quotidienne des vols — 2015')
    st.caption('Sélectionnez une journée pour comparer les événements, les aéroports et les horaires.')
    _date_state()
    if 'ext_calendar' not in st.session_state:
        st.session_state.ext_calendar = st.session_state.ext_selected_date
    if 'ext_event_choice' not in st.session_state:
        day = st.session_state.ext_selected_date
        st.session_state.ext_event_choice = (
            f'{_event_label(day)} — {day:%d/%m/%Y}' if _event_label(day)
            else 'Choisir librement une journée'
        )
    event_choices = _event_choices()
    c1, c2 = st.columns([1.15, 1])
    with c1:
        st.selectbox('Événement', event_choices,
                     key='ext_event_choice', on_change=_event_changed)
    with c2:
        st.date_input('Journée étudiée', min_value=date(2015, 1, 1),
                      max_value=date(2015, 12, 31), format='DD/MM/YYYY',
                      key='ext_calendar', on_change=_calendar_changed)
    _render_daily_summary(get_spark)
    st.divider()
    day = _date_state()
    event_label = _event_label(day)
    context_label = f'**Analyse détaillée** · Date : **{day:%d/%m/%Y}**'
    if event_label:
        context_label += f' · Événement : **{escape(event_label)}**'
    st.markdown(context_label)
    events_tab, map_tab, traffic_tab = st.tabs(
        ['📈 Événements et ponctualité', '🗺️ Carte des aéroports', '🕒 Trafic horaire']
    )
    with events_tab:
        render_events_tab(get_spark, style_chart)
    with map_tab:
        render_map_tab(get_spark, style_chart)
    with traffic_tab:
        render_traffic_tab(get_spark, style_chart)


def _chart(fig, style_chart, height=390):
    fig = style_chart(fig, height)
    fig.update_layout(title=None, margin={'l': 30, 'r': 20, 't': 30, 'b': 35})
    st.plotly_chart(fig, use_container_width=True)

def render_events_tab(get_spark, style_chart):
    day = _date_state()
    st.subheader('Événements et ponctualité')
    st.caption('Les losanges bleus signalent les événements ; le point rouge indique la journée sélectionnée.')
    try:
        daily = _daily(get_spark()).copy()
    except Exception as exc:
        st.warning('Table Gold flights_daily_stats indisponible : exécuter le notebook J3.')
        st.caption(str(exc))
        return
    if daily.empty:
        st.info('Aucune donnée quotidienne.'); return
    daily['flight_date'] = pd.to_datetime(daily['flight_date'])
    chosen = daily.loc[daily.flight_date.dt.date == day]
    if chosen.empty:
        st.info('Aucune donnée pour la date.'); return
    row = chosen.iloc[0]
    _render_super_bowl_context(get_spark, day)
    _render_weather_context(get_spark, day)
    _render_weather_association(get_spark)
    if day in EVENTS:
        st.markdown(
            '<div style="background:#151f30;border:1px solid #2a3547;'
            'border-left:5px solid #ffc933;border-radius:10px;'
            'padding:14px 18px;margin:10px 0 16px;'
            'color:#f1f5f9;font-size:1rem;font-weight:600;">'
            f'📅 Journée sélectionnée — {day:%d/%m/%Y} · {escape(EVENTS[day])}'
            '</div>',
            unsafe_allow_html=True,
        )
    st.markdown('#### Taux de retard à l’arrivée au fil de l’année')
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=daily.flight_date, y=daily.delay_rate, mode='lines',
                             name='Taux de retard', line={'color':'#ffc933','width':2},
                             hovertemplate='%{x|%d/%m/%Y}<br>Retards : %{y:.1f} %<extra></extra>'))
    events_on_data = daily[daily.flight_date.dt.date.isin(EVENTS)].copy()
    events_on_data['event_name'] = events_on_data.flight_date.dt.date.map(EVENTS)
    # Un seul libellé visible : le jour choisi. Les autres fêtes restent identifiables au survol.
    fig.add_trace(go.Scatter(x=events_on_data.flight_date, y=events_on_data.delay_rate,
                             mode='markers', name='Fêtes / événements',
                             marker={'color':'#4cc9f0', 'size':9, 'symbol':'diamond'},
                             text=events_on_data.event_name,
                             hovertemplate='<b>%{text}</b><br>%{x|%d/%m/%Y}<br>Retards : %{y:.1f} %<extra></extra>'))
    fig.add_vline(x=pd.Timestamp(day).timestamp()*1000, line_color='#ff6b6b', line_dash='dot', opacity=.85)
    fig.add_trace(go.Scatter(x=[pd.Timestamp(day)], y=[row.delay_rate], mode='markers',
                             marker={'color':'#ff6b6b','size':13}, name='Jour sélectionné',
                             hovertemplate=f'{day:%d/%m/%Y}<br>{row.delay_rate:.1f} %<extra></extra>'))
    fig.update_layout(xaxis_title='Date', yaxis_title='Vols en retard à l’arrivée (%)')
    _chart(fig, style_chart, 440)
    st.markdown('**📅 Événements repérés sur la courbe**')
    chips = []
    for event_day, name in EVENTS.items():
        selected = event_day == day
        border = '#ffc933' if selected else '#33506a'
        bg = '#263145' if selected else '#111d2e'
        chips.append(
            f'<span style="display:inline-block;background:{bg};'
            f'border:1px solid {border};border-radius:8px;'
            'padding:8px 11px;margin:0 7px 9px 0;'
            'color:#e8eef7;font-size:0.88rem;white-space:nowrap;">'
            f'<span style="color:#4cc9f0">◆</span> ' 
            f'{escape(name)} · {event_day:%d/%m/%Y}</span>'
        )
    st.markdown(''.join(chips), unsafe_allow_html=True)
    st.caption('Les données sont historiques (2015). Un écart observé lors d’une fête ne démontre pas une relation de causalité.')


def _airport_map(airport_day, routes_day, airport_code, style_chart, airport_names=None):
    fig = go.Figure()
    if airport_code != 'Tous' and not routes_day.empty:
        edges = routes_day[routes_day.origin_airport == airport_code].nlargest(25, 'scheduled_flights')
        for r in edges.itertuples():
            fig.add_trace(go.Scattergeo(
                lon=[r.origin_longitude,r.destination_longitude], lat=[r.origin_latitude,r.destination_latitude],
                mode='lines', line={'width':1.5, 'color':'#4cc9f0'}, opacity=.45,
                hoverinfo='skip',showlegend=False))
    airports = airport_day.dropna(subset=['latitude','longitude']).copy()
    if airports.empty:
        return None
    airports['size'] = ((airports.scheduled_flights.clip(lower=0)**.5)*.55+5).clip(upper=32)
    airports['color'] = airports.delay_rate.apply(
        lambda x: '#94a3b8' if pd.isna(x) else '#ff6b6b' if x >= 25 else '#ffc933' if x >= 15 else '#3ddc97')
    airport_names = airport_names or {}
    labels = [
        f'{airport_names.get(r.origin_airport, r.origin_airport)} ({r.origin_airport})'
        f'<br>Vols programmés : {r.scheduled_flights:,}'
        + (f'<br>Retards arrivée : {r.delay_rate:.1f} %'
           if pd.notna(r.delay_rate) else '<br>Retards : données insuffisantes')
        for r in airports.itertuples()
    ]
    fig.add_trace(go.Scattergeo(
        lon=airports.longitude,lat=airports.latitude, mode='markers',text=labels,hoverinfo='text',
        marker={'size':airports['size'],'color':airports['color'], 'opacity':.9,
                'line':{'width':1, 'color':'#f8fafc'}},showlegend=False))
    fig.update_geos(scope='usa',projection_type='albers usa',showland=True,landcolor='#17263b',
                    showocean=True,oceancolor='#0a0f18',showlakes=True,lakecolor='#0f2437',
                    showcountries=False,showsubunits=True,subunitcolor='#334155',bgcolor='rgba(0,0,0,0)')
    fig=style_chart(fig,560)
    fig.update_layout(title=None,margin={'l':0,'r':0,'t':6,'b':0},geo_bgcolor='rgba(0,0,0,0)')
    return fig


def render_map_tab(get_spark, style_chart):
    day = _date_state()
    st.subheader('🗺️ Carte des aéroports')
    st.caption('Sélectionne un aéroport pour afficher ses principales liaisons.')
    try:
        spark = get_spark()
        airport_day = _geo(spark, GOLD_AIRPORT, day)
        routes_day = _geo(spark, GOLD_ROUTES, day)
    except Exception as exc:
        st.warning('Tables géographiques indisponibles. Exécuter build_gold_dashboard.py.')
        st.caption(str(exc));return
    if airport_day.empty:
        st.info('Aucun aéroport disponible pour cette journée.');return
    airports = sorted(airport_day.origin_airport.dropna().unique().tolist())
    airport_names = _airport_names(spark)
    map_labels = {
        code: f'{airport_names.get(code, code)} ({code})'
        for code in airports
    }
    map_col, summary_col = st.columns([1.7, 1])
    with summary_col:
        airport_code = st.selectbox(
            'Aéroport à détailler',
            ['Tous', *airports],
            format_func=lambda code: 'Tous les aéroports' if code == 'Tous' else map_labels.get(code, code),
            key='ext_map_airport',
        )
        selected_airports = airport_day if airport_code == 'Tous' else airport_day[
            airport_day.origin_airport == airport_code
        ]
        if airport_code != 'Tous':
            st.caption(f'{airport_names.get(airport_code, airport_code)} ({airport_code})')
        scheduled = int(selected_airports.scheduled_flights.sum())
        delay_rate = selected_airports.delay_rate.mean()
        st.metric('Vols programmés', f'{scheduled:,}'.replace(',', ' '))
        st.metric(
            'Retards à l’arrivée',
            'Données indisponibles' if pd.isna(delay_rate) else f'{delay_rate:.1f} %',
        )
        st.caption('🟢 < 15 % · 🟡 15–25 % · 🔴 ≥ 25 % · ⚪ données indisponibles')
    with map_col:
        fig = _airport_map(airport_day, routes_day, airport_code, style_chart, airport_names)
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
    st.caption('Taille des cercles = nombre de vols programmés. Les liaisons sont schématiques ; aucun avion n’est suivi en temps réel.')


def render_traffic_tab(get_spark, style_chart):
    day = _date_state()
    st.subheader('🕒 Trafic et retards par heure')
    st.caption(f'Données du {day:%d/%m/%Y} · retards au départ (et non à l’arrivée).')
    try:
        spark=get_spark()
        airports=_geo(spark,GOLD_AIRPORT,day)
        airport_codes=sorted(airports.origin_airport.dropna().unique().tolist()) if not airports.empty else []
        airport_names=_airport_names(spark)
        traffic_labels={
            code: f'{airport_names.get(code, code)} ({code})'
            for code in airport_codes
        }
        # Un nouveau nom de clé évite de conserver l'ancienne sélection ABQ de la V4.
        selected=st.selectbox(
            'Périmètre',
            ['Tous les aéroports',*airport_codes],
            format_func=lambda code: 'Tous les aéroports' if code == 'Tous les aéroports' else traffic_labels.get(code, code),
            key='ext_hour_airport_v5',
        )
        data=_hours(spark,day,selected)
    except Exception as exc:
        st.warning('Statistiques horaires indisponibles : vérifier la table Gold flights_airport_traffic_delays.')
        st.caption(str(exc));return
    if data.empty:
        st.info('Aucun créneau horaire pour la sélection.');return

    if selected != 'Tous les aéroports':
        selected_name = airport_names.get(selected, selected)
        st.caption(f'Analyse de : {selected_name} ({selected})')
    data['scheduled_hour']=data['scheduled_hour'].astype(int)
    data['delay_rate']=100*data['delayed']/data['observed'].where(data['observed']>0)
    # Un pourcentage avec moins de 10 observations est trop instable pour être interprété.
    data['rate_reliable']=data['delay_rate'].where(data['observed']>=10)
    excluded_hours = int((data['observed']<10).sum())
    if excluded_hours:
        st.markdown(
            '<div style="background:#151f30;border:1px solid #2a3547;'
            'border-left:5px solid #ffc933;border-radius:10px;'
            'padding:12px 16px;margin:10px 0 16px;'
            'color:#f1f5f9;font-size:.92rem;">'
            f'ℹ️ <strong>{excluded_hours} créneau(x) horaire(s)</strong> comptent moins de ' 
            '10 vols avec retard au départ connu : leur taux n’est pas affiché ' 
            'pour éviter une interprétation trompeuse.'
            '</div>', unsafe_allow_html=True
        )
    total_scheduled=int(data.scheduled.sum())
    total_observed=int(data.observed.sum())
    c1,c2=st.columns(2)
    c1.metric('Vols programmés sur la journée',f'{total_scheduled:,}'.replace(',',' '))
    c2.metric('Vols avec retard de départ connu',f'{total_observed:,}'.replace(',',' '))
    left,right=st.columns(2)
    with left:
        st.markdown('#### Nombre de départs programmés par heure')
        fig=go.Figure(go.Bar(x=data.scheduled_hour,y=data.scheduled,marker_color='#4cc9f0',
                             hovertemplate='Heure : %{x} h<br>Vols : %{y:,}<extra></extra>'))
        fig.update_layout(xaxis={'title':'Heure de départ prévue','dtick':1},yaxis_title='Vols programmés',showlegend=False)
        _chart(fig,style_chart,390)
    with right:
        st.markdown('#### Part des vols retardés au départ')
        if data['rate_reliable'].notna().sum() == 0:
            st.markdown(
                '<div style="min-height:330px;display:flex;flex-direction:column;'
                'justify-content:center;align-items:center;text-align:center;'
                'padding:25px;background:#101827;border:1px solid #2a3547;'
                'border-radius:12px;color:#f1f5f9;">'
                '<span style="font-size:1.3rem;font-weight:700;color:#ffc933;">'
                'Données insuffisantes</span><br>'
                'Aucune heure ne comporte au moins 10 vols observés.<br>'
                '<span style="color:#c4d0e0;">'
                'Sélectionnez « Tous les aéroports » ou un aéroport plus fréquenté.'
                '</span></div>', unsafe_allow_html=True
            )
        else:
            fig=go.Figure(go.Scatter(x=data.scheduled_hour,y=data.rate_reliable,mode='lines+markers',
                                     line={'color':'#ffc933','width':2},
                                     hovertemplate='Heure : %{x} h<br>Retards : %{y:.1f} %<extra></extra>'))
            fig.update_layout(xaxis={'title':'Heure de départ prévue','dtick':1},
                              yaxis={'title':'Vols retardés (%)','range':[0,100]},showlegend=False)
            _chart(fig,style_chart,390)
    st.caption('Exemple de lecture : une barre haute signifie beaucoup de vols programmés à cette heure ; '
               'un point haut signifie qu’une grande proportion des vols observés sont partis avec au moins 15 minutes de retard. '
               'La hausse de l’un n’implique pas forcément celle de l’autre.')
