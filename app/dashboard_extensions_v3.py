"""Exploration quotidienne : un onglet unique, une date partagée et trois analyses.
Ne modifie aucune table Gold ni aucun des quatre onglets historiques.
"""
from datetime import date
from html import escape
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

GOLD_DAILY = '/lakehouse/gold/flights_daily_stats'
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

def _date_state():
    if 'ext_selected_date' not in st.session_state:
        st.session_state.ext_selected_date = date(2015, 11, 26)
    return st.session_state.ext_selected_date

def _event_changed():
    """Callback appelé AVANT la reconstruction des widgets Streamlit."""
    label = st.session_state.get('ext_event_choice', '')
    for day, name in EVENTS.items():
        if label == f'{name} — {day:%d/%m/%Y}':
            st.session_state.ext_selected_date = day
            st.session_state.ext_calendar = day
            return


def _calendar_changed():
    day = st.session_state.ext_calendar
    st.session_state.ext_selected_date = day
    st.session_state.ext_event_choice = (
        f'{EVENTS[day]} — {day:%d/%m/%Y}'
        if day in EVENTS else 'Choisir librement une journée'
    )


def render_exploration_tab(get_spark, style_chart):
    """Point d'entrée unique : les trois analyses partagent le même calendrier."""
    st.subheader('📅 Exploration quotidienne des vols — 2015')
    st.caption('Choisissez une date une seule fois : tous les graphiques ci-dessous se mettent à jour.')
    _date_state()
    if 'ext_calendar' not in st.session_state:
        st.session_state.ext_calendar = st.session_state.ext_selected_date
    if 'ext_event_choice' not in st.session_state:
        day = st.session_state.ext_selected_date
        st.session_state.ext_event_choice = (
            f'{EVENTS[day]} — {day:%d/%m/%Y}' if day in EVENTS
            else 'Choisir librement une journée'
        )
    event_choices = ['Choisir librement une journée'] + [
        f'{name} — {day:%d/%m/%Y}' for day, name in EVENTS.items()
    ]
    c1, c2 = st.columns(2)
    with c1:
        st.selectbox('Accéder directement à un événement', event_choices,
                     key='ext_event_choice', on_change=_event_changed)
    with c2:
        st.date_input('Choisir une journée', min_value=date(2015, 1, 1),
                      max_value=date(2015, 12, 31), format='DD/MM/YYYY',
                      key='ext_calendar', on_change=_calendar_changed)
    day = _date_state()
    st.markdown(f'### ✈️ Journée du {day:%d/%m/%Y}' +
                (f' — {EVENTS[day]}' if day in EVENTS else ''))
    st.divider()
    render_events_tab(get_spark, style_chart)
    st.divider()
    with st.expander(f'🗺️ Carte des aéroports — {day:%d/%m/%Y}', expanded=True):
        render_map_tab(get_spark, style_chart)
    st.divider()
    with st.expander(f'🕒 Trafic et retards horaires — {day:%d/%m/%Y}', expanded=True):
        render_traffic_tab(get_spark, style_chart)


def _chart(fig, style_chart, height=390):
    fig = style_chart(fig, height)
    fig.update_layout(title=None, margin={'l': 30, 'r': 20, 't': 30, 'b': 35})
    st.plotly_chart(fig, use_container_width=True)

def render_events_tab(get_spark, style_chart):
    day = _date_state()
    st.subheader(f'Événements et ponctualité — {day:%d/%m/%Y}')
    st.caption('Les losanges bleus signalent les événements. Leurs noms apparaissent au survol et dans la liste visible sous le graphique.')
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
    c1,c2,c3,c4 = st.columns(4)
    c1.metric('Vols programmés', f'{int(row.scheduled_flights):,}'.replace(',', ' '))
    c2.metric('Retards à l’arrivée ≥ 15 min', f'{row.delay_rate:.1f} %')
    c3.metric('Annulations', f'{row.cancellation_rate:.2f} %')
    c4.metric('Retard médian à l’arrivée', f'{int(row.median_arrival_delay)} min')
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


def _airport_map(airport_day, routes_day, airport_code, style_chart):
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
    labels = [f'{r.origin_airport}<br>Vols programmés : {r.scheduled_flights:,}' +
              (f'<br>Retards arrivée : {r.delay_rate:.1f} %' if pd.notna(r.delay_rate) else '<br>Retards : données insuffisantes')
              for r in airports.itertuples()]
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
    st.markdown(f'#### Carte des aéroports — {day:%d/%m/%Y}')
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
    airport_code = st.selectbox('Afficher les principales liaisons au départ de', ['Tous',*airports],key='ext_map_airport')
    st.markdown('**Ponctualité à l’arrivée :** 🟢 < 15 % de retards · 🟡 15–25 % · 🔴 ≥ 25 % · ⚪ données indisponibles')
    fig=_airport_map(airport_day,routes_day,airport_code,style_chart)
    if fig is not None:
        st.plotly_chart(fig,use_container_width=True)
    st.caption('Taille des cercles = nombre de vols programmés. Les liaisons sont schématiques ; aucun avion n’est suivi en temps réel.')


def render_traffic_tab(get_spark, style_chart):
    day = _date_state()
    st.markdown(f'#### Trafic et retards par heure — {day:%d/%m/%Y}')
    st.caption(f'Données du {day:%d/%m/%Y} — retards au départ (et non à l’arrivée).')
    try:
        spark=get_spark()
        airports=_geo(spark,GOLD_AIRPORT,day)
        airport_codes=sorted(airports.origin_airport.dropna().unique().tolist()) if not airports.empty else []
        # Un nouveau nom de clé évite de conserver l'ancienne sélection ABQ de la V4.
        selected=st.selectbox('Périmètre', ['Tous les aéroports',*airport_codes],key='ext_hour_airport_v5')
        data=_hours(spark,day,selected)
    except Exception as exc:
        st.warning('Statistiques horaires indisponibles : vérifier la table Gold flights_airport_traffic_delays.')
        st.caption(str(exc));return
    if data.empty:
        st.info('Aucun créneau horaire pour la sélection.');return
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
