import pydeck as pdk
import pandas as pd


def _color_interpolate(val, vmin, vmax, colorscale):
    if vmin == vmax:
        norm = 0.5
    else:
        norm = (val - vmin) / (vmax - vmin)
    norm = max(0.0, min(1.0, norm))

    n = len(colorscale) - 1
    idx = norm * n
    lo = int(idx)
    hi = min(lo + 1, n)
    frac = idx - lo

    c_lo = colorscale[lo]
    c_hi = colorscale[hi]
    r = int(c_lo[0] + (c_hi[0] - c_lo[0]) * frac)
    g = int(c_lo[1] + (c_hi[1] - c_lo[1]) * frac)
    b = int(c_lo[2] + (c_hi[2] - c_lo[2]) * frac)
    return [r, g, b, 200]


def _hex_to_rgb(hex_str):
    hex_str = hex_str.lstrip("#")
    return [int(hex_str[i:i+2], 16) for i in (0, 2, 4)]


COORDS_COLOMBIA = {
    "Bogota": (4.6097, -74.0817),
    "Medellin": (6.2442, -75.5812),
    "Cali": (3.4516, -76.5320),
    "Barranquilla": (10.9685, -74.7813),
    "Cartagena": (10.3997, -75.5144),
    "Bucaramanga": (7.1254, -73.1198),
    "Cucuta": (7.8939, -72.5078),
    "Manizales": (5.0689, -75.5174),
    "Pereira": (4.8133, -75.6961),
    "Ibague": (4.4389, -75.2322),
    "Villavicencio": (4.1510, -73.6346),
    "Santa Marta": (11.2408, -74.1990),
    "Neiva": (2.9273, -75.2819),
    "Pasto": (1.2892, -77.3579),
    "Armenia": (4.5339, -75.6811),
    "Monteria": (8.7479, -75.8814),
    "Sincelejo": (9.3017, -75.3972),
    "Valledupar": (10.4631, -73.2532),
    "Popayan": (2.4382, -76.6131),
    "Tunja": (5.5353, -73.3678),
    "Riohacha": (11.5444, -72.9072),
    "Florencia": (1.6144, -75.6062),
    "Quibdo": (5.6919, -76.6583),
    "Yopal": (5.3378, -72.3959),
}


def colombia_scatter_map(
    df,
    lat_col="lat",
    lon_col="lon",
    size_col="n",
    color_col="n",
    text_col="city",
    hover_col="hover",
    colorscale=None,
    colorbar_title="Volumen",
    height=500,
    size_max=35,
    key=None,
):
    if colorscale is None:
        colorscale_rgb = [[227, 242, 253], [41, 181, 232], [17, 86, 127]]
    else:
        colorscale_rgb = [_hex_to_rgb(c) if isinstance(c, str) else c for c in colorscale]

    work = df.copy()
    work["_lat"] = work[lat_col].astype(float)
    work["_lon"] = work[lon_col].astype(float)
    work["_size"] = work[size_col].astype(float)
    work["_color_val"] = work[color_col].astype(float)
    work["_text"] = work[text_col].astype(str)

    vmin = float(work["_color_val"].min())
    vmax = float(work["_color_val"].max())
    work["_color"] = work["_color_val"].apply(
        lambda v: _color_interpolate(v, vmin, vmax, colorscale_rgb)
    )

    smin = float(work["_size"].min())
    smax = float(work["_size"].max())
    if smin == smax:
        work["_radius"] = 25000
    else:
        work["_radius"] = work["_size"].apply(
            lambda s: 8000 + (50000 - 8000) * ((s - smin) / (smax - smin))
        )

    if hover_col and hover_col in work.columns:
        tooltip_html = "<b>{_text}</b><br/>{" + hover_col + "}"
    else:
        tooltip_html = "<b>{_text}</b>"

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=work,
        get_position=["_lon", "_lat"],
        get_radius="_radius",
        get_fill_color="_color",
        pickable=True,
        opacity=0.85,
    )

    text_layer = pdk.Layer(
        "TextLayer",
        data=work,
        get_position=["_lon", "_lat"],
        get_text="_text",
        get_size=12,
        get_color=[17, 86, 127, 255],
        get_angle=0,
        get_text_anchor='"middle"',
        get_alignment_baseline='"bottom"',
        get_pixel_offset=[0, -20],
    )

    view_state = pdk.ViewState(
        latitude=4.5,
        longitude=-74.0,
        zoom=5,
        pitch=0,
        height=height,
    )

    tooltip = {
        "html": tooltip_html,
        "style": {
            "backgroundColor": "#11567F",
            "color": "white",
            "fontSize": "12px",
            "padding": "8px",
        },
    }

    deck = pdk.Deck(
        map_provider="carto",
        map_style="light",
        layers=[layer, text_layer],
        initial_view_state=view_state,
        tooltip=tooltip,
    )

    return deck
