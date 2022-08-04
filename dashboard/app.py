from dash import Dash, dcc, html, dependencies
import plotly.express as px
import pandas as pd

from data_loader import DataLoader

app = Dash(__name__)

colors = {
    'background': '#f0e9e9',
    'text': '#120101'
}

data_loader = DataLoader(6)


@app.callback(
    dependencies.Output('prices-over-time', 'figure'),
    [dependencies.Input('item-select-dropdown', 'value')]
)
def create_line_plot(item_names=None):
    df = data_loader.prices
    if item_names is not None:
        item_key = data_loader.item_id_key
        item_ids = list(item_key[item_key["name"].isin(item_names)]["item_id"])
        df = data_loader.filter_time_series(
            item_ids=item_ids
        )

    fig = px.line(df, x="datetime", y="price", color='name')
    fig.update_layout(
        plot_bgcolor=colors['background'],
        paper_bgcolor=colors['background'],
        font_color=colors['text']
    )
    return fig


app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    html.H1(
        children='Grand Exchange Price Prediction',
        style={
            'textAlign': 'center',
            'color': colors['text']
        }
    ),

    html.Div(children='A deep learning price prediction model.', style={
        'textAlign': 'center',
        'color': colors['text']
    }),

    dcc.Dropdown(
        options=sorted(list(data_loader.item_id_key["name"].unique())),
        value=["Cannonball"],
        multi=True,
        id="item-select-dropdown"
    ),

    dcc.Graph(
        id='prices-over-time'
    )
])


if __name__ == '__main__':
    app.run_server(debug=True)
