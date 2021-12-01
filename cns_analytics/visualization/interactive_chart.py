import os
import sys
import token
import tokenize
from io import BytesIO
from token import NAME, OP, STRING
from typing import Optional

from bokeh.application import Application
from bokeh.application.handlers import FunctionHandler
from bokeh.server.server import Server

sys.path.insert(0, os.path.abspath("."))

import asyncio
import nest_asyncio

from bokeh.models import TextInput
from bokeh.layouts import column, row
from bokeh.plotting import figure
from bokeh.models import CustomJS, CrosshairTool, Button, ColumnDataSource, DatetimeTickFormatter, \
    Div, ColorPicker

from cns_analytics import Exchange, TimeSeries, DataBase, utils

nest_asyncio.apply()


def add_vlinked_crosshairs(*figs):
    js_leave = ''
    js_move = 'if(cb_obj.x >= fig.x_range.start && cb_obj.x <= fig.x_range.end &&\n'
    js_move += 'cb_obj.y >= fig.y_range.start && cb_obj.y <= fig.y_range.end){\n'
    for i in range(len(figs)-1):
        js_move += '\t\t\tother%d.spans.height.computed_location = cb_obj.sx\n' % i
    js_move += '}else{\n'
    for i in range(len(figs)-1):
        js_move += '\t\t\tother%d.spans.height.computed_location = null\n' % i
        js_leave += '\t\t\tother%d.spans.height.computed_location = null\n' % i
    js_move += '}'
    crosses = [CrosshairTool() for fig in figs]
    for i, fig in enumerate(figs):
        fig.add_tools(crosses[i])
        args = {'fig': fig}
        k = 0
        for j in range(len(figs)):
            if i != j:
                args['other%d'%k] = crosses[j]
                k += 1
        fig.js_on_event('mousemove', CustomJS(args=args, code=js_move))
        fig.js_on_event('mouseleave', CustomJS(args=args, code=js_leave))


MAX_CHARTS = 5


ts: Optional[TimeSeries] = None


async def load_md(symbols):
    global ts
    DataBase.set_default_exchange(Exchange.Finam)

    ts = TimeSeries(*symbols)
    try:
        await ts.load(resolution='1m')
    except Exception:
        return
    ts.resample('1h')
    df = ts.get_raw_df()
    df['index'] = df.index


loop = asyncio.get_event_loop()


def wait_for_md(symbols):
    loop.run_until_complete(load_md(symbols))
    return ts.get_raw_df()


wait_for_md({'RTS'})


def my_eval(code_str, context=None):
    context = context or {}

    code = list(tokenize.tokenize(BytesIO(code_str.encode('utf-8')).readline))
    new_code = []

    symbols = set()

    for idx, (toknum, tokval, _, _, _) in enumerate(code):
        if toknum == NAME:
            if tokval == 'df':
                symbol = code[idx + 2][1][1:-1]
                symbols.add(symbol)
                new_code.append((toknum, tokval))
            else:
                if toknum not in context:
                    symbols.add(tokval)

                new_code.extend([
                    (NAME, 'df'),
                    (OP, '['),
                    (STRING, repr(tokval)),
                    (OP, ']')
                ])
        else:
            new_code.append((toknum, tokval))

    data = wait_for_md(symbols)

    new_code_str = tokenize.untokenize(new_code).decode('utf-8')

    return eval(new_code_str, context, {'df': data})


class Formula:
    def __init__(self, idx, color, line):
        self.input = TextInput(placeholder="Formula", value="")
        self.input.sizing_mode = 'scale_width'
        self.input.on_change('value', self._on_text_changed)
        self.text = ""
        self.formula_stack = None
        self.idx = idx
        self.color = color
        self.line = line

    def set_text(self, value):
        self.text = value
        self.input.value = value

    def _on_text_changed(self, attr, old, new):
        if attr == 'value':
            self.text = new
            # select_idx(self)


DATA = [None, None, None, None, None]
SELECTED_FORMULA = None
color_picker = None
source = None
formulas = None


def select_idx(formula):
    global SELECTED_FORMULA
    SELECTED_FORMULA = formula
    if color_picker is not None:
        color_picker.color = formula.color


def get_input_row(x):
    btn = Button(label='⚙', button_type="primary", width=30, css_classes=['btn-input-settings'])
    btn.on_click(lambda: select_idx(x))

    input_row = row(
        x.input,
        btn
    )

    return input_row


def on_change_color(attr, old, new):
    if SELECTED_FORMULA is None:
        return

    if attr == 'color':
        SELECTED_FORMULA.line.glyph.line_color = new
        SELECTED_FORMULA.color = new


def on_ols_regress():
    reload_chart_data(None)

    if SELECTED_FORMULA is None or SELECTED_FORMULA.idx == 0:
        return

    l1 = DATA[0].ffill()
    l2 = DATA[SELECTED_FORMULA.idx].ffill()

    if l1 is None or l2 is None:
        return

    coef = round(utils.get_ols_regression(l1, l2)[1], 3)

    code = list(tokenize.tokenize(BytesIO(SELECTED_FORMULA.text.encode('utf-8')).readline))

    if code[1].type == token.OP and code[1].string == '(' and \
            code[-5].type == token.OP and code[-5].string == ')' and \
            code[-4].type == token.OP and code[-4].string == '*' and \
            code[-3].type == token.NUMBER:
        code[-3] = (token.NUMBER, ' ' + str(round(coef * float(code[-3].string), 3)))
        SELECTED_FORMULA.set_text(tokenize.untokenize(code).decode('utf-8'))
    else:
        SELECTED_FORMULA.set_text(f"({SELECTED_FORMULA.text}) * {coef}")

    reload_chart_data(None)


def on_mean_regress():
    reload_chart_data(None)

    if SELECTED_FORMULA is None or SELECTED_FORMULA.idx == 0:
        return

    l1 = DATA[0].ffill()
    l2 = DATA[SELECTED_FORMULA.idx].ffill()

    if l1 is None or l2 is None:
        return

    coef = round(utils.get_mean_regression(l1, l2), 3)

    code = list(tokenize.tokenize(BytesIO(SELECTED_FORMULA.text.encode('utf-8')).readline))

    if code[1].type == token.OP and code[1].string == '(' and \
            code[-5].type == token.OP and code[-5].string == ')' and \
            code[-4].type == token.OP and code[-4].string == '*' and \
            code[-3].type == token.NUMBER:
        code[-3] = (token.NUMBER, ' ' + str(round(coef * float(code[-3].string), 3)))
        SELECTED_FORMULA.set_text(tokenize.untokenize(code).decode('utf-8'))
    else:
        SELECTED_FORMULA.set_text(f"({SELECTED_FORMULA.text}) * {coef}")

    reload_chart_data(None)


def reload_chart_data(event):
    values = [
        x.text for x in formulas
    ]

    df = ts.get_raw_df()
    for i, value in enumerate(values):
        key = f'_FORMULA_{i+1}'
        if not value:
            try:
                del df[key]
                DATA[i] = None
            except KeyError:
                pass
        else:
            df[key] = my_eval(value)
            DATA[i] = df[key]

    source.data = df


def main(doc):
    global source, formulas, color_picker, SELECTED_FORMULA

    fig = figure(tools='pan,xwheel_zoom,box_zoom,reset', active_scroll='xwheel_zoom')
    name = 'RTS'
    data = ts.get_raw_df()
    data['_FORMULA_1'] = data[name]
    DATA[0] = data[name]
    source = ColumnDataSource(data=data)

    colors = [
        '#F4A261',
        '#005F73',
        '#2A9D8F',
        '#E9C46A',
        '#E76F51',
    ]

    fig.sizing_mode = 'stretch_both'
    fig.syncable = False
    fig.xaxis.formatter = DatetimeTickFormatter(
        hours=["%d %b %Y"],
        days=["%d %b %Y"],
        months=["%d %b %Y"],
        years=["%d %b %Y"])

    formulas = []

    for idx in range(MAX_CHARTS):
        line = fig.line(y=f'_FORMULA_{idx + 1}', x='index', source=source, color=colors[idx])
        formulas.append(Formula(idx, color=colors[idx], line=line))

    formulas[0].set_text('RTS')

    for formula in formulas:
        formula.formula_stack = formulas

    button_reload = Button(label="Reload", button_type="success")
    button_reload.on_click(reload_chart_data)
    button_reload.js_on_click(CustomJS(args=dict(fig=fig), code="""
        fig.reset.emit()
    """))

    add_vlinked_crosshairs(fig)

    SELECTED_FORMULA = formulas[0]
    color_picker = ColorPicker(name='Color', color=colors[0])
    color_picker.on_change('color', on_change_color)

    ols_regress = Button(label='OLS Regress', button_type="primary")
    ols_regress.on_click(on_ols_regress)
    mean_regress = Button(label='Mean Regress', button_type="primary")
    mean_regress.on_click(on_mean_regress)

    row1 = row(
        fig,
        column(
            *[
                get_input_row(x)
                for x in formulas
            ],
            button_reload,
            Div(style={
                    'border': '1px solid black',
                    'margin-bottom': '30px',
                    'margin-top': '10px',
                    'width': '100%'
                }, sizing_mode='stretch_width'),
            ols_regress,
            mean_regress,
            color_picker,
            sizing_mode='stretch_width'
        ),
        sizing_mode='stretch_both'
    )

    row1.cols = {
        0: 'max',
        1: 'min',
    }

    # header = Div(text="""
    #     <style>
    #         .btn-input-settings .bk {
    #             border-bottom-left-radius: 0;
    #             border-top-left-radius: 0;
    #         }
    #     </style>
    # """, height=0)

    layout = row(
        column(
            row1,
            # header,
            sizing_mode='stretch_both'
        ),
        sizing_mode='stretch_both'
    )

    doc.add_root(layout)


apps = {'/': Application(FunctionHandler(main))}

server = Server(apps, port=5000)
server.start()
server.run_until_shutdown()

# EWJ*25 - RSX*50