import os
from datetime import datetime

import altair as alt
import pandas as pd
import streamlit as st
from dateutil.relativedelta import relativedelta

from dbt_meta import DbtProject
from nelson_rules import generate_nelson_rules_sql, RULES

st.set_page_config(layout="wide", page_title='Otto')

PROJECT_DIR = os.environ['PROJECT_DIR']


@st.experimental_singleton
def get_project(project_dir):
    return DbtProject(project_dir)


def main():
    project = get_project(PROJECT_DIR)
    metrics = project.get_metrics()

    @st.experimental_memo(ttl=600)
    def run_query(sql_query: str):
        with project.connection.handle.cursor() as cursor:
            cursor.execute(sql_query)
            return pd.DataFrame.from_records(iter(cursor), columns=[x[0] for x in cursor.description])

    selected_metric = st.selectbox('Metric', metrics.keys(), format_func=lambda x: metrics[x].label)
    if selected_metric:
        m = metrics[selected_metric]

        col1, col2 = st.columns(2)
        with col1:
            selected_grain = st.selectbox('Metric', m.time_grains)
            selected_dimensions = st.multiselect('Dimensions', m.dimensions)
            has_dimensions = len(selected_dimensions) > 0
            limit = 10
            if has_dimensions:
                limit = st.number_input('Number of series', min_value=1, max_value=100, value=limit)
        with col2:
            start_date = st.date_input("Date from", value=(datetime.now() - relativedelta(years=1)).date())
            end_date = st.date_input("Date to")

    if selected_metric:
        metric_name = metrics[selected_metric].name

        raw_sql = generate_nelson_rules_sql(
            metric_name,
            selected_grain,
            str(start_date),
            str(end_date),
            selected_dimensions,
            limit=limit
        )

        with st.expander("Raw SQL"):
            st.write(f'```jinja2\n{raw_sql}')

        compiled_sql = project.compile_sql(
            sql=raw_sql,
            depends_on=[]
        )

        with st.expander("Compiled SQL"):
            compiled_sql = '\n'.join([s for s in compiled_sql.splitlines() if s.strip()])
            st.write(f'```sql\n{compiled_sql}')

        with st.spinner('Loading data...'):
            df = run_query(compiled_sql)

        with st.expander("Metric data"):
            st.dataframe(df)

    plot_args = {
        "x": "TS",
        "y": "VALUE"
    }
    if has_dimensions:
        plot_args['color'] = "DIMENSION"
    charts = alt.Chart(df).mark_line(
        color='green'
    ).encode(**plot_args).properties(
        height=500
    )

    for i in range(len(RULES)):
        rule_number = i + 1
        rule_column = f'VIOLATE_RULE_{rule_number}'
        rule_name = f'Rule {rule_number}'
        if rule_column in df:
            rule_description = RULES[i]

            has_violation = len(df[df[rule_column] == 1]) > 0
            checked = st.checkbox(f'{rule_name} - {rule_description}', disabled=not has_violation,
                                  value=has_violation)
            annotations_df = df[df[f'VIOLATE_RULE_{rule_number}'] == 1]
            annotations_df['violate'] = f'Rule {rule_number}'

            if checked:
                annotation_layer = (
                    alt.Chart(annotations_df)
                    .mark_text(size=20, text="⬇", dx=0, dy=-15, align="center", color='red')
                    .encode(
                        x="TS",
                        y=alt.Y("VALUE:Q"),
                        tooltip=["violate"],
                    )
                    .interactive()
                )
                charts += annotation_layer

    st.altair_chart(charts, use_container_width=True)


if __name__ == '__main__':
    main()
