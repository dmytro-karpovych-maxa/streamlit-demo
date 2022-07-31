RULES = [
    'One point is more than 3 standard deviations from the mean.',
    'Nine (or more) points in a row are on the same side of the mean.',
    'Six (or more) points in a row are continually increasing (or decreasing).',
    'Fourteen (or more) points in a row alternate in direction, increasing then decreasing.',
    'Two out of three points in a row are more than 2 standard deviations from the mean in the same direction.',
    'Four out of five points in a row are more than 1 standard deviation from the mean in the same direction.',
    'Fifteen points in a row are all within 1 standard deviation of the mean on either side of the mean.',
    'Eight points in a row exist, but none within 1 standard deviation of the mean, '
    'and the points are in both directions from the mean.',
]


def generate_nelson_rules_sql(metric_name, grain, start_date, end_date, dimensions, limit=10):
    dimensions = ', '.join([f"'{d}'" for d in dimensions])

    macros = """nelson_rules.nelson_rules(
    table_name=metrics.calculate(
        metric('{metric_name}'),
        grain='{grain}',
        dimensions=[{dimensions}],
        secondary_calculations=[
            metrics.period_over_period(comparison_strategy="ratio", interval=1, alias="pop_1mo"),
            metrics.period_over_period(comparison_strategy="difference", interval=1),
            metrics.period_over_period(comparison_strategy="ratio", interval=12, alias="pop_12mo_yoy_pct"),
            metrics.period_over_period(comparison_strategy="difference", interval=12, alias="pop_12mo_yoy_diff"),
            metrics.rolling(aggregate="sum", interval=12, alias="last_12_mo")
        ],
        start_date='{start_date}',
        end_date='{end_date}'
    ),
    timestamp_column='date_{grain}',
    value_column='{metric_name}',
    dimensions=[{dimensions}]
)""".format(
        metric_name=metric_name,
        grain=grain,
        dimensions=dimensions,
        start_date=start_date,
        end_date=end_date,
    )

    sql = """select * 
from ({macros})
where ordering <= {limit}
    """.format(
        macros="{{" + macros + "}}",
        limit=limit,
    )

    return sql
