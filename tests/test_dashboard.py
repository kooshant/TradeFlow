
from pathlib import Path

from streamlit.testing.v1 import AppTest


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DASHBOARD_PATH = (
    PROJECT_ROOT
    / "src"
    / "tradeflow"
    / "dashboard.py"
)


def test_dashboard_loads_successfully():

    app = AppTest.from_file(
        str(DASHBOARD_PATH),
        default_timeout=60
    ).run()

    # Check for application errors
    assert len(app.exception) == 0, [
        error.message for error in app.exception
    ]

    # Check expected dashboard KPIs
    metric_labels = {
        metric.label for metric in app.metric
    }

    expected_metrics = {
        "TOTAL EXPORTS · FOB",
        "TOTAL IMPORTS · CIF",
        "EXPORT RECORDS",
        "IMPORT RECORDS",
        "NUMBER OF IMPORT PARTNERS",
        "TOP 5 PARTNER SHARE",
    }

    assert expected_metrics.issubset(metric_labels), (
        f"Missing metrics: {expected_metrics - metric_labels}"
    )
