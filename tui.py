from textual.app import App, ComposeResult
from textual.containers import Grid
from textual.widgets import Footer, Header, Static


class DashboardWidget(Static):
    pass

class OperationsWidget(Static):
    pass

class ProgressWidget(Static):
    pass

class LogsWidget(Static):
    pass

class PipelineApp(App):
    TITLE = "SmartMoney Pipeline Manager"
    CSS = """
    $cyan: #00ffff;
    $green: #00ff00;
    $magenta: #ff00ff;
    $yellow: #ffff00;
    $panel: #1e1e1e;

    Screen {
        background: #121212;
    }
    #main-grid {
        layout: grid;
        grid-size: 2 3;
        grid-rows: 1fr 1fr 1fr;
        grid-columns: 1fr 1fr;
        height: 100%;
        padding: 1;
    }
    #status-dashboard {
        border: double $cyan;
        background: $panel;
        padding: 1;
    }
    #operations {
        border: double $green;
        background: $panel;
        padding: 1;
    }
    #scraping-progress {
        border: double $magenta;
        background: $panel;
        padding: 1;
    }
    #live-logs {
        border: double $yellow;
        background: $panel;
        row-span: 3;
        padding: 1;
    }
    """


    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Grid(id="main-grid"):
            yield DashboardWidget("Dashboard", id="status-dashboard")
            yield LogsWidget("Live Logs", id="live-logs")
            yield OperationsWidget("Operations", id="operations")
            yield ProgressWidget("Progress", id="scraping-progress")
        yield Footer()

if __name__ == "__main__":
    app = PipelineApp()
    app.run()

