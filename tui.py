from textual.app import App, ComposeResult
from textual.widgets import Header, Footer

class PipelineApp(App):
    TITLE = "SmartMoney Pipeline Manager"
    CSS = """
    Screen {
        background: #121212;
    }
    """

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Footer()

if __name__ == "__main__":
    app = PipelineApp()
    app.run()
