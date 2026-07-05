import pytest

from tui import PipelineApp


@pytest.mark.asyncio
async def test_app_title():
    app = PipelineApp()
    async with app.run_test():
        assert app.title == "SmartMoney Pipeline Manager"

@pytest.mark.asyncio
async def test_widgets_present():
    from tui import PipelineApp
    app = PipelineApp()
    async with app.run_test():
        assert app.query_one("#status-dashboard") is not None
        assert app.query_one("#operations") is not None
        assert app.query_one("#scraping-progress") is not None
        assert app.query_one("#live-logs") is not None

