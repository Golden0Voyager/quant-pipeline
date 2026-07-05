import pytest
from tui import PipelineApp

@pytest.mark.asyncio
async def test_app_title():
    app = PipelineApp()
    async with app.run_test() as pilot:
        assert app.title == "SmartMoney Pipeline Manager"
