import pytest

from artigraph.api.run import RunContext, current_run_context
from artigraph.orm.run import Run
from tests.test_api.test_artifact_model import SimpleArtifactModel


async def test_run_context():
    """Test the run context."""
    run = Run(None)
    assert current_run_context(allow_none=True) is None
    async with RunContext(run) as run_context:
        assert current_run_context() is run_context
    assert current_run_context(allow_none=True) is None


async def test_run_context_nested():
    """Test the run context."""
    assert current_run_context(allow_none=True) is None
    run1 = Run(None)
    async with RunContext(run1) as run_context1:
        assert current_run_context() is run_context1
        run2 = Run(None)
        async with RunContext(run2) as run_context2:
            assert current_run_context() is run_context2
        assert current_run_context() is run_context1
    assert current_run_context(allow_none=True) is None


async def test_run_context_nested_exception():
    """Test the run context."""
    assert current_run_context(allow_none=True) is None
    run1 = Run(None)
    with pytest.raises(RuntimeError):
        async with RunContext(run1) as run_context1:
            assert current_run_context() is run_context1
            run2 = Run(None)
            async with RunContext(run2) as run_context2:
                assert current_run_context() is run_context2
                raise RuntimeError()
    assert current_run_context(allow_none=True) is None


async def test_run_context_save_load_artifact():
    """Test the run context."""
    run = Run(None)
    async with RunContext(run) as run_context:
        artifact = SimpleArtifactModel("test", None)
        await run_context.save_artifact("test", artifact)
    assert await run_context.load_artifact("test") == artifact


async def test_run_context_save_load_artifacts():
    """Test the run context."""
    run = Run(None)
    async with RunContext(run) as run_context:
        artifacts = {
            "test1": SimpleArtifactModel("test1", None),
            "test2": SimpleArtifactModel("test2", None),
        }
        await run_context.save_artifacts(artifacts)
    assert await run_context.load_artifacts() == artifacts
