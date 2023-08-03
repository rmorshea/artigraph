import pytest

from artigraph.api.run import RunManager, run_manager
from artigraph.orm.run import Run
from tests.test_api.test_artifact_model import SimpleArtifactModel


async def test_run_context():
    """Test the run context."""
    run = Run(None)
    assert run_manager(allow_none=True) is None
    async with RunManager(run) as run_context:
        assert run_manager() is run_context
    assert run_manager(allow_none=True) is None


async def test_run_context_nested():
    """Test the run context."""
    assert run_manager(allow_none=True) is None
    run1 = Run(None)
    async with RunManager(run1) as run_context1:
        assert run_manager() is run_context1
        run2 = Run(None)
        async with RunManager(run2) as run_context2:
            assert run_manager() is run_context2
        assert run_manager() is run_context1
    assert run_manager(allow_none=True) is None


async def test_run_context_nested_exception():
    """Test the run context."""
    assert run_manager(allow_none=True) is None
    run1 = Run(None)
    with pytest.raises(RuntimeError):
        async with RunManager(run1) as run_context1:
            assert run_manager() is run_context1
            run2 = Run(None)
            async with RunManager(run2) as run_context2:
                assert run_manager() is run_context2
                raise RuntimeError()
    assert run_manager(allow_none=True) is None


async def test_run_context_save_load_artifact():
    """Test the run context."""
    run = Run(None)
    async with RunManager(run) as run_context:
        artifact = SimpleArtifactModel("test", None)
        await run_context.save_artifact("test", artifact)
    assert await run_context.load_artifact("test") == artifact


async def test_run_context_save_load_artifacts():
    """Test the run context."""
    run = Run(None)
    async with RunManager(run) as run_context:
        artifacts = {
            "test1": SimpleArtifactModel("test1", None),
            "test2": SimpleArtifactModel("test2", None),
        }
        await run_context.save_artifacts(artifacts)
    assert await run_context.load_artifacts() == artifacts
