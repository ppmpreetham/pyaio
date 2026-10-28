import asyncio
import pytest
import pyaio_faq

@pytest.mark.asyncio
async def test_sleep_ret():
    val = await pyaio_faq.sleep_ret(41, 10)
    assert val == 42

@pytest.mark.asyncio
async def test_ready_ret():
    val = await pyaio_faq.ready_ret(9)
    assert val == 10

@pytest.mark.asyncio
async def test_ready_inline():
    val = await pyaio_faq.ready_inline(99)
    assert val == 100

@pytest.mark.asyncio
async def test_fail():
    with pytest.raises(ValueError, match="rust error"):
        await pyaio_faq.fail()

@pytest.mark.asyncio
async def test_panic_translated():
    with pytest.raises(Exception, match="kaboom"):
        await pyaio_faq.boom()

@pytest.mark.asyncio
async def test_cancellation():
    task = asyncio.create_task(pyaio_faq.hang_then_drop())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
