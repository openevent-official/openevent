"""Server startup, restart and fatal exit acceptance; uses the installed SDK."""

from contextlib import contextmanager
from importlib.metadata import version
import os
import socket
import subprocess
from threading import Event

import grpc
from packaging.version import Version
import pytest

if Version(version("openevent-sdk")) < Version("0.8.0"):
    raise RuntimeError("server runtime tests require installed openevent-sdk>=0.8.0")

from openevent.sdk import AdminClient, OpenEventClient


def address():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{sock.getsockname()[1]}"


def config_file(root, limit):
    public, admin = address(), address()
    while admin == public:
        admin = address()
    config = root / "server.yaml"
    config.write_text(
        f'grpc:\n  listen_addr: "{public}"\n'
        f'admin:\n  listen_addr: "{admin}"\n'
        f'storage:\n  path: "{root / "data"}"\n'
        + ("" if limit is None else f"limits:\n  max_payload_bytes: {limit}\n")
    )
    return config, public, admin


@contextmanager
def running(root, limit=None):
    config, public, admin = config_file(root, limit)
    with (root / "server.log").open("a") as log:
        process = subprocess.Popen(
            [os.environ["OPENEVENT_SERVER_BIN"], str(config)], stdout=log, stderr=log
        )
        try:
            with OpenEventClient(public) as client, AdminClient(admin) as manager:
                grpc.channel_ready_future(client.channel).result(timeout=10)
                grpc.channel_ready_future(manager.channel).result(timeout=10)
                yield process, client, manager
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    pytest.fail("server shutdown did not finish")


@pytest.mark.parametrize("limit", [0, 62914561, -1])
def test_invalid_payload_configuration_exits_before_listening(tmp_path, limit):
    config, public, admin = config_file(tmp_path, limit)
    result = subprocess.run(
        [os.environ["OPENEVENT_SERVER_BIN"], str(config)],
        capture_output=True, text=True, timeout=10,
    )
    assert result.returncode != 0
    assert "max_payload_bytes" in result.stderr or "bad conversion" in result.stderr
    assert "listening" not in result.stdout
    assert not (tmp_path / "data").exists()
    for endpoint in (public, admin):
        host, port = endpoint.split(":")
        with socket.socket() as sock:
            assert sock.connect_ex((host, int(port))) != 0


def test_lower_write_limit_preserves_large_history(tmp_path):
    payload = b"h" * (40 * 1024 * 1024)
    with running(tmp_path, 60 * 1024 * 1024) as (process, client, admin):
        token = admin.add_token(1).binding.token
        channel = client.create_channel(1, token, "history").channel.channel_id
        uuid = client.get_uuid()
        seq = client.publish_auto_seq(1, token, channel, payload, uuid).seq
    assert process.returncode == 0
    # Omitted limit exercises the 16 MiB default with the same initialized data root.
    with running(tmp_path) as (process, client, admin):
        assert client.get_seq_by_uuid(uuid) == seq
        assert client.get_seq_by_uuid(0) == 0
        assert client.get_status(1, token).min_seq == 0
        assert client.fetch(1, token, seq, 1000).messages[0].payload == payload
        assert admin.list_messages(seq, 1000).messages[0].payload == payload
        stream = client.subscribe(1, token, from_seq=seq)
        try:
            stream.wait_started(5_000)
            assert next(stream).message.payload == payload
        finally:
            stream.cancel()
        with pytest.raises(grpc.RpcError) as error:
            client.publish_auto_seq(1, token, channel, payload, client.get_uuid())
        assert error.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
        assert client.get_status(1, token).max_seq == seq
    assert process.returncode == 0


@pytest.mark.parametrize("damage", ["missing", "size", "directory", "fifo"])
def test_committed_object_damage_exits_on_read(tmp_path, damage):
    with running(tmp_path) as (process, client, admin):
        token = admin.add_token(1).binding.token
        key = client.write_object(1, token, "file", "binary", b"contents")
    assert process.returncode == 0
    path = tmp_path / "data" / "objects" / str(key.object_id)
    path.unlink()
    if damage == "size":
        path.write_bytes(b"x")
    elif damage == "directory":
        path.mkdir()
    elif damage == "fifo":
        os.mkfifo(path)
    with running(tmp_path) as (process, client, admin):
        # Startup and metadata access must not inspect committed object files.
        assert client.get_object_metadata(key.object_id, key.object_token).nbytes == 8
        stream = client.subscribe(1, token, channels=[0])
        stream.wait_started(5_000)
        assert next(stream).message.seq == 0
        ended = Event()
        stream.add_done_callback(lambda _: ended.set())
        try:
            with pytest.raises(grpc.RpcError) as error:
                client.read_object(key.object_id, key.object_token, 0, 1)
            assert error.value.code() == grpc.StatusCode.DATA_LOSS
            assert process.wait(timeout=10) != 0
            assert ended.wait(5)
        finally:
            stream.cancel()


def test_normal_shutdown_ends_idle_subscriptions(tmp_path):
    with running(tmp_path) as (process, client, admin):
        token = admin.add_token(1).binding.token
        stream = client.subscribe(1, token, only_my_recipient=True)
        stream.wait_started(5_000)
        ended = Event()
        stream.add_done_callback(lambda _: ended.set())
        try:
            process.terminate()
            assert process.wait(timeout=10) == 0
            assert ended.wait(5)
        finally:
            stream.cancel()
