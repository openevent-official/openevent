"""Server startup, restart and fatal exit acceptance using the locally built SDK wheel."""

from contextlib import contextmanager
from importlib.metadata import version
import json
import os
import socket
import subprocess
from threading import Event
import time

import grpc
from packaging.version import Version
import pytest

if Version(version("openevent-sdk")) < Version("0.13.0"):
    raise RuntimeError("server runtime tests require openevent-sdk>=0.13.0; run make test-runtime")

from openevent.sdk import AdminClient, OpenEventClient
from openevent.sdk.proto import openevent_pb2


def address():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{sock.getsockname()[1]}"


def config_file(root, limit, public=None):
    public, admin = public or address(), address()
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
def running(root, limit=None, public=None):
    config, public, admin = config_file(root, limit, public)
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
        token = admin.add_token(1).token
        channel = client.create_channel(1, token, "history").channel.channel_id
        uuid = client.get_uuid()
        seq = client.publish_auto_seq(1, token, channel, payload, uuid).seq
    assert process.returncode == 0
    # Omitted limit exercises the 16 MiB default with the same initialized data root.
    with running(tmp_path) as (process, client, admin):
        assert client.get_seq_by_uuid(uuid) == seq
        assert client.get_seq_by_uuid(0) == 0
        assert client.get_status(1, token).max_seq == seq
        assert client.fetch(1, token, seq, 1000).messages[0].payload == payload
        assert admin.list_messages(seq, 1000).messages[0].payload == payload
        stream = client.subscribe(1, token, from_seq=seq)
        try:
            stream.wait_started(5_000)
            assert next(stream).message.payload == payload
        finally:
            stream.cancel()
        for publish, uses_cas in ((client.publish_auto_seq, False), (client.publish, True)):
            request = dict(principal=1, token=token, channel_id=channel, payload=payload, uuid=uuid)
            if uses_cas:
                request["seq"] = seq  # Already committed, so this CAS is also stale.
            with pytest.raises(grpc.RpcError) as error:
                publish(**request)
            assert error.value.code() == grpc.StatusCode.ALREADY_EXISTS
            assert client.get_seq_by_uuid(uuid) == seq

            request["uuid"] = client.get_uuid()
            if uses_cas:
                request["seq"] = seq + 1
            with pytest.raises(grpc.RpcError) as error:
                publish(**request)
            assert error.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
            with pytest.raises(grpc.RpcError) as error:
                client.get_seq_by_uuid(request["uuid"])
            assert error.value.code() == grpc.StatusCode.NOT_FOUND
        assert client.get_status(1, token).max_seq == seq
    assert process.returncode == 0


def test_token_deletion_is_scoped_to_principal_and_persists(tmp_path):
    with running(tmp_path) as (process, client, admin):
        token = admin.add_token(100).token
        other_token = admin.add_token(200).token
        admin.delete_token(300, token)
        assert client.get_status(100, token).max_seq == 0
        admin.delete_token(100, token)
        admin.delete_token(100, token)
        with pytest.raises(grpc.RpcError) as error:
            client.get_status(100, token)
        assert error.value.code() == grpc.StatusCode.UNAUTHENTICATED
        assert client.get_status(200, other_token).max_seq == 0
    assert process.returncode == 0
    with running(tmp_path) as (process, client, admin):
        with pytest.raises(grpc.RpcError) as error:
            client.get_status(100, token)
        assert error.value.code() == grpc.StatusCode.UNAUTHENTICATED
        assert client.get_status(200, other_token).max_seq == 0
    assert process.returncode == 0


def test_channel_system_messages_survive_restart(tmp_path):
    owner, initial_member, added_member = 2**64 - 1, 2**53 + 1, 2**63 + 17
    name = '对话 "quoted" \\ path\n'
    description = 'line\n\twith null\x00'
    # System messages must be committed even when their JSON exceeds this
    # application-payload limit. Large principals must remain exact JSON integers.
    with running(tmp_path, 1) as (process, client, admin):
        token = admin.add_token(owner).token
        channel = client.create_channel(
            owner, token, name,
            visibility=openevent_pb2.VISIBILITY_PRIVATE,
            protocol="chat.v1", description=description,
            members=[initial_member, owner, initial_member],
        ).channel
        cid = channel.channel_id
        user_seq = client.publish_auto_seq(owner, token, cid, b"x", client.get_uuid()).seq
        client.add_member(owner, token, cid, added_member)
        client.remove_member(owner, token, cid, initial_member)
        messages = list(client.fetch(owner, token, 0, 100, channels=[cid]).messages)
        assert [message.seq for message in messages] == [1, 2, 3, 4]
        assert messages[1].seq == user_seq
        assert messages[1].principal == owner
        assert messages[1].payload == b"x"
        initialization, added, removed = (messages[index] for index in (0, 2, 3))
        assert json.loads(initialization.payload) == {
            "kind": "system.channel_initialized",
            "data": {
                "schema_version": 1, "name": name, "protocol": "chat.v1",
                "description": description, "visibility": "private", "creator": owner,
                "members": list(channel.members),
            },
        }
        assert set(json.loads(initialization.payload)["data"]["members"]) == {owner, initial_member}
        for message, kind, member in (
            (added, "system.member_added", added_member),
            (removed, "system.member_removed", initial_member),
        ):
            assert json.loads(message.payload) == {
                "kind": kind, "data": {"schema_version": 1, "member": member},
            }
        for message in (initialization, added, removed):
            assert message.principal == 0
            assert not message.recipients and not message.object_keys
            assert message.uuid > 0 and message.ts_ms > 0
        assert len({message.uuid for message in messages}) == len(messages)
        high = client.get_status(owner, token).max_seq
    assert process.returncode == 0

    with running(tmp_path, 1) as (process, client, admin):
        assert client.get_status(owner, token).max_seq == high
        assert set(client.get_channel(owner, token, cid).channel.members) == {owner, added_member}
        assert list(client.fetch(owner, token, 0, 100, channels=[cid]).messages) == messages
        for message in messages:
            assert client.get_seq_by_uuid(message.uuid) == message.seq
        all_messages = admin.list_messages(0, 100).messages
        assert all_messages[0].seq == 0
        assert list(all_messages[1:]) == messages
    assert process.returncode == 0


@pytest.mark.parametrize("damage", ["missing", "size", "directory", "fifo"])
def test_committed_object_damage_exits_on_read(tmp_path, damage):
    with running(tmp_path) as (process, client, admin):
        token = admin.add_token(1).token
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

    log = (tmp_path / "server.log").read_text()
    diagnostics = [line for line in log.splitlines() if line.startswith("fatal storage error:")]
    assert len(diagnostics) == 1
    operation = "open committed object file" if damage == "missing" else "stat committed object file"
    reason = "No such file" if damage == "missing" else "type or size mismatch"
    assert f"operation={operation}" in diagnostics[0]
    assert "error=" in diagnostics[0] and "grpc_code=15" in diagnostics[0]
    assert reason in diagnostics[0]
    assert all(secret not in log for secret in (token, key.object_token, "contents"))


def test_normal_shutdown_ends_idle_subscriptions(tmp_path):
    with running(tmp_path) as (process, client, admin):
        token = admin.add_token(1).token
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


def test_server_keepalive_closes_connection_without_ping_ack(tmp_path):
    public = address()
    with running(tmp_path, public=public) as (process, client, admin):
        token = admin.add_token(1).token
        host, port = public.rsplit(":", 1)
        # A raw HTTP/2 peer has no client keepalive or RPC deadline. Only the
        # server can close this connection when its PING is not acknowledged.
        with socket.create_connection((host, int(port)), timeout=5) as peer:
            peer.sendall(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n" + bytes.fromhex("000000040000000000"))
            deadline = time.monotonic() + 35

            def receive_exact(size):
                data = bytearray()
                while len(data) < size:
                    remaining = deadline - time.monotonic()
                    assert remaining > 0, "server keepalive exceeded its timeout"
                    peer.settimeout(remaining)
                    chunk = peer.recv(size - len(data))
                    if not chunk:
                        assert not data, "connection closed within an HTTP/2 frame"
                        return None
                    data.extend(chunk)
                return data

            ping_received = False
            while True:
                header = receive_exact(9)
                if header is None:
                    assert ping_received, "server closed before sending a PING"
                    break
                payload = receive_exact(int.from_bytes(header[:3], "big"))
                assert payload is not None
                frame_type, flags = header[3], header[4]
                if frame_type == 4 and not flags & 1:
                    # SETTINGS must be acknowledged to complete the handshake.
                    peer.sendall(bytes.fromhex("000000040100000000"))
                elif frame_type == 6 and not flags & 1:
                    assert len(payload) == 8
                    if not ping_received:
                        ping_received = True
                        # Ten seconds plus scheduling slack; do not extend the
                        # deadline if another PING is received.
                        deadline = time.monotonic() + 15
                    # Deliberately withhold the PING acknowledgement.
        assert process.poll() is None
        assert client.get_status(1, token).max_seq == 0
