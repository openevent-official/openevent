"""Server startup, restart and fatal exit acceptance using the locally built SDK wheel."""

from concurrent.futures import Future, wait
from contextlib import contextmanager
import json
import os
import re
import signal
import socket
import subprocess
from threading import Event, Thread
import time

import grpc
import pytest

from openevent.sdk import AdminClient, OpenEventClient
from openevent.sdk.proto import openevent_pb2


def read_subscription_message(stream, expected_seq):
    received = Future()

    def read():
        try:
            received.set_result(next(stream).message)
        except Exception as error:
            received.set_exception(error)

    Thread(target=read, daemon=True).start()
    ready, _ = wait([received], timeout=10)
    if not ready:
        stream.cancel()
        pytest.fail(f"Subscription did not deliver seq {expected_seq} within 10 seconds")
    message = received.result()
    assert message.seq == expected_seq
    return message


def address():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{sock.getsockname()[1]}"


def config_file(root, limit, public=None, *, admin=None, storage_path=None):
    public = public or address()
    if admin is None:
        admin = address()
        while admin == public:
            admin = address()
    config = root / "server.yaml"
    if storage_path is None:
        storage_path = root / "data"
    config.write_text(
        f'grpc:\n  listen_addr: "{public}"\n'
        f'admin:\n  listen_addr: "{admin}"\n'
        f'storage:\n  path: {json.dumps(str(storage_path), ensure_ascii=False)}\n'
        + ("" if limit is None else f"limits:\n  max_payload_bytes: {limit}\n")
    )
    return config, public, admin


def running(root, limit=None, public=None):
    return running_config(*config_file(root, limit, public))


@contextmanager
def running_config(config, public, admin, *, cwd=None):
    with (config.parent / "stdout.log").open("a") as stdout, (config.parent / "stderr.log").open("a") as stderr:
        process = subprocess.Popen(
            [os.environ["OPENEVENT_SERVER_BIN"], str(config)], cwd=cwd, stdout=stdout, stderr=stderr
        )
        try:
            with OpenEventClient(public) as client, AdminClient(admin) as manager:
                grpc.channel_ready_future(client.channel).result(timeout=10)
                grpc.channel_ready_future(manager.channel).result(timeout=10)
                yield process, client, manager
        finally:
            # Tests that wait for SIGINT or a fatal exit check that status themselves.
            if process.returncode is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    pytest.fail("server shutdown did not finish")
                assert process.returncode == 0


def test_relative_storage_symlink_reopens_and_rejects_a_second_server(tmp_path):
    public = f"dns:///{address()}"
    admin = f"unix-abstract:openevent-runtime-{os.getpid()}:0"
    config, _, _ = config_file(tmp_path, None, public, admin=admin, storage_path="data")
    working_dir = tmp_path / "work"
    working_dir.mkdir()
    target = tmp_path / "disk"
    target.mkdir()
    data = working_dir / "data"
    data.symlink_to(target, target_is_directory=True)
    paths = "|".join(re.escape(str(path)) for path in (data, target))
    directory_in_log = re.compile(rf"(?<![\w/.-])(?:{paths})/?(?![\w/-]|\.\w)")
    payload = b"stored through a symlink"
    with running_config(config, public, admin, cwd=working_dir) as (_, client, manager):
        deadline = time.monotonic() + 5
        while True:
            output = (tmp_path / "stdout.log").read_text()
            errors = (tmp_path / "stderr.log").read_text()
            lines = output.splitlines()
            public_logged = any("public" in line.lower() and public in line for line in lines)
            admin_logged = any("admin" in line.lower() and admin in line for line in lines)
            storage_logged = directory_in_log.search(output) or directory_in_log.search(errors)
            if storage_logged and public_logged and admin_logged:
                break
            assert time.monotonic() < deadline, (output, errors)
            time.sleep(0.05)
        second_config = config_file(working_dir, None, storage_path=target)
        second = subprocess.run(
            [os.environ["OPENEVENT_SERVER_BIN"], str(second_config[0])],
            capture_output=True, text=True, timeout=10,
        )
        assert second.returncode != 0, second.stderr
        token = manager.add_token(1).token
        key = client.write_object(1, token, "file", "binary", payload)
    with running_config(*second_config) as (_, client, _):
        result = client.read_object(key.object_id, key.object_token, 0, len(payload))
        assert result.data == payload


def test_invalid_config_exits_before_opening_storage(tmp_path):
    config, _, _ = config_file(tmp_path, None, public="127.0.0.1:0")
    result = subprocess.run(
        [os.environ["OPENEVENT_SERVER_BIN"], str(config)],
        capture_output=True, text=True, timeout=10,
    )
    assert result.returncode != 0
    assert "grpc.listen_addr" in result.stderr
    assert not (tmp_path / "data").exists()


def test_lower_write_limit_preserves_large_history_and_sigint_ends_subscription(tmp_path):
    principal = 2**64 - 1
    payload = b"h" * (40 * 1024 * 1024)
    with running(tmp_path, 60 * 1024 * 1024) as (_, client, admin):
        token = admin.add_token(principal).token
        channel = client.create_channel(principal, token, "history").channel.channel_id
        seq = client.publish_auto_seq(principal, token, channel, payload, client.get_uuid()).seq
    # Omitted limit exercises the 16 MiB default with the same initialized data root.
    with running(tmp_path) as (process, client, admin):
        historical = client.fetch(principal, token, seq, 1000).messages[0]
        assert historical.principal == principal
        assert historical.payload == payload
        assert admin.list_messages(seq, 1000).messages[0].payload == payload

        unused_uuid = client.get_uuid()
        with pytest.raises(grpc.RpcError) as error:
            client.publish(principal, token, channel, seq + 1, payload, unused_uuid)
        assert error.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
        with pytest.raises(grpc.RpcError) as error:
            client.get_seq_by_uuid(unused_uuid)
        assert error.value.code() == grpc.StatusCode.NOT_FOUND
        assert client.get_status(principal, token).max_seq == seq

        stream = client.subscribe(principal, token, from_seq=seq)
        try:
            assert read_subscription_message(stream, seq).payload == payload
            ended = Event()
            stream.add_done_callback(lambda _: ended.set())
            assert stream.is_active()
            process.send_signal(signal.SIGINT)
            assert process.wait(timeout=10) == 0
            assert ended.wait(5)
        finally:
            stream.cancel()


def test_channel_system_messages_survive_restart(tmp_path):
    owner, initial_member, added_member = 2**64 - 1, 2**53 + 1, 2**63 + 17
    name = '对话 "quoted" \\ path\n'
    protocol = 'chat."v1\\'
    description = '详情\r\b\f\t\n"\\\x00\x01'
    # System messages must be committed even when their JSON exceeds this
    # application-payload limit. Large principals must remain exact JSON integers.
    with running(tmp_path, 1) as (_, client, admin):
        token = admin.add_token(owner).token
        cid = client.create_channel(
            owner, token, name,
            visibility=openevent_pb2.VISIBILITY_PRIVATE,
            protocol=protocol, description=description,
            members=[initial_member, owner, initial_member],
        ).channel.channel_id
        client.add_member(owner, token, cid, added_member)
        client.remove_member(owner, token, cid, initial_member)
        messages = list(client.fetch(owner, token, 0, 100).messages)
        assert [message.seq for message in messages] == [0, 1, 2, 3]
        initialization, added, removed = messages[1:]
        initialized = json.loads(initialization.payload)
        initialized["data"]["members"].sort()
        assert initialized == {
            "kind": "system.channel_initialized",
            "data": {
                "schema_version": 1, "name": name, "protocol": protocol,
                "description": description, "visibility": "private", "creator": owner,
                "members": sorted([owner, initial_member]),
            },
        }
        for message, kind, member in (
            (added, "system.member_added", added_member),
            (removed, "system.member_removed", initial_member),
        ):
            assert json.loads(message.payload) == {
                "kind": kind, "data": {"schema_version": 1, "member": member},
            }
        assert initialization.principal == 0
        assert not initialization.recipients and not initialization.object_keys
        assert all(message.ts_ms > 0 for message in (initialization, added, removed))

    with running(tmp_path, 1) as (_, client, admin):
        assert set(client.get_channel(owner, token, cid).channel.members) == {owner, added_member}
        assert client.get_seq_by_uuid(removed.uuid) == removed.seq
        assert list(admin.list_messages(0, 100).messages) == messages


def test_missing_object_allows_metadata_and_references_until_read_shuts_down_server(tmp_path):
    payload = b"contents"
    with running(tmp_path) as (_, client, admin):
        token = admin.add_token(1).token
        key = client.write_object(1, token, "file", "binary", payload)

    (tmp_path / "data" / "objects" / str(key.object_id)).unlink()
    with running(tmp_path) as (process, client, _):
        reference = openevent_pb2.ObjectKey(object_id=key.object_id, object_token=key.object_token)
        assert client.get_object_metadata(key.object_id, key.object_token).nbytes == len(payload)
        assert client.batch_get_object_metadata([reference]).metadata[0].nbytes == len(payload)
        channel = client.create_channel(1, token, "references").channel.channel_id
        client.publish_auto_seq(1, token, channel, b"reference", client.get_uuid(), object_keys=[reference])

        stream = client.subscribe(1, token, only_my_recipient=True)
        stream.wait_started(5_000)
        ended = Event()
        stream.add_done_callback(lambda _: ended.set())
        with pytest.raises(grpc.RpcError) as error:
            client.read_object(key.object_id, key.object_token, 0, 1)
        assert error.value.code() == grpc.StatusCode.DATA_LOSS
        assert process.wait(timeout=10) > 0
        assert ended.wait(5)

    output = (tmp_path / "stdout.log").read_text()
    errors = (tmp_path / "stderr.log").read_text()
    diagnostics = [
        line for line in errors.lower().splitlines()
        if re.search(r"\b(data[_ -]loss|15)\b", line)
        and re.search(r"\b(missing|not found|no such file|enoent)\b", line)
    ]
    assert len(diagnostics) == 1, errors
    assert re.search(r"\b(open|read)\b", diagnostics[0]) and re.search(r"\b(object|file)\b", diagnostics[0])
    for log in (output, errors):
        assert token not in log and key.object_token not in log


def test_server_keepalive_closes_connection_without_ping_ack(tmp_path):
    public = address()
    with running(tmp_path, public=public) as (_, client, _):
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
                        return None
                    data.extend(chunk)
                return data

            ping_received = False
            while True:
                header = receive_exact(9)
                if header is None:
                    break
                if receive_exact(int.from_bytes(header[:3], "big")) is None:
                    break
                frame_type, flags = header[3], header[4]
                if frame_type == 4 and not flags & 1:
                    # SETTINGS must be acknowledged to complete the handshake.
                    peer.sendall(bytes.fromhex("000000040100000000"))
                elif frame_type == 6 and not flags & 1 and not ping_received:
                    # Withhold PING acknowledgements and measure from the first PING.
                    ping_received = True
                    deadline = time.monotonic() + 15
            assert ping_received, "server closed before sending a complete PING"
        assert client.get_seq_by_uuid(0) == 0
