"""Replicação exclusivamente Pull e monitoramento local de Fetches."""

from dataclasses import dataclass, field
from threading import Event, Thread
import time

from config import FETCH_INTERVAL, HEARTBEAT_TIMEOUT, NODE_ADDRESSES
from node_state import Role
from storage import LogEntry


@dataclass(frozen=True)
class FetchRequest:
    term: int
    follower_id: str
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class FetchResponse:
    term: int
    leader_id: str
    is_leader: bool
    commit_index: int = 0
    entries: list[LogEntry] = field(default_factory=list)
    prev_log_index: int = 0
    prev_log_term: int = 0
    truncate_after_index: int | None = None


class Replication:
    def __init__(self, node, clock=time.monotonic, interval=FETCH_INTERVAL):
        self.node = node
        self._clock = clock
        self.interval = interval
        self.last_fetch_time = {}
        self.active_followers = {}
        self._heartbeat_term = None
        self._stop = Event()
        self._thread = None

    def _refresh_activity(self):
        status = self.node.get_status()
        if status.role != Role.LEADER or self._heartbeat_term != status.current_term:
            self.last_fetch_time.clear()
            self.active_followers.clear()
            self._heartbeat_term = status.current_term if status.role == Role.LEADER else None
        if status.role == Role.LEADER:
            for peer in NODE_ADDRESSES:
                if peer != status.node_id:
                    last = self.last_fetch_time.get(str(peer))
                    self.active_followers[str(peer)] = (
                        last is not None and self._clock() - last <= HEARTBEAT_TIMEOUT)

    def activity(self):
        with self.node._lock:
            self._refresh_activity()
            return dict(self.last_fetch_time), dict(self.active_followers)

    def fetch(self, request):
        with self.node._lock:
            self.node.election.observe_term(request.term)
            status = self.node.get_status()
            self._refresh_activity()
            valid = (status.role == Role.LEADER and request.term == status.current_term
                     and request.follower_id in {str(i) for i in NODE_ADDRESSES}
                     and request.follower_id != str(status.node_id))
            if not valid:
                return FetchResponse(status.current_term, status.leader_id,
                                     status.role == Role.LEADER)
            self.last_fetch_time[request.follower_id] = self._clock()
            self.active_followers[request.follower_id] = True
            log = self.node.storage.get_entries()
            index = request.last_log_index
            matches = ((index == 0 and request.last_log_term == 0) or
                       (0 < index <= len(log) and log[index - 1].term == request.last_log_term))
            # Em divergência, enviar o log completo permite validar o prefixo
            # comitado e substituir somente o sufixo conflitante na réplica.
            prev = index if matches else 0
            return FetchResponse(
                status.current_term, str(status.node_id), True, status.commit_index,
                log[prev:], prev, log[prev - 1].term if prev else 0,
                len(log) if index > len(log) else None,
            )

    def accept(self, peer_id, response):
        with self.node._lock:
            self.node.election.observe_term(response.term)
            status = self.node.get_status()
            if (response.term != status.current_term or not response.is_leader
                    or response.leader_id != str(peer_id)
                    or str(peer_id) == str(status.node_id)
                    or peer_id not in NODE_ADDRESSES):
                return False
            self.node.storage.apply_fetch(
                response.prev_log_index, response.prev_log_term, response.entries,
                response.commit_index, response.truncate_after_index,
            )
            return self.node.election.observe_leader(response.term, response.leader_id)

    def step(self, get_status, send_fetch):
        with self.node._lock:
            self._refresh_activity()
            status = self.node.get_status()
        if status.role == Role.LEADER:
            return
        peer = int(status.leader_id) if status.leader_id else None
        if peer is None:
            # Candidatos também descobrem líderes para interromper candidaturas.
            for candidate in NODE_ADDRESSES:
                if self._stop.is_set():
                    return
                if candidate == status.node_id:
                    continue
                remote = get_status(candidate)
                if remote is None:
                    continue
                with self.node._lock:
                    self.node.election.observe_term(remote.current_term)
                    current = self.node.get_status()
                    if (remote.node_id == candidate and remote.role == Role.LEADER
                            and remote.current_term >= current.current_term):
                        peer = candidate
                        break
        if peer is None or self._stop.is_set():
            return
        with self.node._lock:
            if self.node._role == Role.LEADER:
                return
            metadata, (index, term) = self.node.storage.snapshot()
            request = FetchRequest(metadata.current_term, str(self.node._node_id), index, term)
        response = send_fetch(peer, request)
        if self._stop.is_set():
            return
        if response is not None and self.accept(peer, response):
            return
        with self.node._lock:
            # Falha não reinicia o timeout; nova iteração tentará descobrir líder.
            if self.node._leader_id == str(peer):
                self.node._leader_id = ""

    def start(self, get_status, send_fetch):
        self._thread = Thread(target=self._run, args=(get_status, send_fetch), name="fetch")
        self._thread.start()

    def _run(self, get_status, send_fetch):
        while not self._stop.wait(self.interval):
            try:
                self.step(get_status, send_fetch)
            except ValueError as exc:
                # Uma resposta incompatível não autoriza commit nem heartbeat.
                self.node.election._log(f"Fetch rejeitado: {exc}")

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
