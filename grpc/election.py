"""Regras de eleição sem dependência de gRPC; estado protegido pelo lock do nó."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import logging
import random
from threading import Event, Thread
import time

from config import ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX, NODE_ADDRESSES, QUORUM
from node_state import Role


@dataclass(frozen=True)
class VoteRequest:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class VoteResponse:
    term: int
    vote_granted: bool


class Election:
    def __init__(self, node, clock=time.monotonic, uniform=random.uniform):
        self.node = node
        self._clock = clock
        self._uniform = uniform
        self._votes = set()
        self._stop = Event()
        self._thread = None
        self._reset_timeout()

    def _reset_timeout(self):
        self._deadline = self._clock() + self._uniform(
            ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def _log(self, message):
        status = self.node.get_status()
        logging.info("node_id=%s role=%s term=%s commit_index=%s %s",
                     status.node_id, status.role.value, status.current_term,
                     status.commit_index, message)

    def observe_term(self, term):
        with self.node._lock:
            if term > self.node.storage.load_metadata().current_term:
                self.node.storage.save_metadata(term, None)
                self.node._role = Role.FOLLOWER
                self.node._leader_id = ""
                self._votes.clear()
                self._reset_timeout()
                self.node.leadership_changed()
                self._log("Termo superior recebido")

    def observe_leader(self, term, leader_id):
        """Reconhece comunicação bem-sucedida com um líder validado pelo protocolo."""
        with self.node._lock:
            self.observe_term(term)
            if (term < self.node.storage.load_metadata().current_term
                    or leader_id not in {str(i) for i in NODE_ADDRESSES}
                    or leader_id == str(self.node._node_id)):
                return False
            changed = self.node._role != Role.FOLLOWER or self.node._leader_id != leader_id
            self.node._role = Role.FOLLOWER
            self.node._leader_id = leader_id
            self._votes.clear()
            self._reset_timeout()
            if changed:
                self.node.leadership_changed()
                self._log("Líder reconhecido")
            return True

    def request_vote(self, request):
        with self.node._lock:
            self.observe_term(request.term)
            metadata, (index, term) = self.node.storage.snapshot()
            granted = (
                request.term == metadata.current_term
                and request.candidate_id in {str(i) for i in NODE_ADDRESSES}
                and metadata.voted_for in (None, request.candidate_id)
                and (request.last_log_term, request.last_log_index) >= (term, index)
            )
            if granted:
                self.node.storage.save_metadata(request.term, request.candidate_id)
                self._reset_timeout()
                self._log(f"Voto concedido a {request.candidate_id}")
            return VoteResponse(metadata.current_term, granted)

    def start_election(self):
        with self.node._lock:
            if self._stop.is_set() or self.node._role == Role.LEADER:
                return None
            metadata, (index, log_term) = self.node.storage.snapshot()
            term = metadata.current_term + 1
            own_id = str(self.node._node_id)
            self.node.storage.save_metadata(term, own_id)
            self.node._role = Role.CANDIDATE
            self.node._leader_id = ""
            self._votes = {own_id}
            self._reset_timeout()
            self.node.leadership_changed()
            self._log("Eleição iniciada")
            return VoteRequest(term, own_id, index, log_term)

    def receive_vote(self, peer_id, election_term, response):
        with self.node._lock:
            # Mesmo uma resposta atrasada pode revelar um termo superior.
            self.observe_term(response.term)
            if (self._stop.is_set() or self.node._role != Role.CANDIDATE
                    or self.node.storage.load_metadata().current_term != election_term
                    or response.term != election_term or not response.vote_granted
                    or str(peer_id) not in {str(i) for i in NODE_ADDRESSES}):
                return
            self._votes.add(str(peer_id))
            if len(self._votes) >= QUORUM:
                self.node._role = Role.LEADER
                self.node._leader_id = str(self.node._node_id)
                self.node.leadership_changed()
                self._log("Líder eleito com quórum")

    def start_if_due(self):
        with self.node._lock:
            if self._clock() >= self._deadline:
                return self.start_election()
            return None

    def start(self, send_vote):
        """send_vote(peer_id, request) retorna VoteResponse ou None se indisponível."""
        self._thread = Thread(target=self._run, args=(send_vote,), name="election")
        self._thread.start()

    def _run(self, send_vote):
        with ThreadPoolExecutor(max_workers=3) as pool:
            while not self._stop.wait(0.02):
                request = self.start_if_due()
                if request is None:
                    continue
                futures = {pool.submit(send_vote, peer, request): peer
                           for peer in NODE_ADDRESSES if peer != self.node._node_id}
                for future in as_completed(futures):
                    response = future.result()
                    if response is not None:
                        self.receive_vote(futures[future], request.term, response)

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
