"""Controles voláteis e opt-in, usados sob o lock do nó."""

import os

import grpc
from cluster_protocol import demo_pb2 as pb, demo_pb2_grpc as rpc


class FaultInjection:
    def __init__(self):
        self.crash_armed = False
        self.commit_paused = False

    def after_persist(self):
        if self.crash_armed:
            # Executado sob o lock que Fetch também usa: a nova entrada ainda
            # não pode ter sido lida/enviada. SQLite já concluiu a transação.
            os._exit(86)


class DemoControl(rpc.DemoControlServicer):
    def __init__(self, node):
        if node.faults is None:
            raise ValueError("Controles exigem modo --demo")
        self.node = node

    def Configure(self, request, context):
        with self.node._lock:
            faults = self.node.faults
            if request.action == pb.ARM_CRASH:
                if self.node._role.value != "LEADER":
                    context.abort(grpc.StatusCode.FAILED_PRECONDITION, "O alvo deve ser líder")
                faults.crash_armed = True
            elif request.action == pb.PAUSE_COMMIT:
                if self.node._role.value != "LEADER":
                    context.abort(grpc.StatusCode.FAILED_PRECONDITION, "O alvo deve ser líder")
                faults.commit_paused = True
            elif request.action == pb.RESUME_COMMIT:
                faults.commit_paused = False
            elif request.action == pb.RESET:
                faults.crash_armed = False
                faults.commit_paused = False
            elif request.action != pb.STATUS:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Ação inválida")
            self.node.election._log(f"DEMO action={request.action} crash={faults.crash_armed} "
                                    f"commit_paused={faults.commit_paused}")
            return pb.ControlResponse(crash_armed=faults.crash_armed, commit_paused=faults.commit_paused)
