"""Batch Batch monitoring PoC.

Receives signed Azure OpenAI Batch webhooks, keeps a per-job registry with a transactional
outbox, relays normalised events downstream, reconciles missed final states by polling and
derives lifecycle indicators. Core logic is dependency-free and talks to infrastructure only
through the protocols in :mod:`foundry_batch.ports`.
"""

__version__ = "0.1.0"
