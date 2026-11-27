"""Local stand-ins for the external services, so the PoC can be built before cloud access.

Every emulator honours the same wire contract as the real service and is reached through the
production adapters (``FoundryRestClient``, ``HttpDownstream``) by swapping only the transport.
Switching to the real service is a configuration change, never a code change. Settings refuse
these backends outside local environments (see ``Settings.local_profile``).

Facts the documentation leaves open (retry schedule, ordering, payload fields, endpoint limits,
error codes) are *configurable assumptions* here, never claims about Azure: see
``DeliveryPolicy`` and ``docs/local-development.md``.
"""
