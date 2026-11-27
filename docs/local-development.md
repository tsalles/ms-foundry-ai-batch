# Local development

The in-process demo needs no credentials and makes no live Foundry calls:

```powershell
foundry-batch local-demo
foundry-batch local-demo --scenario lossy
foundry-batch local-demo --scenario outages
foundry-batch local-demo --scenario vault-rotation
```

It reports synthetic outcomes and exits nonzero on an invariant violation.
Provider, storage and secret-store doubles exercise the production application interfaces.
The optional [.env.example](../.env.example) shows how to select the local profile for CLI
commands other than `local-demo`. These settings are deliberately incompatible with hosted
Azure execution.

For Functions, copy [local.settings.example.json](../functions/local.settings.example.json)
to the ignored local settings file and supply the backend configuration appropriate to your
local services. The example uses Azurite storage coordinates and placeholder cloud endpoints;
it is not a ready-to-run cloud deployment or an alternative to the in-process demo.
Do not commit the populated file.

The [test guide](testing.md) covers SDK stand-ins, loopback contract tests, Azurite and the
explicit live-test opt-ins. Do not enable live tests unless authorized to create, inspect
and clean up synthetic cloud resources.
