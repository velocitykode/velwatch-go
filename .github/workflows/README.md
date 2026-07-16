# CI/CD status: remote Actions OFF

GitHub Actions is disabled at the repo level for this repository (2026-07-16).
The org hit 90% of its included Actions minutes and local checks cover the same
ground. No workflow in this directory runs remotely.

Re-enable:

```sh
gh api -X PUT repos/velocitykode/velwatch-go/actions/permissions -F enabled=true
gh workflow list -R velocitykode/velwatch-go --all   # then gh workflow enable <id>
```

Workflows were also paused individually (`disabled_manually`), so both levels
need re-enabling.
