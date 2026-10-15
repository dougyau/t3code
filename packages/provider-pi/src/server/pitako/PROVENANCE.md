Client-only snapshot from Pitako, revision
`1932ea0259336855a2e31420c8a9e1059db08455`
(`Merge pull request #38 from dougyau/t3/worker-public-todo-snapshots`;
protocol 2, MIT; see LICENSE).

The six files below `extensions/agent/` preserve the bridge client's relative
framing imports. Only `client.js`'s Node import declarations are normalized to
T3's namespace-import convention; its original local names are retained.
They have no Pi extension or
TypeScript runtime dependency. This snapshot is independent of the external
Pitako extension installed by the user. Update all six files together.

No build step is needed: the `.js` and `.d.ts` sources are
already the published passive client. The source-copy command from the
repository root is:

```sh
for file in bridge/client.js bridge/client.d.ts bridge/protocol.js bridge/protocol.d.ts framing.js framing.d.ts; do
  cp "/home/doug/repos/pitako/extensions/agent/$file" "packages/provider-pi/src/server/pitako/$file"
done
```

After copying, normalize the four Node imports in `client.js` as above.
The corresponding declarations and the remaining five sources are verbatim.

Upstream copied-source SHA-256 hashes, before import normalization:

| File | SHA-256 |
| --- | --- |
| `bridge/client.js` | `cf7c85b65156133d35a67eef54c9604dfef424e29f275596b15f18403a47f4ff` |
| `bridge/client.d.ts` | `d8b27e4efccdb4248abef12f3c592416d7957ce4f26a9b797a9cc005de16a42e` |
| `bridge/protocol.js` | `d1bdd766c25ec110aa71f631c4d6b8e8501ae73e6ed30ff1879bcf23d7ea0f90` |
| `bridge/protocol.d.ts` | `b20bb2988e90ae7a80b7ecf4eb95c5ffc48284824b58b61e051e10bb5c3eae27` |
| `framing.js` | `054b4358aab2267b51e710357ba5df0109f1c0f80674e6a13709c223f359dcf4` |
| `framing.d.ts` | `e8bd1a0be75788ac12918d5b4b18a03b4c199ab7558ce5cfafab0c5f67f18dce` |
