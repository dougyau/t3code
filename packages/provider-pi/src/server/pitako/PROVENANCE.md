Client-only snapshot from Pitako, revision
`198bb5204d9dc85b87e83a2f67999f3224c5d10d`
(`Merge pull request #56 from dougyau/t3/2077712c`;
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
  git -C /home/doug/repos/pitako show \
    "198bb5204d9dc85b87e83a2f67999f3224c5d10d:extensions/agent/$file" \
    > "packages/provider-pi/src/server/pitako/$file"
done
```

After copying, normalize the four Node imports in `client.js` as above.
The corresponding declarations and the remaining five sources are verbatim.

Upstream copied-source SHA-256 hashes, before import normalization:

| File                   | SHA-256                                                            |
| ---------------------- | ------------------------------------------------------------------ |
| `bridge/client.js`     | `d9ea8873c579fe27fc0d58b04da3bec14f72f71ff5924299c66471214195f5f2` |
| `bridge/client.d.ts`   | `572a99d930fdfc40ae7e872d8c7e86eb1dc4d357bcc3e72d8d91367f81d56622` |
| `bridge/protocol.js`   | `2c6f699a4d0c9d880c15fdb216a5fe6b845a20705f5b20bd32e8a029eb4a3c10` |
| `bridge/protocol.d.ts` | `171891be8fed9e4b804b4ba3028d94d0d4ebc8949deeda7964072a0ca28ef6c4` |
| `framing.js`           | `054b4358aab2267b51e710357ba5df0109f1c0f80674e6a13709c223f359dcf4` |
| `framing.d.ts`         | `e8bd1a0be75788ac12918d5b4b18a03b4c199ab7558ce5cfafab0c5f67f18dce` |
