Client-only snapshot from Pitako, revision
`9f75a08dac88ec26d11c373c0438fe87905fdcbf`
(`Merge pull request #48 from dougyau/docs/architecture-background-plan`;
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
    "9f75a08dac88ec26d11c373c0438fe87905fdcbf:extensions/agent/$file" \
    > "packages/provider-pi/src/server/pitako/$file"
done
```

After copying, normalize the four Node imports in `client.js` as above.
The corresponding declarations and the remaining five sources are verbatim.

Upstream copied-source SHA-256 hashes, before import normalization:

| File | SHA-256 |
| --- | --- |
| `bridge/client.js` | `d9ea8873c579fe27fc0d58b04da3bec14f72f71ff5924299c66471214195f5f2` |
| `bridge/client.d.ts` | `572a99d930fdfc40ae7e872d8c7e86eb1dc4d357bcc3e72d8d91367f81d56622` |
| `bridge/protocol.js` | `cccdba1c49e004f6509a4c809691e1258265927e35a2c197bf6e881f9dd01018` |
| `bridge/protocol.d.ts` | `9206e61f8b337fa5efc74a5576d987a38e98dbc3ba19aca40c4209866b3d83ff` |
| `framing.js` | `054b4358aab2267b51e710357ba5df0109f1c0f80674e6a13709c223f359dcf4` |
| `framing.d.ts` | `e8bd1a0be75788ac12918d5b4b18a03b4c199ab7558ce5cfafab0c5f67f18dce` |
