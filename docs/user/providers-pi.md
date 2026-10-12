# Pi

T3 Code can use your existing Pi coding agent installation while keeping Pi's models, auth,
extensions, skills, context files, and native session history.

## Set Up Pi

1. Install [Pi](https://pi.dev) on the machine running the T3 Code server. Pi requires Node.js
   22.19 or newer; its platform installers can install Node if needed. T3 Code supports Pi
   0.80.5 or newer.
2. Run `pi` in a terminal, then `/login` to connect your model provider or configure its API key.
3. Open T3 Code Settings, enable Pi, and refresh the provider.

For an npm installation, use:

```sh
npm install -g --ignore-scripts @earendil-works/pi-coding-agent
```

If `pi` is not on the server's `PATH`, set Pi's binary path to the executable. Provider environment
variables and launch arguments are also available for installations that need a custom agent
directory, endpoint, or model configuration. `--provider` must be paired with `--model`. T3 Code
rejects launch arguments that change Pi's execution mode or select a session because T3 owns those
parts of the process lifecycle.

On a remote or headless server, connect to that machine over SSH and run `pi`, then `/login`.
Open the authorization URL on your own computer. If the callback cannot reach the server,
paste the final redirect URL or authorization code back into Pi when prompted. You can also
set your model provider's API-key environment variable in the Pi instance's settings; mark
secret values as sensitive.

Update installer-managed Pi with `pi update`. If another package manager owns the installation,
update it through that manager instead. For example, a Nix profile installation uses
`nix profile upgrade pi`.

## What Carries Over

T3 Code discovers the models reported by Pi and exposes their supported thinking levels. The
thinking picker marks the discovered active model's configured level as its default without
overriding it. Other models keep their Pi defaults when no level is selected. Threads
use Pi's native session files for resume, rollback, and forks within the same Pi instance. Forks
preserve the native conversation through the selected turn in the destination workspace.
Switching providers uses portable conversation context. Extension
dialogs appear in the T3 Code composer, and the composer context meter follows Pi's own usage
reporting while a response streams and after it settles.

Pi skills appear in the composer's `$` menu. This includes user skills and project skills that Pi
loads for the current workspace; selecting one uses Pi's native skill expansion.

Pi loads its normal user and project extensions. Blocking `select`, `confirm`, `input`, and `editor`
dialogs work in T3 Code. Notifications appear in the work log. Pi terminal decoration such as
titles, status lines, and widgets does not have a T3 Code equivalent.

## Permission Modes

T3 Code applies the composer permission mode through Pi's blocking tool hook:

- **Supervised** asks before commands, file changes, and extension tools. Read-only tools continue.
- **Auto-accept edits** allows Pi's edit and write tools, but asks before commands and extension
  tools.
- **Full access** allows tools without T3 Code approval prompts.

The **Auto** option is not shown for Pi because Pi does not expose an AI approval reviewer.
Threads that already stored Auto before Pi support was added behave and display as Supervised.

Changing the mode restarts the Pi provider session and resumes the same native conversation. The
policy covers Pi tool calls; it is not an operating-system sandbox, and code that a trusted Pi
extension runs outside a tool call remains governed by Pi's own extension trust model.

T3 Code's `delegate_task` tool creates durable child threads in the shared subagent UI. If the user
installs Pi's example `subagent` extension, T3 Code also shows its task progress and results in that
UI. Pi runs those children without a session, so they cannot be opened or resumed as T3 Code
threads.

## Inspect Pitako workers

Install a compatible Pitako extension in Pi on the server machine. T3 Code does not install
Pitako or create workers. In Settings, edit the Pi provider instance and enable **Pitako worker
bridge**. History, routing, and controls have separate opt-ins. All four settings start off and
apply to the next Pi process. Saving does not restart a running coordinator.

Open the thread's Agents view and choose **Inspect Pitako workers**, even while the coordinator
is idle. Pitako workers have a separate roster. They are not T3 subagents or durable T3 threads.
Load the roster, select a worker, then request the brief, activity, result, or history metadata
you need. Pages are bounded and replace the previous page. Reload deliberately when a cursor
is stale.

Provider settings set ceilings, not grants. Acknowledge private history and routing separately
before reading either. To control a worker, inspect and activate its exact current target.
Submit query or steer input explicitly. Input requires the coordinator's effective **Full access**
policy. Cancellation is separate and requires confirmation. A cancellation receipt means
requested, not settled, disposed, or result-ready. Closing inspection does not cancel work.

Inspection depends on the installed extension's capabilities and the resident Pi process.
Synchronous `agent_run` workers cannot be controlled. An unsupported extension, missing runtime,
retired owner, or pruned history remains unavailable. A last observation is not a promise of a
live connection. Requested model and service-tier settings are not proof of the returned tier.

Private access belongs to one authenticated view, not other tabs or devices. Closing the view or
changing its connection clears private pages and activation. Receipt lookup can recover an
existing decision in the same valid authenticated session, within bounded retention. It does
not restore grants or allow a resend. Pending or unknown receipts are not useful answers.
There is no durable worker recovery after the coordinator process is gone.

## Troubleshooting

- If Pi is unavailable, confirm that the configured binary runs on the server machine, then refresh
  the provider in Settings.
- If no models appear, open Pi directly and confirm its authentication and model configuration.
- If discovery cannot complete, T3 Code keeps Pi available with the `Pi default` model. Start a
  thread to let the interactive Pi session handle any startup prompt.
- If a project extension is missing, approve the project in Pi, then start a fresh provider session.
- If a project skill is missing from the `$` menu, approve the project in Pi and refresh the provider.
- The context meter appears once Pi reports usage for the thread. Some model providers only
  report usage when a response completes, so the meter can wait for the first reply.
