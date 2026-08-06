# Satta private dashboard

This app is the private, read-only operator dashboard for the Trench paper
daemon. It is intentionally scaffold-only until the dashboard workstream is
started; it must not grow wallet, order, exchange, Telegram, or secret-bearing
features.

The dashboard integration contract lives in
[`../../docs/dashboard/private-readonly-contract.md`](../../docs/dashboard/private-readonly-contract.md).
The server-side adapter may read the daemon's authenticated Unix status socket,
but the browser must never connect to that socket or expose it over TCP.

## Adding components

To add components to your app, run the following command:

```bash
npx shadcn@latest add button
```

This will place the ui components in the `components` directory.

## Using components

To use the components in your app, import them as follows:

```tsx
import { Button } from "@/components/ui/button";
```

## Local checks

```bash
npm run lint
npm run typecheck
npm run build
```

Keep status polling read-only and fail closed when the daemon status is stale
or unavailable. The dashboard is not a trading control plane.
