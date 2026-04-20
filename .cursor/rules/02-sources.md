# Source rule

Supported source modes:
1. first-party public web
2. free-tier APIs / public datasets
3. logged-out public LinkedIn observation path
4. manual analyst validation

Logged-out LinkedIn path constraints:
- public pages only
- no authentication
- no CAPTCHA solving
- no rotating proxies
- no stealth browser logic
- no retry logic meant to push through gating
- fail closed on blocks or unstable access

Always:
- cache responses
- persist raw evidence snapshots
- version parsers
- return typed normalized observations
