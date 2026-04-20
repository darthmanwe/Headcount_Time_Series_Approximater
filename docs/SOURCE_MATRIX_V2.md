# Source Matrix V2

## 1. Source categories

| Source | Primary use | Strengths | Weaknesses | Priority |
|---|---|---|---|---|
| Company website / about page | Identity, public statements | First-party, low ambiguity | Often stale, sparse | High |
| Careers page | Hiring/growth evidence | Easy to parse, useful directional signal | Not exact headcount | High |
| Press releases / newsroom | Event evidence | Good for growth, layoffs, acquisitions | Sparse and promotional | High |
| SEC / issuer filings | Public-company validation | High trust, legally clean | Infrequent, not startup-friendly | Medium |
| Manual analyst observations | Correction and anchors | High quality for priority accounts | Not scalable | High |
| Free-tier APIs | Secondary anchors / resolution | Useful if available | Rate-limited, incomplete | Medium |
| Logged-out public LinkedIn company pages | Current anchor | Closest metric surface to target | Can be gated, volatile | High |
| Logged-out public LinkedIn public profiles | Employment intervals | Best public interval signal | Coverage bias, parsing noise | High |
| Job boards / ATS pages | Hiring activity | Useful directional support | Not headcount truth | Medium |

## 2. Recommended weighting by role

### For canonical company resolution
1. domain
2. first-party website
3. manually verified aliases
4. source links
5. free-tier API identifiers

### For current anchor selection
1. manually verified anchor
2. visible public company page employee count
3. recent first-party company statement
4. free-tier API anchor
5. stale public statement

### For event segmentation
1. first-party press release
2. reliable public news / issuer filing
3. manual analyst event note
4. weaker public hints

### For historical monthly reconstruction
1. public employment intervals
2. prior stored snapshots
3. event-aware adjustments
4. fallback directional signals

## 3. Source policy for the LinkedIn public path

Use only:
- public company pages
- public profile pages
- visible public experience content

Do not implement:
- logins
- CAPTCHA solutions
- session pools
- proxy rotation
- stealth fingerprinting
- forceful retries after blocks

Fail closed and route to review.
