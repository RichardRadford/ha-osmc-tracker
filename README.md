# OSMC Watch Tracker – Home Assistant app

Tracks what's played on an OSMC / Kodi box, and for how long, with a history and
stats panel in Home Assistant plus sensors and events for automations.

## Install

**Option A – local app (quickest)**
1. Install the *Samba share* or *Studio Code Server* app in Home Assistant.
2. Copy the `osmc_tracker` folder into the `/addons` share (e.g. `\\homeassistant\addons\osmc_tracker`).
3. **Settings → Apps → App store → ⋮ → Check for updates**. *OSMC Watch Tracker* appears under **Local apps**.
4. Install it, set `kodi_host` on the Configuration tab, then start it. Turn on *Show in sidebar*.

**Option B – your own GitHub repository**
Add this repository URL under **App store → ⋮ → Repositories**:
`https://github.com/RichardRadford/ha-osmc-tracker`

See [osmc_tracker/DOCS.md](osmc_tracker/DOCS.md) for Kodi setup, options, entities and automation examples.
