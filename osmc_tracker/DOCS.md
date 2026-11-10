# OSMC Watch Tracker

Records everything played on your OSMC box (Kodi): what it was, when, how long
you actually watched it (pauses excluded), how far through you got, and whether
you finished it. History and stats appear in the **OSMC Tracker** sidebar panel.

## 1. Prepare OSMC / Kodi

On the OSMC box: **Settings → Services → Control** and turn on
**Allow remote control from applications on other systems**.
This opens Kodi's JSON-RPC websocket on port **9090**. No username or password
is needed for it.

Giving the OSMC box a fixed IP address on your router is recommended.

## 2. Configure the app

| Option | Default | Meaning |
|---|---|---|
| `kodi_host` | `osmc.local` | IP or hostname of the OSMC box |
| `kodi_port` | `9090` | Kodi websocket port |
| `poll_interval` | `10` | Seconds between position checks. Play, pause and stop events are picked up instantly anyway. |
| `min_session_seconds` | `60` | Shorter plays (skipping through files, trailers) are not recorded |
| `completed_percent` | `90` | Reaching this point marks a session as finished ✓ |
| `publish_sensors` | `true` | Create sensors in Home Assistant |
| `entity_prefix` | `osmc` | Prefix for the sensor entity IDs |

## What gets recorded

One row per *session* (a continuous play of one item). If you stop an episode
and resume it tomorrow, that's two sessions and both count towards the totals.

* Watched time counts only while playing. Pauses are excluded and counted separately.
* Episodes record show, season and episode. Movies record the year. Music records the artist and album. Live TV records the channel.
* If the OSMC box is switched off or loses network, the session in progress is closed.
* The data lives in `/data/osmc_tracker.db` (SQLite) and is included in Home Assistant backups.
* **History → Export CSV** downloads everything.

## Home Assistant entities

| Entity | State | Useful attributes |
|---|---|---|
| `sensor.osmc_now_playing` | Title, or `idle` / `disconnected` | `playback_state`, `media_type`, `show`, `season`, `episode`, `progress_percent`, `session_minutes` |
| `sensor.osmc_watch_time_today` | Minutes watched today | `episode_minutes`, `movie_minutes`, `song_minutes`, `sessions` |
| `sensor.osmc_last_watched` | Last finished session | `watched_minutes`, `percent`, `completed`, `ended` |

These sensors are pushed by the app, so they have no unique ID and can't be renamed
in the UI. After a Home Assistant restart they reappear within about 10 minutes.

When each session ends, the app also fires the event **`osmc_tracker_session_ended`**
(`title`, `media_type`, `show`, `season`, `episode`, `watched_minutes`, `percent`, `completed`).

### Example: a notification when a film is finished

```yaml
automation:
  - alias: Film finished on OSMC
    trigger:
      - platform: event
        event_type: osmc_tracker_session_ended
        event_data:
          media_type: movie
          completed: true
    action:
      - service: notify.notify
        data:
          message: "Finished {{ trigger.event.data.title }} ({{ trigger.event.data.watched_minutes | round }} min)"
```

### Example: a TV time limit for the kids

```yaml
automation:
  - alias: TV limit reached
    trigger:
      - platform: numeric_state
        entity_id: sensor.osmc_watch_time_today
        above: 120
    action:
      - service: notify.notify
        data:
          message: "Over 2 hours of TV today"
```

### Example: a dashboard card

```yaml
type: entities
entities:
  - entity: sensor.osmc_now_playing
  - entity: sensor.osmc_watch_time_today
  - entity: sensor.osmc_last_watched
```

## Troubleshooting

* **"OSMC not reachable"**: check `kodi_host` and that the remote-control setting
  above is on. From another machine, `nc -vz <osmc-ip> 9090` should connect.
* Set `log_level: debug` to see every Kodi event in the app log.
