# Onboarding live sources

`config/sources.yaml` is the source registry. Bootstrap updates source definitions without deleting observed health or history. Restart after editing it. Run one dedicated connector scheduler initially; polling leases prevent concurrent claims for the same due source.

The enabled defaults are public broadcaster news feeds in all three countries. Scheduled fetching remains an explicit operational switch. LSM Culture, ERR News, and LRT English returned parseable RSS during implementation (50, 50, and 100 entries respectively). Treat this as a point-in-time fetch check, not a coverage guarantee or approval to redistribute full publisher content.

## RSS/Atom

```yaml
- id: approved-city-news
  name: City newsroom
  country: lv
  city_ids: ["lv:riga"]
  language: lv
  kind: rss
  url: https://publisher.example/feed.xml
  enabled: true
  interval_seconds: 600
```

Use `city_ids` only for a source with established city scope. National article mentions populate `candidate_city_ids`; agents receive those articles as potentially relevant news with a qualification. Article publication dates are not event dates. A national article with no resolved city wakes the country agent.

## JSON-LD event pages

Set `kind: html_jsonld` on a permitted page containing `application/ld+json` schema.org Events. Nested `@graph` and item lists are supported. Structured address locality can identify a monitored city; an arbitrary mention in body text cannot establish an event venue. Listing pages that only link to events require a publisher-specific adapter; the connector deliberately reports an extraction error if no events can be parsed.

## JSON API mapping

The parser accepts schema.org-like objects or an explicit field mapping:

```yaml
- id: approved-events-api
  name: Approved organizer API
  country: ee
  city_ids: ["ee:tallinn"]
  kind: json
  url: https://publisher.example/api/events
  url_env: APPROVED_EVENTS_URL
  token_env: APPROVED_EVENTS_TOKEN
  items_path: data.events
  field_map:
    identifier: id
    name: title
    description: description
    url: public_url
    startDate: starts_at
    endDate: ends_at
    dateModified: updated_at
    eventStatus: schema_org_status
  authoritative: true
  enabled: true
  interval_seconds: 1800
```

Mapping paths support nested object keys. The mapped target names are the parser's schema.org fields. `eventStatus` should contain `EventCancelled` or `EventPostponed` for those states; adapt publisher-specific enums before ingestion. Do not enable the Visit Estonia placeholder URL as if it were a functioning data endpoint: configure the approved API URL, token, and fields first.

## ICS

`kind: ics` accepts explicit VEVENT instances. Set source city scope. UID establishes source identity; STATUS=CANCELLED propagates cancellation; timezone-aware dates are normalized to UTC. Recurrence rules are rejected so missing occurrences cannot be silently advertised as complete coverage. Obtain an expanded feed or add a bounded recurrence adapter before using recurring calendars. An absent end date remains unknown, and the item cannot support a date-overlap theme.

## Validation procedure

1. Establish permitted access and which fields can be retained/displayed. Choose a stable source ID and canonical event IDs.
2. Fetch a representative sample, including one correction/cancellation; inspect extracted location, timezone, language, dates, status, identity, and URLs.
3. Add a fixture-based parser test. Do not check private feeds, tokens, or licensed full articles into git.
4. Poll the enabled source and inspect `/api/sources`, `/api/facts`, and affected city memory. Empty extraction is an error unless `allow_empty:true` is deliberately configured.
5. Measure false city matches and missing events with a guide before calling coverage complete.

Connectors use HTTPS, reject non-public resolved addresses and embedded URL credentials, do not follow redirects, cap responses at 5 MB, use ETag/Last-Modified, and back off after failures. Source URLs are operator-controlled. Production should additionally apply network egress controls; DNS prechecks alone are not a complete DNS-rebinding defense.
