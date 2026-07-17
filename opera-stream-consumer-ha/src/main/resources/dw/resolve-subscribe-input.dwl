%dw 2.0
output application/java
// vars.offsetRecord: {offset: String, uniqueEventId: String, lastConnectedAt: String} | null (null on first run,
// or if os:contains found nothing). Offset is always treated as an opaque String - never parsed as a Number.
var chainCode = p('ohip.chainCode')
// Subtracting two DateTimes yields a Period, which is NOT coercible to Number. Convert each side to
// epoch millis first, then subtract.
var gapMs = if (vars.offsetRecord == null) null
            else (now() as Number {unit: "milliseconds"}) - (vars.offsetRecord.lastConnectedAt as DateTime as Number {unit: "milliseconds"})
var oneDayMs = 24 * 60 * 60 * 1000
var sevenDaysMs = 7 * oneDayMs
// Resume from the saved offset ONLY inside the [1 day, 7 day] window: fresher than 1 day -> just take the
// latest (subscribe fresh); older than 7 days -> Oracle has purged the offset, so subscribe fresh too.
// Every other case (no record, gap <1d, gap >7d) subscribes fresh with just the chainCode.
var withinResumeWindow = gapMs != null and gapMs >= oneDayMs and gapMs <= sevenDaysMs
---
if (withinResumeWindow) { chainCode: chainCode, offset: vars.offsetRecord.offset }
else { chainCode: chainCode }
