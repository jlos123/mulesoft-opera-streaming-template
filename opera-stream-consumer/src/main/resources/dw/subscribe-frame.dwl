%dw 2.0
output application/json
// ORCHESTRATION MODE (Oracle's Streaming API Guide "Details Steps for Orchestration"): the `detail` array is
// intentionally NOT requested.
var inputArgsGraphQL = (vars.subscribeInput pluck ((value, key) -> '$(key): "$(value)"')) joinBy ", "
---
{
    id: vars.subscriptionId,
    'type': "subscribe",
    payload: {
        query: "subscription { newEvent(input: { " ++ inputArgsGraphQL ++ " }) { metadata { offset uniqueEventId } moduleName eventName primaryKey timestamp hotelId } }"
    }
}
