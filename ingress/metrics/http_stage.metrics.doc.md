| Name | Type | Attributes | Description |
|---------|---------|---------|---------|
| http.server.request.duration | `histogram` `float` | [http.request.method](#http.request.method) [url.scheme](#url.scheme) [http.response.status_code](#http.response.status_code) [network.protocol.version](#network.protocol.version) | Duration of HTTP server requests from handler entry until the response is written. |
| http.server.active_requests | `upDownCounter` `integer` | - | Number of HTTP server requests currently being handled. |
| http.server.request.body.size | `histogram` `integer` | [http.request.method](#http.request.method) [url.scheme](#url.scheme) [http.response.status_code](#http.response.status_code) [network.protocol.version](#network.protocol.version) | Size of HTTP request bodies observed by the server, including partially read bodies. |
| http.server.response.body.size | `histogram` `integer` | [http.request.method](#http.request.method) [url.scheme](#url.scheme) [http.response.status_code](#http.response.status_code) [network.protocol.version](#network.protocol.version) | Size of HTTP response bodies successfully written by the server. |
| goccia.http.ingress.queue.len | `gauge` `integer` | - | Number of HTTP request messages currently waiting in the ingress output queue. |
| goccia.http.ingress.queue.wait.duration | `histogram` `float` | [outcome](#outcome) | Time spent waiting to enqueue an HTTP request message for downstream processing. |
| goccia.http.ingress.pending_responses | `upDownCounter` `integer` | - | Number of HTTP requests currently awaiting a downstream response. |
| goccia.http.ingress.response.wait.duration | `histogram` `float` | [outcome](#outcome) | Time spent waiting for a downstream response after a successful queue handoff. |
