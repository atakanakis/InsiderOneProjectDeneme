#!/usr/bin/env python3
"""Burst load test against a running stack (docker compose up).

Enqueues N low-priority SMS in batches of 1000, then one high-priority SMS,
and reports:
  * how quickly the high-priority message overtook the backlog,
  * end-to-end throughput,
  * the highest number of requests the mock provider received in any one
    second (must not exceed the 100/s per-channel rate limit).

Usage: python3 scripts/loadtest.py [count]   (stdlib only, default 2000)
"""
import json
import sys
import time
import urllib.request

API = "http://localhost:8080"
PROVIDER = "http://localhost:9090"


def call(url, body=None, method=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method or ("POST" if data else "GET"),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        raw = r.read()
        return json.loads(raw) if raw else None


def main():
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
    call(PROVIDER + "/reset", method="POST")

    start = time.time()
    batches = []
    for b in range(0, count, 1000):
        items = [{"recipient": "+90555%07d" % i, "channel": "sms",
                  "content": "Flash sale! #%d" % i, "priority": "low"}
                 for i in range(b, min(b + 1000, count))]
        batches.append(call(API + "/v1/notifications/batch", {"notifications": items})["batchId"])
    vip = call(API + "/v1/notifications", {"recipient": "+905550000001", "channel": "sms",
                                           "content": "VIP: your order shipped", "priority": "high"})
    print(f"enqueued {count} low + 1 high priority SMS in {time.time() - start:.2f}s")

    vip_reported = False
    while True:
        finished = 0
        for bid in batches:
            counts = call(API + "/v1/batches/" + bid)["counts"]
            finished += counts["sent"] + counts["failed"] + counts["cancelled"]
        if not vip_reported and call(API + "/v1/notifications/" + vip["id"])["status"] == "sent":
            vip_reported = True
            print(f"high priority SMS sent after {time.time() - start:.2f}s, "
                  f"while only {finished}/{count} low priority ones were finished")
        if finished >= count:
            break
        time.sleep(0.5)

    elapsed = time.time() - start
    stats = call(PROVIDER + "/stats")
    sms = stats["channels"]["sms"]
    print(f"all {count} finished in {elapsed:.1f}s ({count / elapsed:.1f} msg/s incl. retry backoff)")
    print(f"provider requests: {sms['received']} (accepted {sms['accepted']}, "
          f"failed/throttled {stats['failedResponses']} and retried)")
    print(f"max requests in any sliding second: {sms['maxRequestsInAnySlidingSecond']} (limit 100)")


if __name__ == "__main__":
    main()
