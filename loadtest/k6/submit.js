// k6 load test for the HTTP submit endpoint (API + Postgres insert + XADD), not job execution.
//   docker run --rm -i --network host -e API=http://localhost:18082 -e KEY=dev-key \
//     grafana/k6 run - < loadtest/k6/submit.js
import http from "k6/http";
import { check } from "k6";

export const options = {
  scenarios: {
    submit: {
      executor: "constant-arrival-rate", // open-loop: fixed request rate regardless of latency
      rate: Number(__ENV.RATE || 200),
      timeUnit: "1s",
      duration: __ENV.DURATION || "30s",
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<250"],
  },
};

const API = __ENV.API || "http://localhost:18082";
const params = { headers: { "Content-Type": "application/json", "X-API-Key": __ENV.KEY || "dev-key" } };

export default function () {
  const res = http.post(`${API}/v1/jobs`, JSON.stringify({ type: "noop", payload: {} }), params);
  check(res, { "201 created": (r) => r.status === 201 });
}
