import http from 'k6/http';
import { check, sleep } from 'k6';
import exec from 'k6/execution';

export let options = {
    stages: [
        { duration: '30s', target: 100 },  // ramp-up to 100 VUs
        { duration: '2m', target: 100 },   // stay at 100 VUs
        { duration: '30s', target: 0 },    // ramp-down to 0
    ],
    // Fail the test if 95% of requests take longer than 500ms
    thresholds: {
        http_req_duration: ['p(95)<500'],
        http_req_failed: ['rate<0.01'],    // fewer than 1% failed requests
    },
};

// Build a nanosecond‑format ISO timestamp
function getHighResTimestamp() {
    // Absolute start time (integer ms since epoch)
    const startMs = exec.scenario.startTime;
    // Elapsed time since scenario start (float ms, sub‑ms precision)
    const elapsedMs = exec.instance.currentTestRunDuration;
    // Combined float ms timestamp
    const nowMs = startMs + elapsedMs;

    // ISO seconds part, e.g. "2025-05-02T16:57:43"
    const isoSec = new Date(Math.floor(nowMs)).toISOString().split('.')[0];

    // Fractional part: convert fractional ms to µs (6 digits), pad to 9 digits
    const fraction = Math.round((nowMs % 1000) * 1e6) // µs
    .toString()
    .padStart(6, '0')
    + '000'; // pad to nanoseconds

    return `${isoSec}.${fraction}Z`;
}

export default function () {
    const timestamp = getHighResTimestamp();

    const url = 'http://ec2-3-148-254-82.us-east-2.compute.amazonaws.com:8000/send-email/';

    // capture the current timestamp
    // const now = new Date().toISOString();

    // Always use this email per spec:
    const payload = JSON.stringify({
        email: 'aniruddhatupe@protonmail.com',
        subject: 'k6 Load Test',
        message: timestamp,
    });

    const params = {
        headers: {
        'Content-Type': 'application/json',
        },
    };

    // Send the POST request
    const res = http.post(url, payload, params);

    // Verify we got a 200 OK back
    check(res, {
        'status is 200': (r) => r.status === 200,
        'returned status message': (r) => JSON.parse(r.body).status === 'Message sent to Kafka',
    });

    // Pause for a second between iterations
    sleep(1);
}
