import http from 'k6/http';
import { check, sleep } from 'k6';
import exec from 'k6/execution';

export let options = {
    stages: [
        { duration: '30s', target: 1 },
        // { duration: '1m', target: 100 },
        // { duration: '30s', target: 0 },
    ],
};

export default function () {
    const url = 'http://localhost/notification/send-email/';

    const payload = JSON.stringify({
        subject: 'Test Subject',
        message: 'Test Message',
        category: 'marketing'
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
        'returned status message': (r) => JSON.parse(r.body).status.startsWith('Messages sent to'),
    });

    // Pause for a second between iterations
    sleep(1);
}
