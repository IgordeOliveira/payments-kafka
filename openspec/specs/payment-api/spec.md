# payment-api Specification

## Purpose
Exposes the HTTP API that records Pix payments and their lifecycle transitions, writing each state change and its event atomically through a transactional outbox.

## Requirements

### Requirement: Create a payment
`POST /payments` with a `merchant_id` and a positive integer `amount_cents` SHALL create a payment in status `CREATED` with `sequence` 1 and currency `BRL`, and return `201` with the payment representation.

#### Scenario: Valid request
- **WHEN** `POST /payments` is called with `{"merchant_id": "merchant-007", "amount_cents": 4590}`
- **THEN** the response is `201` with a new `payment_id`, `status` `CREATED`, `sequence` 1 and `amount_cents` 4590

#### Scenario: Invalid body
- **WHEN** `POST /payments` is called with a missing `merchant_id` or an `amount_cents` that is zero, negative or not an integer
- **THEN** the response is `400` and no payment or outbox row is created

### Requirement: Lifecycle transitions
The API SHALL allow `confirm` and `expire` only from `CREATED` and `refund` only from `CONFIRMED`, incrementing `sequence` by one on each successful transition.

#### Scenario: Confirm then refund
- **WHEN** a created payment is confirmed and then refunded
- **THEN** both calls return `200`, and the payment ends in status `REFUNDED` with `sequence` 3

#### Scenario: Invalid transition
- **WHEN** `refund` is called on a payment in status `CREATED`, or `confirm` on a payment that is already `EXPIRED`
- **THEN** the response is `409`, the payment is unchanged and no outbox row is created

#### Scenario: Unknown payment
- **WHEN** a transition or `GET /payments/{id}` is requested for an id that does not exist
- **THEN** the response is `404`

### Requirement: Transactional outbox
Every successful state change SHALL insert exactly one row into `outbox` in the same database transaction as the payment change, so that either both are committed or neither is.

#### Scenario: State change commits both rows
- **WHEN** a payment is created successfully
- **THEN** the `payments` table has the payment and `outbox` has exactly one row for it with `event_type` `payment.created` and the payment's `merchant_id`

#### Scenario: Failure rolls back both rows
- **WHEN** inserting the outbox row fails during a state change
- **THEN** the payment change is rolled back and the API returns a server error

### Requirement: Outbox payload matches the event contract
The `payload` of every outbox row SHALL be a payment event that validates against `contracts/payment-event.v1.schema.json`, with `event_id` equal to the row `id`, and with `event_type` and `sequence` matching the transition.

#### Scenario: Payloads validate
- **WHEN** a payment goes through created, confirmed and refunded
- **THEN** the three outbox payloads validate against the schema and carry sequences 1, 2 and 3 with distinct `event_id` values

### Requirement: No dependency on Kafka
The API SHALL NOT connect to Kafka, and SHALL keep accepting and committing requests while the Kafka broker is unavailable.

#### Scenario: Kafka is down
- **WHEN** the Kafka broker is stopped and payments are created
- **THEN** every request succeeds and the corresponding outbox rows are committed

### Requirement: Concurrent transitions are safe
Concurrent transition requests for the same payment SHALL result in at most one successful transition per lifecycle step.

#### Scenario: Two simultaneous confirms
- **WHEN** two `confirm` requests for the same created payment are sent at the same time
- **THEN** exactly one returns `200`, the other returns `409`, and exactly one `payment.confirmed` outbox row exists

### Requirement: Health endpoint
The API SHALL expose `GET /actuator/health` returning `200` when it can serve requests.

#### Scenario: Healthy service
- **WHEN** the service is running and connected to Postgres
- **THEN** `GET /actuator/health` returns `200`
