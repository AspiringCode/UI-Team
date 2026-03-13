UNIVERSAL AGENT MESSAGING PACKAGE MVP

START

SET package_name = enterprise_router
SET persistence = SQLite
SET model = single-process, polling-based
SET delivery_mode = hybrid

DEFINE GOAL
    Build Python package + CLI demo
    Support:
        - agent self-registration
        - approval flow
        - registry lookup
        - message submission
        - priority routing
        - allowlist enforcement
        - per-recipient queues
        - message inspection
        - next-message fetching
        - ack / nack / retry
        - queue auditing

DEFINE CORE ENTITIES
    AgentRecord:
        agent_name, role, hierarchy_level, trust_level
        file_path, endpoint, active, registration_status
        allowed_senders, allowed_task_types
        created_at, approved_at

    RegistrationRequest:
        agent_name, role, secret_token
        file_path, endpoint, metadata

    MessageEnvelope:
        id, timestamp, sender, recipient
        task_type, context, payload
        status, error

    RoutingHints:
        provenance_source, provenance_agent
        provenance_trust_level, urgency
        ttl_seconds, dedupe_key

    QueuedMessage:
        MessageEnvelope + internal metadata
        computed_priority, attempt_count
        lease_until, delivery_state, blocked_reason

DEFINE PUBLIC METHODS
    register_agent()
    request_registration()
    approve_registration()
    reject_registration()
    get_agent()
    list_agents()
    create_message()
    submit_message()
    peek_messages()
    fetch_next()
    ack_message()
    nack_message()
    requeue_expired_leases()
    list_queue()

DEFINE STORAGE
    TABLE agents
    TABLE registration_requests
    TABLE messages
    TABLE routing_metadata
    TABLE audit_log

SELF-REGISTRATION FLOW
    Agent calls request_registration()
    VALIDATE request format
    CHECK duplicate agent_name
    STORE request as pending
    DENY pending agent from sending/receiving
    ADMIN approves or rejects request

    IF approved
        CREATE or ACTIVATE AgentRecord
    ELSE IF rejected
        STORE rejection reason in history

    STORE secret tokens as hashes only

ROUTING RULES
    USE hierarchy scale:
        1 = highest authority
        larger number = lower authority

    DEFAULT roles:
        CEO = 1
        PM / Finance / Engineering / Marketing / Sales / HR = 2
        worker / sub-agent = 3+

    COMPUTE priority_score =
        recipient_weight
        + urgency_weight
        + provenance_weight
        + age_bonus
        - hierarchy_penalty

    urgency_weight:
        low = 10
        normal = 25
        high = 50
        critical = 100

    APPLY tie_breaker:
        older timestamp first

    IF sender/recipient not approved or active
        REJECT message

    IF sender role or task_type not allowed by recipient policy
        STORE message as blocked
    ELSE
        STORE message as pending

DELIVERY RULES
    submit_message():
        VALIDATE envelope
        VALIDATE sender and recipient
        COMPUTE priority
        STORE as pending
        WRITE audit event

    peek_messages():
        RETURN filtered messages
        DO NOT mutate state

    fetch_next():
        SELECT highest-priority eligible pending message
        MARK as in_progress
        SET lease_until = now + 60 seconds

    ack_message():
        MARK message as done

    nack_message():
        INCREMENT attempt_count
        CLEAR lease
        RETURN message to pending
        RECOMPUTE priority
        RECORD failure reason

    IF attempt_count > 3
        MOVE to dead-letter / error

    IF ttl_seconds expires before ack
        MARK as expired
        EXCLUDE from normal fetches

    requeue_expired_leases():
        FIND expired leases
        RETURN them to pending

CLI COMMANDS
    request-registration
    approve-registration
    reject-registration
    list-agents
    send
    peek
    fetch
    ack
    nack
    show-queue
    show-audit

DEMO SCENARIOS
    SCENARIO 1:
        new worker self-registers
        status remains pending until approved

    SCENARIO 2:
        low-level approved agent sends to CEO
        message kept but delayed in priority

    SCENARIO 3:
        peer-to-peer approved message delivered normally

    SCENARIO 4:
        blocked-by-policy message stored with blocked reason

EXTRA FEATURES
    SUPPORT idempotency with dedupe_key
    TRACK provenance separately from sender
    LOG every state change
    EXPOSE queue visibility for:
        pending
        blocked
        in-progress
        expired
        dead-lettered
        pending-registration
    STORE explicit rejection and blocked reasons
    ENFORCE stable ordering and retry logic
    VALIDATE required envelope schema
    TRACK approval trail

TEST CASES
    VERIFY self-registration persists
    VERIFY duplicate names rejected
    VERIFY invalid registration rejected
    VERIFY pending agents cannot send/receive
    VERIFY approved agents become active
    VERIFY rejected registrations remain queryable
    VERIFY registry persists across restarts
    VERIFY valid message stored with priority
    VERIFY malformed message rejected
    VERIFY unknown sender/recipient rejected
    VERIFY envelope remains unchanged
    VERIFY routing metadata stored separately
    VERIFY allowed peer message succeeds
    VERIFY unauthorized sender is blocked
    VERIFY low-to-high hierarchy message delayed, not dropped
    VERIFY FIFO for equal priority
    VERIFY fetch_next leases one message only
    VERIFY ack marks done
    VERIFY nack requeues and increments attempts
    VERIFY lease expiry requeues message
    VERIFY TTL expiry removes stale message
    VERIFY dead-letter after max retries
    VERIFY peek does not mutate queue

ACCEPTANCE CRITERIA
    Agent can self-register without DB access
    Registration remains pending until approved
    Approved agents can send/receive
    Pending agents cannot send/receive
    Message can be submitted with recipient + payload + hints
    Recipient lookup works from persistent registry
    Priority score is computed and stored
    Unauthorized messages are blocked, not deleted
    Recipient can inspect or fetch messages
    Every state change is queryable
    MVP runs locally with SQLite only

DEFAULT ASSUMPTIONS
    Python is main target
    SQLite is acceptable for MVP
    Outer message envelope must not change
    Internal routing metadata stored separately
    Self-registration allowed, activation approval-gated
    Shared environment token acceptable for MVP
    Broadcast, push, and distributed workers deferred
    Default lifecycle = pending -> in_progress -> done/error
    Default fetch = single message
    Default upward-hierarchy behavior = delay, not block

END
