package protocol

import (
	"sort"
	"strconv"
	"strings"
	"unicode"
)

type ErrorClass string

const (
	ErrorClassAuthorization ErrorClass = "authorization"
	ErrorClassAvailability  ErrorClass = "availability"
	ErrorClassConflict      ErrorClass = "conflict"
	ErrorClassFencing       ErrorClass = "fencing"
	ErrorClassInternal      ErrorClass = "internal"
	ErrorClassNotFound      ErrorClass = "not_found"
	ErrorClassRouting       ErrorClass = "routing"
	ErrorClassValidation    ErrorClass = "validation"
)

type ErrorClassification struct {
	Class     ErrorClass
	Retryable bool
}

func IsKnownErrorClass(class ErrorClass) bool {
	switch class {
	case ErrorClassAuthorization, ErrorClassAvailability, ErrorClassConflict, ErrorClassFencing,
		ErrorClassInternal, ErrorClassNotFound, ErrorClassRouting, ErrorClassValidation:
		return true
	default:
		return false
	}
}

type ErrorResponse struct {
	Code                   string
	Class                  ErrorClass
	Retryable              bool
	ClassExplicit          bool
	RetryableExplicit      bool
	ExplicitClassification bool
	Fields                 map[string]string
	Details                []string
	Raw                    string
}

var errorRegistry = buildErrorRegistry()

func buildErrorRegistry() map[string]ErrorClassification {
	registry := make(map[string]ErrorClassification)
	register := func(class ErrorClass, retryable bool, codes ...string) {
		for _, code := range codes {
			registry[code] = ErrorClassification{Class: class, Retryable: retryable}
		}
	}

	register(ErrorClassRouting, true,
		"NOT_COORDINATOR", "NOT_LEADER", "NOT_PARTITION_LEADER",
	)
	register(ErrorClassAvailability, true,
		"cluster_metadata_unavailable", "cluster_not_available", "coordinator_not_available", "fsm_not_available", "handler_closed",
		"insufficient_in_sync_replicas", "leader_election_result_unavailable", "leader_not_found", "no_raft_leader", "offset_manager_not_available", "replication_unavailable", "request_cancelled", "router_not_available",
		"transaction_abort_marker_failed", "transaction_commit_failed", "transaction_manager_not_available", "transaction_sync_failed",
	)
	register(ErrorClassFencing, false,
		"GEN_MISMATCH", "NOT_OWNER", "PARTITION_LEADER_FENCED", "STALE_LEADER_EPOCH", "STALE_TOPIC_LIFECYCLE_EPOCH", "member_not_found",
		"missing_topic_lifecycle_epoch", "producer_fenced", "stale_producer_epoch",
	)
	register(ErrorClassConflict, false,
		"OFFSET_OUT_OF_RANGE", "leader_election_rejected", "offset_regression", "snapshot_version_exceeds_stream",
		"producer_reinitialization_required", "topic_delete_blocked", "topic_lifecycle_pending", "topic_revision_conflict", "topic_truncate_blocked",
		"topic_not_assigned_to_group", "transaction_aborted", "transaction_already_committed",
		"transaction_marker_partition_not_touched", "transaction_not_abortable", "transaction_not_committing",
		"transaction_record_not_staged", "version_conflict",
	)
	register(ErrorClassAuthorization, false,
		"NOT_AUTHORIZED_FOR_OPERATION", "NOT_AUTHORIZED_FOR_PARTITION", "NOT_AUTHORIZED_FOR_TOPIC", "authentication_failed", "authentication_required",
		"internal_auth_not_configured", "internal_batch_requires_token_wrapper", "internal_command_unauthorized", "internal_topic_delete_forbidden", "internal_topic_truncate_forbidden",
		"internal_topic_write_forbidden", "internal_txn_publish_forbidden", "transaction_metadata_forbidden",
	)
	register(ErrorClassNotFound, false,
		"group_not_found", "partition_metadata_not_found", "partition_not_found", "topic_not_found", "transaction_not_found",
	)
	register(ErrorClassValidation, false,
		"UNSUPPORTED_FEATURE", "UNSUPPORTED_PROTOCOL_VERSION", "batch_decode_failed", "decode_failed",
		"distribution_not_enabled", "distribution_required", "duplicate_partition", "empty_command", "empty_messages",
		"empty_required_params", "event_sourcing_not_enabled", "invalid_acks", "invalid_batch_commit_entry", "invalid_batch_commit_format", "invalid_min_in_sync_replicas",
		"invalid_auth", "invalid_consume_syntax", "invalid_control_batch_bytes", "invalid_control_batch_coordinator_epoch",
		"invalid_control_batch_version", "invalid_commit_watermark", "invalid_epoch", "invalid_generation", "invalid_is_idempotent",
		"invalid_event_sourcing", "invalid_idempotent", "invalid_if_exists", "invalid_offset", "invalid_partition", "invalid_partitions", "invalid_payload", "invalid_payload_json",
		"invalid_protocol_features", "invalid_protocol_version", "invalid_replication_factor", "invalid_require_features",
		"invalid_transaction_control_batch", "invalid_transaction_control_epoch", "invalid_transaction_control_record",
		"invalid_transaction_marker", "invalid_transaction_marker_producer", "invalid_transaction_result",
		"invalid_transaction_state", "invalid_txn_offsets",
		"invalid_retention_bytes", "invalid_retention_hours", "invalid_schema_version", "invalid_seq_num",
		"invalid_snapshot_catchup_response", "invalid_snapshot_payload", "invalid_stream_syntax",
		"invalid_expected_revision", "invalid_topic_name", "invalid_topic_policy", "invalid_version", "invalid_from_version", "malformed_input", "missing_expected_revision", "missing_generation", "missing_group", "missing_key",
		"unsupported_topic_policy",
		"missing_broker", "missing_leader_fence", "missing_member", "missing_message", "missing_offset", "missing_partition", "missing_payload",
		"missing_coordinator_key", "missing_ownership_params", "missing_producer_id", "missing_protocol_version",
		"missing_min_in_sync_replicas", "missing_required_params", "missing_topic", "missing_transactional_id",
		"missing_version", "no_valid_offsets", "unknown_command", "unmarshal_failed",
	)
	register(ErrorClassInternal, false,
		"append_stream_failed", "broker_error", "command_failed", "coordinator_error", "create_topic_failed",
		"alter_topic_config_failed", "delete_topic_failed", "empty_command_response", "find_coordinator_failed", "forward_to_coordinator_failed",
		"forward_to_leader_failed", "forward_to_partition_leader_failed", "group_status_failed",
		"local_processor_not_configured", "marshal_ack_failed", "marshal_brokers_failed", "marshal_cluster_status_failed", "marshal_metadata_failed",
		"marshal_snapshot_failed", "marshal_snapshots_failed", "marshal_status_failed", "negotiation_context_required",
		"no_partitions_available", "offset_sync_failed", "partition_lookup_failed", "raft_apply_failed",
		"raft_batch_apply_failed", "register_group_failed", "replica_append_failed", "replica_index_failed", "replica_index_prepare_failed",
		"snapshot_catchup_failed", "snapshot_list_failed", "snapshot_read_failed", "snapshot_replicate_failed",
		"snapshot_save_failed", "snapshot_store_failed", "stream_index_failed", "topic_create_missing", "truncate_topic_failed",
		"init_producer_failed", "transaction_abort_failed", "transaction_begin_failed", "transaction_offsets_failed",
		"transaction_prepare_failed", "transaction_publish_failed",
	)
	return registry
}

func IsKnownErrorCode(code string) bool {
	_, ok := errorRegistry[code]
	return ok
}

func KnownErrorCodes() []string {
	codes := make([]string, 0, len(errorRegistry))
	for code := range errorRegistry {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	return codes
}

func ClassifyErrorCode(code string) ErrorClassification {
	if classification, ok := errorRegistry[code]; ok {
		return classification
	}

	lower := strings.ToLower(code)
	switch {
	case strings.HasPrefix(lower, "not_authorized"), strings.HasPrefix(lower, "authorization_"):
		return ErrorClassification{Class: ErrorClassAuthorization}
	case strings.HasSuffix(lower, "_not_found"), strings.HasSuffix(lower, "_missing"):
		return ErrorClassification{Class: ErrorClassNotFound}
	case strings.HasPrefix(lower, "missing_"), strings.HasPrefix(lower, "invalid_"),
		strings.HasPrefix(lower, "empty_"), strings.HasPrefix(lower, "malformed_"),
		strings.HasPrefix(lower, "decode_"), strings.HasPrefix(lower, "unmarshal_"),
		lower == "unknown_command", lower == "distribution_required", lower == "distribution_not_enabled":
		return ErrorClassification{Class: ErrorClassValidation}
	default:
		return ErrorClassification{Class: ErrorClassInternal}
	}
}

func ParseErrorResponse(raw string) (*ErrorResponse, bool) {
	trimmed := strings.TrimSpace(raw)
	var body string
	switch {
	case strings.HasPrefix(trimmed, "ERROR:"):
		body = strings.TrimSpace(strings.TrimPrefix(trimmed, "ERROR:"))
	default:
		return nil, false
	}

	tokens := splitResponseFields(body)
	if len(tokens) == 0 {
		return nil, false
	}

	classification := ClassifyErrorCode(tokens[0])
	parsed := &ErrorResponse{
		Code:      tokens[0],
		Class:     classification.Class,
		Retryable: classification.Retryable,
		Fields:    make(map[string]string),
		Raw:       trimmed,
	}
	for _, token := range tokens[1:] {
		key, value, ok := strings.Cut(token, "=")
		if !ok || key == "" {
			parsed.Details = append(parsed.Details, token)
			continue
		}
		value = decodeFieldValue(value)
		parsed.Fields[key] = value
		if key == "class" && IsKnownErrorClass(ErrorClass(value)) {
			parsed.Class = ErrorClass(value)
			parsed.ClassExplicit = true
		}
		if key == "retryable" {
			if retryable, err := strconv.ParseBool(value); err == nil {
				parsed.Retryable = retryable
				parsed.RetryableExplicit = true
			}
		}
	}
	if _, supplied := parsed.Fields["class"]; supplied && !parsed.ClassExplicit {
		parsed.Class = classification.Class
		parsed.Retryable = classification.Retryable
		parsed.RetryableExplicit = false
	}
	parsed.ExplicitClassification = parsed.ClassExplicit && parsed.RetryableExplicit
	return parsed, true
}

// ErrorCode returns the exact leading broker error code. It is the only
// supported way for internal routing code to classify a textual controller
// response before the server encodes it as a structured Wire v2 error frame.
func ErrorCode(raw string) (string, bool) {
	parsed, ok := ParseErrorResponse(raw)
	if !ok {
		return "", false
	}
	return parsed.Code, true
}

func IsErrorResponse(raw string) bool {
	_, ok := ParseErrorResponse(raw)
	return ok
}

func EnrichErrorResponse(raw string) string {
	parsed, ok := ParseErrorResponse(raw)
	if !ok || parsed.ExplicitClassification || !strings.HasPrefix(strings.TrimSpace(raw), "ERROR:") {
		return raw
	}

	trimmed := strings.TrimSpace(raw)
	bodyStart := len("ERROR:")
	for bodyStart < len(trimmed) && unicode.IsSpace(rune(trimmed[bodyStart])) {
		bodyStart++
	}
	codeEnd := bodyStart
	for codeEnd < len(trimmed) && !unicode.IsSpace(rune(trimmed[codeEnd])) {
		codeEnd++
	}
	metadata := ""
	if !parsed.ClassExplicit {
		metadata += " class=" + string(parsed.Class)
	}
	if !parsed.RetryableExplicit {
		metadata += " retryable=" + strconv.FormatBool(parsed.Retryable)
	}
	return trimmed[:codeEnd] + metadata + trimmed[codeEnd:]
}

func splitResponseFields(value string) []string {
	var fields []string
	var token strings.Builder
	inQuotes := false
	escaped := false
	flush := func() {
		if token.Len() > 0 {
			fields = append(fields, token.String())
			token.Reset()
		}
	}

	for _, r := range value {
		if escaped {
			token.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' && inQuotes {
			token.WriteRune(r)
			escaped = true
			continue
		}
		if r == '"' {
			inQuotes = !inQuotes
			token.WriteRune(r)
			continue
		}
		if unicode.IsSpace(r) && !inQuotes {
			flush()
			continue
		}
		token.WriteRune(r)
	}
	flush()
	return fields
}

func decodeFieldValue(value string) string {
	if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
		if decoded, err := strconv.Unquote(value); err == nil {
			return decoded
		}
	}
	return value
}
