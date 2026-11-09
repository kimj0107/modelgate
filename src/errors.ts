export type ErrorCode =
  | "invalid_request"
  | "body_too_large"
  | "not_found"
  | "method_not_allowed"
  | "unsupported_media_type"
  | "deadline_exceeded"
  | "upstream_unavailable"
  | "invalid_upstream"
  | "client_disconnected"
  | "internal_error";

export class GatewayError extends Error {
  constructor(
    public readonly code: ErrorCode,
    public readonly status: number,
  ) {
    super(code);
  }
}

export function abortError(signal: AbortSignal): GatewayError {
  return signal.reason instanceof GatewayError
    ? signal.reason
    : new GatewayError("internal_error", 500);
}

export function publicError(error: unknown, signal: AbortSignal): GatewayError {
  if (signal.aborted) return abortError(signal);
  if (error instanceof GatewayError) return error;
  return new GatewayError("upstream_unavailable", 502);
}
