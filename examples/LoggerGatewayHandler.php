<?php

namespace App\Logging;

use Illuminate\Support\Facades\Http;
use Monolog\Handler\AbstractProcessingHandler;
use Monolog\LogRecord;

/**
 * Custom Monolog handler: ships records to logden.
 * Compatible with Laravel 9+ / Monolog 3, PHP 8.0+.
 *
 * 1) config/logging.php → add to the 'channels' array:
 *
 *    'remote' => [
 *        'driver'  => 'monolog',
 *        'handler' => \App\Logging\LoggerGatewayHandler::class,
 *        'level'   => 'debug',
 *    ],
 *
 *    No 'formatter' key is required, and the handler deliberately does not rely
 *    on one: LogManager::prepareHandler() calls setFormatter() with its own
 *    LineFormatter on every handler it builds unless the channel config carries
 *    'formatter' => 'default'. A formatter installed in the constructor is
 *    therefore replaced behind your back and $record->formatted arrives as a
 *    string, so the context is normalized in write() below instead.
 *
 * 2) .env:
 *    LOG_GATEWAY_URL=http://logs.internal:8080
 *    LOG_GATEWAY_TOKEN=shared_secret
 *    LOG_GATEWAY_PROJECT=billing-api
 *
 * 3) config/services.php → add the entry that reads them. This indirection is
 *    required: env() returns null once `php artisan config:cache` has run (the
 *    standard production step), which would silently send every record to
 *    "null/logs" with a null token and no visible error.
 *
 *    'logden' => [
 *        'url'     => env('LOG_GATEWAY_URL'),
 *        'token'   => env('LOG_GATEWAY_TOKEN'),
 *        'project' => env('LOG_GATEWAY_PROJECT', env('APP_NAME')),
 *    ],
 *
 * 4) Usage:  Log::channel('remote')->error('...', ['order_id' => 123]);
 *    or make 'remote' part of the stack in LOG_CHANNEL.
 */
class LoggerGatewayHandler extends AbstractProcessingHandler
{
    /**
     * Depth cap for nested arrays and objects: past it a value collapses to a
     * marker, which also keeps a self-referencing structure from recursing forever.
     */
    private const MAX_DEPTH = 6;

    /**
     * Stack frames kept per exception. The bound is the point of the exercise: a
     * framework trace through long vendor paths, multiplied by the previous-chain,
     * clears the gateway's MAX_CONTEXT_BYTES (64 KiB by default), and an oversized
     * context is DISCARDED whole ({"_truncated":true,...}) rather than trimmed —
     * an unbounded trace would lose more than the empty object it replaces.
     */
    private const MAX_TRACE_FRAMES = 20;

    /** How far down the getPrevious() chain to walk before dropping the rest. */
    private const MAX_PREVIOUS_DEPTH = 3;

    protected function write(LogRecord $record): void
    {
        try {
            // The body is an array of events — the same shape the batching clients use.
            $payload = [[
                'project'   => config('services.logden.project', config('app.name')),
                'level'     => strtolower($record->level->getName()),
                'message'   => $record->message,
                'context'   => $this->normalize($record->context),
                // The record's own time, not the moment the gateway sees it.
                'timestamp' => $record->datetime->format(\DateTimeInterface::RFC3339_EXTENDED),
            ]];

            // Encode here instead of handing the array to Http::post(): Guzzle would
            // encode it and THROW on failure, and the catch below would swallow the
            // whole record without a trace. JSON_INVALID_UTF8_SUBSTITUTE covers stray
            // non-UTF-8 bytes in a message, which would otherwise fail the encode
            // before the gateway's own UTF-8 sanitizer ever sees them.
            $flags = JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE
                | JSON_INVALID_UTF8_SUBSTITUTE | JSON_PARTIAL_OUTPUT_ON_ERROR;
            $json = json_encode($payload, $flags);
            if ($json === false) {
                // normalize() should have made this unreachable; if it did not, keep the
                // event and lose only the context, with the reason visible in ClickHouse.
                $payload[0]['context'] = ['_encode_error' => json_last_error_msg()];
                $json = json_encode($payload, $flags);
            }
            if ($json === false) {
                return;
            }

            Http::withToken(config('services.logden.token'))
                ->withBody($json, 'application/json')
                ->timeout(2)
                ->post(rtrim((string) config('services.logden.url'), '/').'/logs');
        } catch (\Throwable) {
            // Deliberate last resort: logging must never break the request, and there is
            // no safe place to report from here — a Log::channel() fallback recurses if
            // the named channel resolves back into a stack holding this handler. If you
            // add one, name a channel that provably excludes it and keep it in its own
            // try/catch. On high-traffic paths, move the send logic to a queue (dispatch).
        }
    }

    /**
     * Reduce an arbitrary Monolog context to values json_encode() can render.
     *
     * Two failure modes make this mandatory. json_encode() serializes public
     * properties only, so ['exception' => $e] — Laravel's own reporting idiom —
     * arrives as {} because a Throwable keeps its class, file, line and trace in
     * protected ones. And a single resource or closure anywhere in the context makes
     * json_encode() return false, which costs the entire record, not just that value.
     */
    private function normalize(mixed $value, int $depth = 0): mixed
    {
        if ($value === null || is_scalar($value)) {
            // NAN and INF have no JSON form at all: json_encode() fails outright on them.
            // Spelled out rather than cast, because (string) NAN raises a warning on PHP
            // 8.5, and Laravel turns warnings into a thrown ErrorException.
            if (is_float($value) && !is_finite($value)) {
                return is_nan($value) ? 'NaN' : ($value > 0 ? 'Inf' : '-Inf');
            }
            return $value;
        }

        if ($value instanceof \Throwable) {
            return $this->normalizeThrowable($value, 0);
        }

        if (is_array($value)) {
            if ($depth >= self::MAX_DEPTH) {
                return '[array]';
            }
            $out = [];
            foreach ($value as $key => $item) {
                $out[$key] = $this->normalize($item, $depth + 1);
            }
            return $out;
        }

        if ($value instanceof \DateTimeInterface) {
            return $value->format(\DateTimeInterface::RFC3339_EXTENDED);
        }

        if ($depth < self::MAX_DEPTH) {
            if ($value instanceof \JsonSerializable) {
                return $this->normalize($value->jsonSerialize(), $depth + 1);
            }
            if (is_object($value)) {
                // Called from outside the class, get_object_vars() returns the public
                // properties — exactly what json_encode() would have emitted for it.
                $vars = get_object_vars($value);
                if ($vars !== []) {
                    return $this->normalize($vars, $depth + 1);
                }
                if ($value instanceof \Stringable) {
                    try {
                        return (string) $value;
                    } catch (\Throwable) {
                        // A __toString() that throws must not take the record down with it.
                    }
                }
            }
        }

        // Resources, closures, property-less objects: a type marker costs one short
        // string, while leaving the value in place costs the whole record.
        return '['.get_debug_type($value).']';
    }

    /**
     * Turn a Throwable into the fields that actually explain the failure. Bounded on
     * both axes — trace length and previous-chain depth — see MAX_TRACE_FRAMES.
     */
    private function normalizeThrowable(\Throwable $e, int $depth): array
    {
        $trace = $e->getTrace();
        $frames = [];
        foreach (array_slice($trace, 0, self::MAX_TRACE_FRAMES) as $frame) {
            // file:line only, never getTraceAsString(): that renders call arguments, so a
            // DSN, token or password passed to a constructor would end up stored in logs.
            $frames[] = ($frame['file'] ?? '[internal]').':'.($frame['line'] ?? 0);
        }
        if (count($trace) > self::MAX_TRACE_FRAMES) {
            $frames[] = '... '.(count($trace) - self::MAX_TRACE_FRAMES).' more frames';
        }

        $out = [
            'class'   => $e::class,
            'message' => $e->getMessage(),
            'code'    => $e->getCode(),
            'file'    => $e->getFile().':'.$e->getLine(),
            'trace'   => $frames,
        ];

        $previous = $e->getPrevious();
        if ($previous !== null && $depth + 1 < self::MAX_PREVIOUS_DEPTH) {
            $out['previous'] = $this->normalizeThrowable($previous, $depth + 1);
        }

        return $out;
    }
}
