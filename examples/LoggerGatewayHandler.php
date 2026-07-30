<?php

namespace App\Logging;

use Illuminate\Support\Facades\Http;
use Monolog\Handler\AbstractProcessingHandler;
use Monolog\LogRecord;

/**
 * Custom Monolog handler: ships records to logden.
 * Compatible with Laravel 9+ / Monolog 3.
 *
 * 1) config/logging.php → add to the 'channels' array:
 *
 *    'remote' => [
 *        'driver'  => 'monolog',
 *        'handler' => \App\Logging\LoggerGatewayHandler::class,
 *        'level'   => 'debug',
 *    ],
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
    protected function write(LogRecord $record): void
    {
        try {
            // The body is an array of events — same shape the batching clients use.
            Http::withToken(config('services.logden.token'))
                ->timeout(2)
                ->post(rtrim((string) config('services.logden.url'), '/').'/logs', [[
                    'project'   => config('services.logden.project', config('app.name')),
                    'level'     => strtolower($record->level->getName()),
                    'message'   => $record->message,
                    'context'   => $record->context,
                    // The record's own time, not the moment the gateway sees it.
                    'timestamp' => $record->datetime->format(\DateTimeInterface::RFC3339_EXTENDED),
                ]]);
        } catch (\Throwable $e) {
            // Logging must never break the request. On high-traffic
            // paths, move the send logic to a queue (dispatch).
        }
    }
}
