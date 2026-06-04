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
 * 3) Usage:  Log::channel('remote')->error('...', ['order_id' => 123]);
 *    or make 'remote' part of the stack in LOG_CHANNEL.
 */
class LoggerGatewayHandler extends AbstractProcessingHandler
{
    protected function write(LogRecord $record): void
    {
        try {
            Http::withToken(env('LOG_GATEWAY_TOKEN'))
                ->timeout(2)
                ->post(rtrim(env('LOG_GATEWAY_URL'), '/').'/logs', [
                    'project' => env('LOG_GATEWAY_PROJECT', config('app.name')),
                    'level'   => strtolower($record->level->getName()),
                    'message' => $record->message,
                    'context' => $record->context,
                ]);
        } catch (\Throwable $e) {
            // Logging must never break the request. On high-traffic
            // paths, move the send logic to a queue (dispatch).
        }
    }
}
