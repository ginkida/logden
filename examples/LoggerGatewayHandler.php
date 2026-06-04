<?php

namespace App\Logging;

use Illuminate\Support\Facades\Http;
use Monolog\Handler\AbstractProcessingHandler;
use Monolog\LogRecord;

/**
 * Кастомный Monolog-handler: шлёт записи в logden.
 * Совместим с Laravel 9+/Monolog 3.
 *
 * 1) config/logging.php → в массив 'channels' добавить:
 *
 *    'remote' => [
 *        'driver'  => 'monolog',
 *        'handler' => \App\Logging\LoggerGatewayHandler::class,
 *        'level'   => 'debug',
 *    ],
 *
 * 2) .env:
 *    LOG_GATEWAY_URL=http://logs.internal:8080
 *    LOG_GATEWAY_TOKEN=общий_секрет
 *    LOG_GATEWAY_PROJECT=billing-api
 *
 * 3) Слать:  Log::channel('remote')->error('...', ['order_id' => 123]);
 *    или сделать 'remote' частью стека в LOG_CHANNEL.
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
            // Логирование не должно ронять запрос. Для высоконагруженных
            // путей логику отправки лучше унести в очередь (dispatch).
        }
    }
}
