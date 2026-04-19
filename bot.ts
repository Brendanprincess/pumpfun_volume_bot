import TelegramController from './src/telegram';
import http from 'http';

console.log('Starting Telegram bot controller...');
// Ensure TELEGRAM_BOT_TOKEN and other necessary env vars are loaded before this
// (dotenv is called in config.ts, which telegram.ts imports)
const port = process.env.PORT ? Number.parseInt(process.env.PORT, 10) : undefined;
if (port && Number.isFinite(port)) {
    const server = http.createServer((req, res) => {
        res.statusCode = 200;
        res.setHeader('content-type', 'text/plain; charset=utf-8');
        res.end('ok');
    });

    server.listen(port, '0.0.0.0', () => {
        console.log(`Healthcheck server listening on port ${port}`);
    });
}

try {
    const telegramController = new TelegramController();
    console.log('Telegram bot is running. Send /help to get started (if authorized).');
} catch (error) {
    console.error("Failed to initialize TelegramController:", error);
    process.exit(1);
}
