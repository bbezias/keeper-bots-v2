import { createLogger, transports, format } from 'winston';

export const logger = createLogger({
	transports: [new transports.Console()],
	format: format.combine(
		format.errors({stack: true}),
		format.colorize(),
		format.timestamp(),
		format.printf(({ timestamp, level, message }) => {
			return `[${timestamp}] ${level}: ${message}`;
		}),
		format.prettyPrint()
	),
});

export const setLogLevel = (logLevel: string) => {
	logger.level = logLevel;
};
