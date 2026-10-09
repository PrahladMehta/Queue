export type LogFields = Record<string, string | number | boolean | null>;

export interface Logger {
  info(event: string, fields?: LogFields): void;
  error(event: string, fields?: LogFields): void;
}

export function createJsonLogger(base: LogFields = {}): Logger {
  const write = (level: 'info' | 'error', event: string, fields: LogFields = {}): void => {
    const line = JSON.stringify({ ts: new Date().toISOString(), level, event, ...base, ...fields });
    if (level === 'error') console.error(line);
    else console.log(line);
  };
  return {
    info: (event, fields) => write('info', event, fields),
    error: (event, fields) => write('error', event, fields),
  };
}

export const silentLogger: Logger = {
  info: () => {},
  error: () => {},
};
