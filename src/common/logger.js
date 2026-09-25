/**
 * Vault Object Storage - Structured Logger
 */

const LOG_LEVELS = {
  DEBUG: 0,
  INFO: 1,
  WARN: 2,
  ERROR: 3,
};

const CURRENT_LEVEL = process.env.VAULT_LOG_LEVEL ? (LOG_LEVELS[process.env.VAULT_LOG_LEVEL.toUpperCase()] ?? 1) : 1;

const COLORS = {
  reset: '\x1b[0m',
  dim: '\x1b[2m',
  bold: '\x1b[1m',
  blue: '\x1b[34m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  red: '\x1b[31m',
  cyan: '\x1b[36m',
  magenta: '\x1b[35m',
};

class Logger {
  constructor(component = 'VAULT') {
    this.component = component;
  }

  child(subComponent) {
    return new Logger(`${this.component}:${subComponent}`);
  }

  _format(level, msg, meta) {
    const ts = new Date().toISOString().split('T')[1].replace('Z', '');
    let color = COLORS.reset;
    if (level === 'DEBUG') color = COLORS.dim;
    if (level === 'INFO') color = COLORS.cyan;
    if (level === 'WARN') color = COLORS.yellow;
    if (level === 'ERROR') color = COLORS.red;

    const metaStr = meta && Object.keys(meta).length > 0 ? ` ${JSON.stringify(meta)}` : '';
    return `${COLORS.dim}[${ts}]${COLORS.reset} ${color}[${level.padEnd(5)}]${COLORS.reset} ${COLORS.bold}[${this.component}]${COLORS.reset} ${msg}${metaStr}`;
  }

  debug(msg, meta) {
    if (CURRENT_LEVEL <= LOG_LEVELS.DEBUG) {
      console.log(this._format('DEBUG', msg, meta));
    }
  }

  info(msg, meta) {
    if (CURRENT_LEVEL <= LOG_LEVELS.INFO) {
      console.log(this._format('INFO', msg, meta));
    }
  }

  warn(msg, meta) {
    if (CURRENT_LEVEL <= LOG_LEVELS.WARN) {
      console.warn(this._format('WARN', msg, meta));
    }
  }

  error(msg, meta) {
    if (CURRENT_LEVEL <= LOG_LEVELS.ERROR) {
      console.error(this._format('ERROR', msg, meta));
    }
  }
}

export const createLogger = (component) => new Logger(component);
export default new Logger('VAULT');
