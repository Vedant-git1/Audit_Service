import { loadConfig } from './env.js';
import { createLogger } from '../shared/logger.js';

const config = loadConfig();
createLogger(config).info(
  {
    serviceName: config.SERVICE_NAME,
    environment: config.NODE_ENV,
    port: config.PORT,
    kafkaEnabled: config.KAFKA_ENABLED,
  },
  'Environment configuration is valid',
);
