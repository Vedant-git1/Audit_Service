import { loadConfig } from '../../src/config/env.js';
import { createKafkaConnection } from '../../src/infrastructure/kafka/client.js';
import pino from 'pino';

describe('Kafka connection configuration', () => {
  it('does not connect to a broker in explicitly disabled local mode', async () => {
    const config = loadConfig({
      NODE_ENV: 'test',
      REDIS_URL: 'redis://localhost:6379',
      JWT_JWKS_URL: 'http://localhost:3001/.well-known/jwks.json',
      DATABASE_URL:
        'postgresql://audit_user:local-test-password@localhost:5433/resolve_audit',
      KAFKA_ENABLED: 'false',
    });
    const connection = createKafkaConnection(
      config,
      { processMessage: jest.fn().mockResolvedValue('PROCESSED') },
      pino({ level: 'silent' }),
    );

    await expect(connection.connect()).resolves.toBeUndefined();
    await expect(connection.checkHealth()).resolves.toBeUndefined();
    await expect(connection.disconnect()).resolves.toBeUndefined();
  });
});
