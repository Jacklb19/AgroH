import { Pool } from "pg";

function makePool() {
  const pool = new Pool({
    host:     process.env.DB_HOST,
    port:     parseInt(process.env.DB_PORT || "5432"),
    database: process.env.DB_NAME,
    user:     process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    ssl: { rejectUnauthorized: false },
    max: 5,
    idleTimeoutMillis:       60000,
    connectionTimeoutMillis: 20000,
  });

  /* La primera conexión al pooler de Supabase (arranque en frío) a veces falla
     o tarda de más: se reintenta una vez ante errores de conexión. */
  const query = pool.query.bind(pool);
  const esErrorConexion = (err) =>
    /timeout exceeded when trying to connect|ECONNRESET|ECONNREFUSED|ENOTFOUND|EAI_AGAIN|Connection terminated/i
      .test(`${err?.code || ""} ${err?.message || ""}`);
  pool.query = async (...args) => {
    try {
      return await query(...args);
    } catch (err) {
      if (!esErrorConexion(err)) throw err;
      await new Promise((r) => setTimeout(r, 500));
      return query(...args);
    }
  };
  return pool;
}

/* Reutilizar el pool entre hot-reloads de Next.js dev */
if (!global._pgPool) global._pgPool = makePool();

export default global._pgPool;
