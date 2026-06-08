/**
 * generar_datos_prueba.js
 * Genera eventos.jsonl y eventos.csv en data/raw/
 * usando el mismo event_generator.js del proyecto.
 *
 * Uso:
 *   node generar_datos_prueba.js           → genera 100,000 registros
 *   node generar_datos_prueba.js 1000      → genera N registros
 */

const fs   = require("fs");
const path = require("path");
const { generarEvento } = require("./kafka/shared/event_generator");

const TIPOS = ["resource", "request", "error", "network", "security"];
const TOTAL = parseInt(process.argv[2]) || 100000;

const OUTPUT_DIR  = path.resolve(__dirname, "data/raw");
const JSONL_FILE  = path.join(OUTPUT_DIR, "eventos.jsonl");
const CSV_FILE    = path.join(OUTPUT_DIR, "eventos.csv");

// Columnas del schema real (sin metadatos kafka)
const FIELDS = [
  "id_log", "timestamp_evento", "servidor", "ip_servidor", "zona",
  "sistema_operativo", "ambiente", "servicio", "tipo_evento", "nivel",
  "codigo_estado", "endpoint", "metodo_http", "usuario", "correo_usuario",
  "ciudad", "ip_origen", "user_agent", "tiempo_respuesta_ms",
  "uso_cpu_porcentaje", "uso_ram_porcentaje", "uso_disco_porcentaje",
  "bytes_entrada", "bytes_salida", "peticiones_por_minuto",
  "conexiones_activas", "errores_minuto", "latencia_red_ms",
  "temperatura_cpu", "paquetes_perdidos", "intento_login",
  "ip_sospechosa", "pais_origen", "trace_id", "session_id", "proceso", "mensaje"
];

// Metadatos kafka simulados (para que el CSV sea igual al real)
const KAFKA_META = [
  "_kafka_topic", "_kafka_partition", "_kafka_offset",
  "_kafka_timestamp", "_consumer_ts"
];

const ALL_FIELDS = [...FIELDS, ...KAFKA_META];

function csvEscape(value) {
  if (value === null || value === undefined) return "";
  const str = String(value);
  if (str.includes(",") || str.includes('"') || str.includes("\n")) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

const jsonlStream = fs.createWriteStream(JSONL_FILE, { flags: "w" });
const csvStream   = fs.createWriteStream(CSV_FILE,   { flags: "w" });

// Escribir encabezado CSV
csvStream.write(ALL_FIELDS.join(",") + "\n");

console.log(`Generando ${TOTAL.toLocaleString()} eventos...`);
const inicio = Date.now();

for (let i = 0; i < TOTAL; i++) {
  const tipo   = TIPOS[i % TIPOS.length];
  const evento = generarEvento(tipo);

  // Metadatos kafka simulados
  const meta = {
    _kafka_topic    : `topic_${tipo}`,
    _kafka_partition: i % 3,
    _kafka_offset   : i,
    _kafka_timestamp: Date.now(),
    _consumer_ts    : new Date().toISOString()
  };

  // JSONL: una línea por evento con metadatos
  jsonlStream.write(JSON.stringify({ ...evento, ...meta }) + "\n");

  // CSV: todos los campos en orden
  const registro = { ...evento, ...meta };
  csvStream.write(ALL_FIELDS.map(f => csvEscape(registro[f])).join(",") + "\n");

  if ((i + 1) % 10000 === 0) {
    process.stdout.write(`\r  ${(i + 1).toLocaleString()} / ${TOTAL.toLocaleString()}`);
  }
}

jsonlStream.end();
csvStream.end();

const segundos = ((Date.now() - inicio) / 1000).toFixed(1);
console.log(`\n\nListo en ${segundos}s`);
console.log(`  ${JSONL_FILE}`);
console.log(`  ${CSV_FILE}`);
